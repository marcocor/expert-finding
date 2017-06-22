import logging
import math
import time
import tagme

from random import random
from multiprocess.dummy import Pool

logger = logging.getLogger("EF_log")

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

def mean(numbers):
    return float(sum(numbers)) / max(len(numbers), 1)


def cossim_efiaf_score(exf, query_entities, query_entity_to_efiaf, author_id):
    author_entity_to_efiaf = dict((e[0], e[3])
                                  for e in exf.ef_iaf_author(author_id))

    return sum(author_entity_to_efiaf[e] * query_entity_to_efiaf[e] for e in set(author_entity_to_efiaf.keys()) & set(query_entity_to_efiaf.keys())) \
        / (math.sqrt(sum(author_entity_to_efiaf.values())) * math.sqrt(sum(query_entity_to_efiaf.values())))


def efiaf_score(exf, query_entities, query_entity_to_efiaf, author_entity_to_ec, author_id):
    author_papers = exf.data_layer.get_author_papers_count(author_id)
    return sum((author_entity_to_ec[e] / float(author_papers)) * query_entity_to_efiaf[e] for e in set(query_entities) & set(author_entity_to_ec.keys()))


def eciaf_score(exf, query_entities, query_entity_to_efiaf, author_entity_to_ec, author_id):
    return sum(author_entity_to_ec[e] * query_entity_to_efiaf[e] for e in set(query_entities) & set(author_entity_to_ec.keys()))


def log_ec_ef_iaf_score(exf, query_entities, query_entity_to_efiaf, author_entity_to_ec, author_id):
    author_papers = exf.data_layer.get_author_papers_count(author_id)
    return sum((math.log(author_entity_to_ec[e]) + author_entity_to_ec[e] / float(author_papers)) * query_entity_to_efiaf[e] for e in set(query_entities) & set(author_entity_to_ec.keys()))


def random_score(*args):
    return random()

def score_entities(exf, scoring_f, query_entities, authors):
    """
    Computes the score for a list of authors using entities approach
    """
    query_entity_to_efiaf = exf.ef_iaf_entities(query_entities)
    author_entity_to_ec = exf.authors_entity_to_ec(authors)
    results = []

    for author_id in authors:
        author_score = scoring_f(exf, query_entities, query_entity_to_efiaf, author_entity_to_ec[author_id], author_id)
        name = exf.data_layer.get_author_name(author_id)
        results.append({"name": name, "author_id": author_id, "score": author_score})
        logger.debug(u"%s score=%.3f", name, author_score)

    return sorted(results, key=lambda t: t["score"], reverse=True)

def lucene_max_score(author):
    """
    MAX among the scores associated to docs retrieved by Lucene for that author
    """
    return max(author["scores"].values())

def lucene_mean_score(author):
    """
    MEAN among the scores associated to docs retrieved by Lucene for that author
    """
    return mean(author["scores"].values())

def score_lucene(exf, scoring_f, authors):
    """
    Computes the score for a list of authors using term-based approach (from Lucene)
    """

    results = []
    for author_id in authors.keys():
        results.append({
            "author_id": author_id,
            "name": authors[author_id]["name"],
            "docs": authors[author_id]["docs"],
            "score": scoring_f(authors[author_id])
        })

    return sorted(results, key=lambda t: t["score"], reverse=True)

def score(exf, scoring_f, query_entities, authors):
    """
    Computes the score for a list of authors using a specific scoring_f function
    In case the score is for Lucene results query_entities is None
    """
    if scoring_f in ENTITIES_SCORING_FUNCTIONS:
        return score_entities(exf, scoring_f, query_entities, authors)
    elif scoring_f in LUCENE_SCORING_FUNCTIONS:
        return score_lucene(exf, scoring_f, authors)
    else:
        return []


def lucene_max_eciaf_score(exf, query_entities, entities_results, lucene_results):
    """
    Combine the score of each author obtained from the query on Lucene and on Entities
    """
    lucene_only_results = []
    results = []

    for lucene_result in lucene_results:
        entity_result = [e_res for e_res in entities_results if e_res["author_id"] == lucene_result["author_id"]]
        if entity_result:
            lucene_result["score"] += entity_result[0]["score"]
            entities_results.remove(entity_result[0])
            results.append(lucene_result)
        else:
            lucene_only_results.append(lucene_result)

    return results + lucene_only_results + entities_results

lucene_max_eciaf_score.ENTITIES_SCORING_FUNCTION = eciaf_score
lucene_max_eciaf_score.LUCENE_SCORING_FUNCTION = lucene_max_score

def lucene_max_eciaf_norm_score(exf, query_entities, entities_results, lucene_results):
    """
    Combine the score of each author obtained from the query on Lucene and on Entities
    """
    lucene_only_results = []
    results = []
    if lucene_results:
        max_lucene_result = lucene_results[0]["score"]
        for lucene_result in lucene_results:
            lucene_result["score"] = lucene_result["score"] / max_lucene_result
    if entities_results:
        max_entities_result = entities_results[0]["score"]
        for entities_result in entities_results:
            entities_result["score"] = entities_result["score"] / max_entities_result


    for lucene_result in lucene_results:
        entity_result = [e_res for e_res in entities_results if e_res["author_id"] == lucene_result["author_id"]]
        if entity_result:
            lucene_result["score"] += entity_result[0]["score"]
            entities_results.remove(entity_result[0])
            results.append(lucene_result)
        else:
            lucene_only_results.append(lucene_result)

    results = results + lucene_only_results + entities_results
    return sorted(results, key=lambda t: t["score"], reverse=True)

lucene_max_eciaf_norm_score.ENTITIES_SCORING_FUNCTION = eciaf_score
lucene_max_eciaf_norm_score.LUCENE_SCORING_FUNCTION = lucene_max_score


def lucene_max_eciaf_norm_rel_score(exf, query_entities, entities_results, lucene_results):
    """
    Combine the score of each author obtained from the query on Lucene and on Entities

    """
    lucene_only_results = []
    results = []

    if lucene_results:
        max_lucene_result = lucene_results[0]["score"]
        for lucene_result in lucene_results:
            lucene_result["score"] = lucene_result["score"] / max_lucene_result
    if entities_results:
        max_entities_result = entities_results[0]["score"]
        for entities_result in entities_results:
            entities_result["score"] = entities_result["score"] / max_entities_result


    for lucene_result in lucene_results:
        entity_result = [e_res for e_res in entities_results if e_res["author_id"] == lucene_result["author_id"]]
        if entity_result:
            lucene_result["score"] += entity_result[0]["score"]
            entities_results.remove(entity_result[0])
            results.append(lucene_result)
        else:
            lucene_only_results.append(lucene_result)


    for lucene_only_result in lucene_only_results:
        _compute_author_entity_rel(exf, lucene_only_result["author_id"], query_entities)

    results = results + lucene_only_results + entities_results
    return sorted(results, key=lambda t: t["score"], reverse=True)

lucene_max_eciaf_norm_rel_score.ENTITIES_SCORING_FUNCTION = eciaf_score
lucene_max_eciaf_norm_rel_score.LUCENE_SCORING_FUNCTION = lucene_max_score

def _compute_author_entity_rel(exf, author_id, query_entities):
    min_rho = 0.20
    author_entities = exf.data_layer.author_entities(author_id, min_rho)
    for query_entity in query_entities:
        logger.debug("Computing relatedness between author %s and query entity %s", author_id, query_entity)
        response = tagme.relatedness_title([(query_entity, entity) for entity in author_entities if is_ascii(entity)])
        author_rel = [response.get_relatedness(i) for i in range(len(response.relatedness))]
        logger.debug("MEAN with %s: %f", query_entity, mean(author_rel))
        logger.debug("MAX with %s: %f", query_entity, max(author_rel))
    return 2


ENTITIES_SCORING_FUNCTIONS = [efiaf_score, eciaf_score, log_ec_ef_iaf_score]
LUCENE_SCORING_FUNCTIONS = [lucene_max_score, lucene_mean_score]
MIX_SCORING_FUNCTIONS = [lucene_max_eciaf_score, lucene_max_eciaf_norm_score, lucene_max_eciaf_norm_rel_score]
