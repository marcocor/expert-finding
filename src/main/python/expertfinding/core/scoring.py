import json
import logging
import math
import time
import tagme
from expertfinding import wiki_util

from random import random
from multiprocess.dummy import Pool

logger = logging.getLogger("EF_log")

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

def mean(numbers, div=1.0):
    if div == 1.0:
        div =  max(len(numbers), div)
    return float(sum(numbers)) / div


class ScoringStructure():
    lucene_scoring = []
    entities_scoring = []
    mix_scoring = []
    def __init__(self, scoring_representation):
        scoring_representation = json.loads(scoring_representation)
        for scoring_f in scoring_representation:
            self.add_scoring_function(scoring_f)
    
    def add_scoring_function(self, scoring_f):
        if ENTITIES_SCORING_FUNCTIONS.has_key(scoring_f["name"]):
            self.entities_scoring.append((ENTITIES_SCORING_FUNCTIONS[scoring_f["name"]], scoring_f["weight"], scoring_f.get("query_expansion", 0)))
        elif LUCENE_SCORING_FUNCTIONS.has_key(scoring_f["name"]):
            self.lucene_scoring.append((LUCENE_SCORING_FUNCTIONS[scoring_f["name"]], scoring_f["weight"]))
        elif MIX_SCORING_FUNCTIONS.has_key(scoring_f["name"]):
            self.mix_scoring.append((MIX_SCORING_FUNCTIONS[scoring_f["name"]], scoring_f["weight"]))
        else:
            raise Exception("Unrecognized function: ", scoring_f["name"])

def cossim_efiaf_score(exf, query_entity_to_efiaf, author_id):
    author_entity_to_efiaf = dict((e[0], e[3])
                                  for e in exf.ef_iaf_author(author_id))

    return sum(author_entity_to_efiaf[e] * query_entity_to_efiaf[e] for e in set(author_entity_to_efiaf.keys()) & set(query_entity_to_efiaf.keys())) \
        / (math.sqrt(sum(author_entity_to_efiaf.values())) * math.sqrt(sum(query_entity_to_efiaf.values())))


def efiaf_score(exf, query_entity_to_efiaf, author_entity_to_ec, author_id):
    author_papers = exf.data_layer.get_author_papers_count(author_id)
    return sum((author_entity_to_ec[e] / float(author_papers)) * query_entity_to_efiaf[e] for e in set(query_entity_to_efiaf.keys()) & set(author_entity_to_ec.keys()))


def eciaf_score(exf, query_entity_to_efiaf, author_entity_to_ec, author_id):
    return sum(author_entity_to_ec[e] * query_entity_to_efiaf[e] for e in set(query_entity_to_efiaf.keys()) & set(author_entity_to_ec.keys()))


def log_ec_ef_iaf_score(exf, query_entity_to_efiaf, author_entity_to_ec, author_id):
    author_papers = exf.data_layer.get_author_papers_count(author_id)
    return sum((math.log(author_entity_to_ec[e]) + author_entity_to_ec[e] / float(author_papers)) * query_entity_to_efiaf[e] for e in set(query_entity_to_efiaf.keys()) & set(author_entity_to_ec.keys()))


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
        author_score = scoring_f(exf, query_entity_to_efiaf, author_entity_to_ec[author_id], author_id)
        name = exf.data_layer.get_author_name(author_id)
        results.append({"name": name, "author_id": author_id, "score": author_score})
        # logger.debug(u"%s score=%.3f", name, author_score)

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


def score_mix(exf, scoring_f, input_query, authors):
    """
    Computes the score for a list of authors using entities approach
    """
    results = []
    for author_id in authors:
        author_score = scoring_f(exf, input_query, author_id)
        name = exf.data_layer.get_author_name(author_id)
        results.append({"name": name, "author_id": author_id, "score": author_score})
    return sorted(results, key=lambda t: t["score"], reverse=True)


def author_entities_relatedness_score(exf, input_query, author_id):
    query_entities = exf.get_query_entities(input_query, query_expansion=0)
    return _compute_author_entities_rel(exf, author_id, query_entities)

def _compute_author_entities_rel(exf, author_id, query_entities):
    min_rho = 0.20
    author_rel = []
    author_entities = exf.data_layer.author_entities(author_id, min_rho)
    total_papers = exf.data_layer.total_papers()
    if not author_entities:
        return 0

    for query_entity_title, query_entity_id in query_entities:
        author_qe_rel = _compute_author_entity_rel(author_entities, query_entity_id, total_papers)
        logger.debug("Similarity between author %s and query entity %s is %f", author_id, query_entity_title, author_qe_rel)
        author_rel.append(author_qe_rel)
    return mean(author_rel)

def _compute_author_entity_rel(author_entities, query_entity, total_papers):
    top_k_results = 5
    entities = author_entities.values()
    response = wiki_util.multi_rel([(query_entity, entity["entity_id"]) for entity in entities], method="milnewitten")
    # Collect entities together with relatedness wrt query_entity + sort by relatedness
    results = sorted([{
        "entity": entities[i],
        "relatedness": response[i],
        "score": entities[i]["score"] * response[i] * math.log(1 + entities[i]["document_count"])
    } for i in range(len(response))], key=lambda t: t["score"] , reverse=True)
    # take just top_k related entities
    results = [res["score"] for res in results[:top_k_results]]

    return mean(results)

def _normalize(result):
    if not result:
        return

    max_score = result[0]["score"]
    if not max_score:
        max_score = 1

    for res in result:
        res["score"] = res["score"] / max_score

def _merge_scores(author_id, author_scores, weights):
    return sum(weights[i] * author_score.get(author_id, 0) for i, author_score in enumerate(author_scores)) / sum(weights)

def normalize_merge(partial_results):
    author_to_scores = []
    weights = []

    author_ids = []
 
    for partial_result, weight in partial_results:
        _normalize(partial_result)
        author_ids = list(set(author_ids + [partial_res["author_id"] for partial_res in partial_result]))
        author_to_scores.append(dict((partial_res["author_id"], partial_res["score"]) for partial_res in partial_result))
        weights.append(weight)

    results = []
    for author_id in author_ids:
        results.append({
            "author_id": author_id,
            "name": author_id,
            "score": _merge_scores(author_id, author_to_scores, weights)
        })

    return sorted(results, key=lambda t: t["score"], reverse=True)

ENTITIES_SCORING_FUNCTIONS = dict((func.__name__, func) for func in [eciaf_score, efiaf_score, log_ec_ef_iaf_score])
LUCENE_SCORING_FUNCTIONS = dict((func.__name__, func) for func in [lucene_max_score, lucene_mean_score])
MIX_SCORING_FUNCTIONS = dict((func.__name__, func) for func in [author_entities_relatedness_score])
