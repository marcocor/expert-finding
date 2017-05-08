import logging
import math
import time

from joblib.parallel import Parallel, delayed

def mean(numbers):
    return float(sum(numbers)) / max(len(numbers), 1)

def cossim_efiaf_score(exf, query_entities, query_entity_to_efiaf, author_id):
    author_entity_to_efiaf = dict((e[0], e[3])
                                  for e in exf.ef_iaf_author(author_id))

    return sum(author_entity_to_efiaf[e] * query_entity_to_efiaf[e] for e in set(author_entity_to_efiaf.keys()) & set(query_entity_to_efiaf.keys())) \
        / (math.sqrt(sum(author_entity_to_efiaf.values())) * math.sqrt(sum(query_entity_to_efiaf.values())))


def efiaf_score(exf, query_entities, query_entity_to_efiaf, author_entity_to_ec, author_id):
    author_papers = exf.data_layer.get_author_papers_count(author_id)
    # author_entity_to_ef = dict((t['_id'], t['document_count'] / float(author_papers))
                            #    for t in exf.data_layer.author_entity_frequency(author_id))
    return sum((author_entity_to_ec[e]/float(author_papers)) * query_entity_to_efiaf[e] for e in set(query_entities) & set(author_entity_to_ec.keys()))


def eciaf_score(exf, query_entities, query_entity_to_efiaf, author_entity_to_ec, author_id):
    # author_entity_to_ec = dict((t['_id'], t['document_count'])
    #                            for t in exf.data_layer.author_entity_frequency(author_id))
    return sum(author_entity_to_ec[e] * query_entity_to_efiaf[e] for e in set(query_entities) & set(author_entity_to_ec.keys()))


def log_ec_ef_iaf_score(exf, query_entities, query_entity_to_efiaf, author_entity_to_ec, author_id):
    author_papers = exf.data_layer.get_author_papers_count(author_id)
    # author_entity_to_ec = dict((t['_id'], t['document_count'])
    #                            for t in exf.data_layer.author_entity_frequency(author_id))
    return sum((math.log(author_entity_to_ec[e]) + author_entity_to_ec[e] / float(author_papers)) * query_entity_to_efiaf[e] for e in set(query_entities) & set(author_entity_to_ec.keys()))



def score(exf, scoring_f, query_entities, authors):
    query_entity_to_efiaf = exf.ef_iaf_entities(query_entities)
    author_entity_to_ec = exf.authors_entity_to_ec(authors)
    results = []

    for author_id in authors:
        author_score = scoring_f(exf, query_entities, query_entity_to_efiaf, author_entity_to_ec[author_id], author_id)
        name = exf.data_layer.get_author_name(author_id)
        results.append({"name": name, "author_id": author_id, "score": author_score})
        logging.debug(u"%s score=%.3f", name, author_score)

    return sorted(results, key=lambda t: t["score"], reverse=True)


def lucene_max_score(authors_scores):
    results = []
    for author_id in authors_scores.keys():
        results.append({
            "author_id": author_id,
            "name": authors_scores[author_id]["name"],
            "docs": authors_scores[author_id]["docs"],
            "score": max(authors_scores[author_id]["scores"].values())
        })

    return sorted(results, key=lambda t: t["score"], reverse=True)


def lucene_mean_score(authors_scores):
    results = []
    for author_id in authors_scores.keys():
        results.append({
            "author_id": author_id,
            "name": authors_scores[author_id]["name"],
            "docs": authors_scores[author_id]["docs"],
            "score": mean(authors_scores[author_id]["scores"].values())
        })

    return sorted(results, key=lambda t: t["score"], reverse=True)


ENTITIES_SCORING_FUNCTIONS = [efiaf_score, eciaf_score, log_ec_ef_iaf_score]#, cossim_efiaf_score]
LUCENE_SCORING_FUNCTIONS = [lucene_max_score, lucene_mean_score]
