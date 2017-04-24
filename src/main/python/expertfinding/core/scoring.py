import logging
import math

def mean(numbers):
    return float(sum(numbers)) / max(len(numbers), 1)


def cossim_efiaf_score(self, query_entities, author_id):
    author_entity_to_efiaf = dict((e[0], e[3]) for e in self.ef_iaf_author(author_id))
    query_entity_to_efiaf = self.ef_iaf_entities(query_entities)
    
    return sum(author_entity_to_efiaf[e] * query_entity_to_efiaf[e] for e in set(author_entity_to_efiaf.keys()) & set(query_entity_to_efiaf.keys())) \
        / (math.sqrt(sum(author_entity_to_efiaf.values())) * math.sqrt(sum(query_entity_to_efiaf.values())))

def efiaf_score(self, query_entities, author_id):
    author_papers = self.author_papers_count(author_id)
    author_entity_to_ef = dict((t[0], t[1]/float(author_papers)) for t in self.author_entity_frequency(author_id))
    query_entity_to_efiaf = self.ef_iaf_entities(query_entities)
    return sum(author_entity_to_ef[e] * query_entity_to_efiaf[e] for e in set(query_entities) & set(author_entity_to_ef.keys()))

def eciaf_score(self, query_entities, author_id):
    author_entity_to_ec = dict((t[0], t[1]) for t in self.author_entity_frequency(author_id))
    query_entity_to_efiaf = self.ef_iaf_entities(query_entities)
    return sum(author_entity_to_ec[e] * query_entity_to_efiaf[e] for e in set(query_entities) & set(author_entity_to_ec.keys()))

def log_ec_ef_iaf_score(self, query_entities, author_id):
    author_papers = self.author_papers_count(author_id)
    author_entity_to_ec = dict((t[0], t[1]) for t in self.author_entity_frequency(author_id))
    query_entity_to_efiaf = self.ef_iaf_entities(query_entities)
    return sum((math.log(author_entity_to_ec[e]) + author_entity_to_ec[e]/float(author_papers)) * query_entity_to_efiaf[e] for e in set(query_entities) & set(author_entity_to_ec.keys()))

def score(exf, scoring_f, query_entities, authors):
    results = []
    for author_id in authors:
        score = scoring_f(exf, query_entities, author_id)
        name = exf.name(author_id)
        results.append({"name":name, "author_id":author_id, "score":score})
        logging.debug(u"%s score=%.3f", name, score)

    return sorted(results, key=lambda t: t["score"], reverse=True)

def lucene_max_score(authors_scores):
    results = []
    for author_id in authors_scores.keys():
        results.append({
            "author_id": author_id,
            "name": authors_scores[author_id]["name"],
            "score": max(authors_scores[author_id]["scores"])
        })
    
    return sorted(results, key=lambda t: t["score"], reverse=True)

def lucene_mean_score(authors_scores):
    results = []
    for author_id in authors_scores.keys():
        results.append({
            "author_id": author_id,
            "name": authors_scores[author_id]["name"],
            "score": mean(authors_scores[author_id]["scores"])
        })

    return sorted(results, key=lambda t: t["score"], reverse=True)

ENTITIES_SCORING_FUNCTIONS = [cossim_efiaf_score, efiaf_score, eciaf_score, log_ec_ef_iaf_score]
LUCENE_SCORING_FUNCTIONS = [lucene_max_score, lucene_mean_score]