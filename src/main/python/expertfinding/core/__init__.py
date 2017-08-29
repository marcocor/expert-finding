from astroid.__pkginfo__ import author
import logging
import math
import os
import pyfscache
import re
import multiprocessing
import string
import tagme
import time

import expertfinding.wiki_util as wiki_util
from expertfinding.cache_fs import Cache

from collections import Counter, namedtuple
from itertools import groupby
from math import log, exp
from numpy import clip, mean
from scipy import stats
from sqlitedict import SqliteDict

logger = logging.getLogger("EF_log")

try:
    import lucene
except ImportError:
    logger.error("Cannot import Lucene")

import expertfinding
from expertfinding.core import data_layer, scoring

__all__ = []

def legit_document(doc_body):
    return doc_body is not None and len(doc_body) > 10


def entities(text):
    if text:
        return [annotation for annotation in tagme.annotate(text).annotations if annotation.entity_title is not None] 
    else:
        return []


def set_cache(expcache):
    # cache = pyfscache.FSCache(cache_dir)
    expertfinding.core.entities = expcache.cache(expertfinding.core.entities)
    expertfinding.wiki_util.set_cache(expcache)


def weighted_geom_mean(vals_weights):
    return exp(sum(w * log(v) for v, w in vals_weights) / sum(w for _, w in vals_weights))

def _str_titles(t1, t2):
    return unicode(sorted([t1, t2])).encode("utf-8")

def lucene_escape(query):
    return query.replace("(","").replace(")","").replace("*","")

Author = namedtuple('Author', ['author_id', 'name', 'institution'])
class ExpertFindingBuilder(object):

    def __init__(self, ef):
        self.ef = ef
        self.ef.data_layer.initialize_db()

    def add_documents(self, input_f, papers_generator, min_year=None, max_year=None):
        papers = list(papers_generator)
        logger.info("%s: Number of papers (total): %d" % (os.path.basename(input_f), len(papers)))

        papers = [p for p in papers if
                  (min_year is None or p.year >= min_year) and (max_year is None or p.year <= max_year)]

        logger.info("%s: Number of papers (filtered) %d" % (os.path.basename(input_f), len(papers)))

        if papers:
            logger.info("%s: Number of papers (filtered) with abstract: %d" % (os.path.basename(input_f), sum(1 for p in papers if legit_document(p.abstract))))
            logger.info("%s: Number of papers (filtered) with DOI but no abstract %d" % (os.path.basename(input_f), sum(1 for p in papers if not legit_document(p.abstract) and p.doi)))

        papers.sort(key=lambda p: p.author_id)
        logger.debug("Papers sorted by author id")
        author_to_papers = groupby(papers, lambda p: Author(p.author_id, p.name, p.institution)._asdict())
        logger.debug("Papers grouped by author_id")

        for author, papers_from_author in author_to_papers:
            self.ef.data_layer.add_papers_from_author(author, papers_from_author)

    def end(self):
        try:
            self.ef.index_writer.optimize()
            self.ef.index_writer.close()
        except Exception:
            logger.warn("Lucene is disabled")

class ExpertFinding(object):
    QUERY_SCORE_THRESHOLD = 0.20

    def __init__(self, database_name, lucene_dir=None, wiki_api_endpoint=None, erase=False, cache_dir=None, relatedness_dict_file=None):
        self.data_layer = data_layer.DataLayer(self, entities_fun=entities, database_name=database_name, erase=erase)        
        # self.db = self.data_layer.db
        self.rel_dict = SqliteDict(relatedness_dict_file) if relatedness_dict_file else dict()
        wiki_util.API_ENDPOINT = wiki_api_endpoint
        try:
            lucene.initVM()
            logger.info("Lucene index directory: %s", lucene_dir)
            index_dir = lucene.SimpleFSDirectory(lucene.File(lucene_dir))
            self.analyzer = lucene.ClassicAnalyzer(lucene.Version.LUCENE_35)
            if erase:
                self.index_writer = lucene.IndexWriter(index_dir, self.analyzer, True, lucene.IndexWriter.MaxFieldLength.UNLIMITED)
            else:
                self.index_searcher = lucene.IndexSearcher(index_dir)
        except Exception as e:
            logger.error(e)
            logger.warn("Lucene disabled")

        if cache_dir:
            self.cache = Cache(cache_dir, readonly=True)
            set_cache(self.cache)
    
    def close(self):
        self.cache.close()

    def builder(self):
        return ExpertFindingBuilder(self)

    def ef_iaf_author(self, author_id):
        """
        Given an author, retrieve the entities cited by him, their EF and IAF.
        """
        total_papers = self.data_layer.total_papers()
        author_entity_frequency = self.author_entity_frequency_and_popularity(author_id)
        author_papers = self.data_layer.get_author_papers_count(author_id)
        return sorted(((
                 entity,
                 entity_author_freq / float(author_papers),
                 log(total_papers/float(entity_popularity)),
                 entity_author_freq / float(author_papers) * log(total_papers/float(entity_popularity)),
                 max_rho,
                 [int(y) for y in years.split(",")],
                ) for entity, entity_author_freq, entity_popularity,  years, max_rho in author_entity_frequency), key=lambda t: t[3], reverse=True)

    def ef_iaf_entities(self, entities):
        total_papers = self.data_layer.total_papers()
        total_authors = self.data_layer.total_authors()
        entity_popularities = self.data_layer.entity_popularity([entity_id for _, entity_id in entities])

        ef_iaf_dict = dict()
        for entity in entity_popularities:
            entity_id = entity["entity_id"]
            entity_name = entity['entity_name']
            entity_popularity = entity['entity_popularity']
            ef_iaf_dict[entity_id] = 1.0/len(entities) * log(total_authors/float(entity_popularity))

        return ef_iaf_dict

    def authors_entity_to_ec(self, authors):
        """
        For every author (author_id) generates a map from entity name to document count
        (document of the author in which the entity appears)
        """
        res = self.data_layer.authors_entity_frequency(authors)
        author_to_entities_count = dict()

        for author in res:
            author_id = author['_id']
            author_to_entities_count[author_id] = dict((entity['entity_id'], entity["score"] * entity["pr_score"] * entity['document_count']) for entity in author['entities'])
            # author_to_entities_count[author_id] = dict((entity['entity_id'], entity["score"] * entity['document_count']) for entity in author['entities'])

        return author_to_entities_count

    def print_documents_quantiles(self):
        papers_count = self.data_layer.total_papers()
        authors_count = self.data_layer.total_authors()
        print "number of documents: {}".format(papers_count)
        print "number of authors: {}".format(authors_count)
        
        for author_id in self.data_layer.all_authors():
            author_entities = self.data_layer.author_entities(author_id, self.QUERY_SCORE_THRESHOLD)
            print len(author_entities)

        # quantiles = stats.mstats.mquantiles(papers_count, prob=[n / 10.0 for n in range(10)])
        # print "quantiles:", quantiles
        # for i in range(len(quantiles)):
        #     begin = int(quantiles[i])
        #     end = int(quantiles[i + 1]) - 1 if i < len(quantiles) - 1 else max(papers_count)
        #     print "{} authors have {}-{} documents with abstract".format(sum(1 for c in papers_count if begin <= c <= end), begin, end)


    def authors_completion(self, author_name):
        """
        Returns author names autocompletion for terms.
        """
        return self.data_layer.complete_author_name(author_name)

    def _prefetch_relatedness(self, entity_group_1, entity_group_2):
        pairs = ((e1, e2) for e1 in entity_group_1 for e2 in entity_group_2)
        pairs_to_retrieve = [p for p in pairs if _str_titles(*p) not in self.rel_dict]
        if pairs_to_retrieve:
            for titles, rel in tagme.relatedness_title(pairs_to_retrieve):
                self.rel_dict[_str_titles(*titles)] = rel
        for p in pairs:
            assert _str_titles(*p) in self.rel_dict
        self.rel_dict.commit()

    def relatedness_geom(self, query_entities, author_id):
        e_a_f = self.data_layer.author_entity_frequency(author_id)
        author_entity_to_ec = dict((t[0], t[1]) for t in e_a_f)
        author_entity_to_maxrho = dict((t[0], t[3]) for t in e_a_f)
        
        alpha = 10.0**-5
        x = 10.0
        self._prefetch_relatedness(query_entities, author_entity_to_ec.keys())
        
        relatedness_weights = {}
        for q_entity in query_entities:
            q_entity_relatedness = [(a_entity, self.rel_dict[_str_titles(q_entity, a_entity)]) for a_entity in author_entity_to_ec.keys()]
            val_weights = [(1.0 - r**x + alpha, author_entity_to_ec[a_entity] * author_entity_to_maxrho[a_entity]) for a_entity, r in q_entity_relatedness]
            relatedness_weights[q_entity] = val_weights

        return mean([clip(1 - weighted_geom_mean(relatedness_weights[q_entity]) + alpha, 0.0, 1.0) ** (1.0/x) for q_entity in relatedness_weights])

    def get_query_entities(self, input_query, query_expansion=0):
        """

        """
        query_entities = dict((a.entity_id, (a.entity_title, a.entity_id)) for a in entities(input_query) if a.score >= self.QUERY_SCORE_THRESHOLD)
        if not query_entities:
            query_entities = dict((a.entity_id, (a.entity_title, a.entity_id)) for a in entities(input_query))


        if query_expansion:
            topk1 = 50
            topk2 = 20
            # query_expansion = dict((e["dstWikiID"], (e["dstWikiTitle"], e["dstWikiID"])) for e in wiki_util.text(input_query)[:topk1])
            # query_expansion2 = dict((e["dstWikiID"], (e["dstWikiTitle"], e["dstWikiID"])) for e in wiki_util.rank(query_entities, "milnewitten"))
            query_expansion3 = dict((e["dstWikiID"], (e["dstWikiTitle"], e["dstWikiID"])) for e in wiki_util.rank(query_entities, "jaccard", query_expansion))
            # query_entities.update(query_expansion)
            # query_entities.update(query_expansion2)
            query_entities.update(query_expansion3)

        return query_entities.values()

    def find_expert_entities(self, input_query, scoring_function, query_expansion=0):
        logger.info(u"Processing Entities query: {}".format(input_query))

        query_entities = self.get_query_entities(input_query, query_expansion=query_expansion)
        # logger.debug(u"Found the following entities in the query: {}".format(u",".join([entity_title for entity_title, _ in query_entities])))
        authors = self.data_layer.citing_authors([entity_id for _, entity_id in query_entities])
        logger.info(u"Found %d authors that matched query entities, computing score for each of them." % len(authors))

        return scoring.score_entities(self, scoring_function, query_entities, authors)

    def find_expert_lucene(self, input_query, scoring_function):
        logger.debug(u"Processing Lucene query: {}".format(input_query))
        query = lucene.QueryParser(lucene.Version.LUCENE_35, "text", self.analyzer).parse(lucene_escape(input_query))
        hits = self.index_searcher.search(query, 40)
        query_result = {}

        for hit in hits.scoreDocs:
            doc = self.index_searcher.doc(hit.doc)
            document_id = doc.get('document_id')
            author_id = doc.get("author_id")
            doc_score = hit.score
            author_score = query_result.get(author_id, {'name': doc.get('author_name'), 'docs': {}, 'scores': {}})
            author_score['docs'][document_id] = {'year':doc.get('year')}
            author_score['scores'][document_id] = doc_score
            query_result[author_id] = author_score

        return scoring.score_lucene(self, scoring_function, query_result)

    def find_expert_mix(self, input_query, authors_id, scoring_function):
        logger.info(u"Processing mix query: {}".format(input_query))
        return scoring.score_mix(self, scoring_function, input_query, authors_id)


    def find_expert(self, input_query, scoring_structure):
        scores = []

        query_entities = [entity_name for entity_name, entity_id in self.get_query_entities(input_query)]
        start_time = time.time()

        entities_scoring_functions = scoring_structure.entities_scoring
        for (entities_scoring_f, weight, query_expansion) in entities_scoring_functions:
            scores.append((self.find_expert_entities(input_query, entities_scoring_f, query_expansion), weight))

        lucene_scoring_functions = scoring_structure.lucene_scoring
        for (lucene_scoring_f, weight) in lucene_scoring_functions:
            scores.append((self.find_expert_lucene(input_query, lucene_scoring_f), weight))
        
        authors_id = list(set([hit["author_id"] for partial_result, _ in scores for hit in partial_result]))
        mix_scoring_functions = scoring_structure.mix_scoring
        for (mix_scoring_f, weight) in mix_scoring_functions:
            scores.append((self.find_expert_mix(input_query, authors_id, mix_scoring_f), weight))

        results = scoring.normalize_merge(scores)
        runtime = time.time() - start_time
        logger.info("Query completed in %.3f sec", runtime)
        return results ,runtime, query_entities


    def find_expert_(self, input_query):
        scores = []
        scores.append(self.find_expert_entities(input_query, scoring.eciaf_score))
        scores.append(self.find_expert_entities(input_query, scoring.eciaf_score, 20))
        scores.append(self.find_expert_lucene(input_query, scoring.lucene_max_score))
        authors_id = list(set([hit["author_id"] for partial_result in scores for hit in partial_result]))
        scores.append(self.find_expert_mix(input_query, authors_id, scoring.author_entities_relatedness_score))
        
        return scores
