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

from collections import Counter, namedtuple
from itertools import groupby
from math import log, exp
from numpy import clip, mean
from scipy import stats
from sqlitedict import SqliteDict


try:
    import lucene
except ImportError:
    logging.error("Cannot import Lucene")

import expertfinding
from expertfinding.core import data_layer, scoring

__all__ = []

DEFAULT_MIN_SCORE = 0.20

def legit_document(doc_body):
    return doc_body is not None and len(doc_body) > 10


def entities(text):
    return tagme.annotate(text).annotations if text else []


def set_cache(cache_dir):
    cache = pyfscache.FSCache(cache_dir)
    expertfinding.core.entities = cache(expertfinding.core.entities)


def join_entities_sql(entities):
    return u", ".join(u"'{}'".format(t.replace("'", "''")) for t in entities)

def weighted_geom_mean(vals_weights):
    return exp(sum(w * log(v) for v, w in vals_weights) / sum(w for _, w in vals_weights))

def _str_titles(t1, t2):
    return unicode(sorted([t1, t2])).encode("utf-8")

Author = namedtuple('Author', ['author_id', 'name', 'institution'])
class ExpertFindingBuilder(object):

    def __init__(self, ef):
        self.ef = ef
        self.ef.data_layer.initialize_db()

    def add_documents(self, input_f, papers_generator, min_year=None, max_year=None):
        papers = list(papers_generator)
        logging.info("%s: Number of papers (total): %d" % (os.path.basename(input_f), len(papers)))

        papers = [p for p in papers if
                  (min_year is None or p.year >= min_year) and (max_year is None or p.year <= max_year)]

        logging.info("%s: Number of papers (filtered) %d" % (os.path.basename(input_f), len(papers)))


        if papers:
            logging.info("%s: Number of papers (filtered) with abstract: %d" % (os.path.basename(input_f), sum(1 for p in papers if legit_document(p.abstract))))
            logging.info("%s: Number of papers (filtered) with DOI but no abstract %d" % (os.path.basename(input_f), sum(1 for p in papers if not legit_document(p.abstract) and p.doi)))


        papers.sort(key=lambda p: p.author_id)
        logging.debug("Papers sorted by author id")
        author_to_papers = groupby(papers, lambda p: Author(p.author_id, p.name, p.institution))
        logging.debug("Papers grouped by author_id")

        for author, papers_from_author in author_to_papers:
            self.ef.data_layer.add_papers_from_author(author, papers_from_author)

    def end(self):
        try:
            self.ef.index_writer.optimize()
            self.ef.index_writer.close()
        except Exception:
            logging.warn("Lucene is disabled")


class ExpertFinding(object):

    def __init__(self, storage_db, database_name, lucene_dir=None, erase=False, relatedness_dict_file=None):
        self.data_layer = data_layer.DataLayer(self, entities_fun=entities, storage_db=storage_db, database_name=database_name, erase=erase)        
        self.db = self.data_layer.db
        self.rel_dict = SqliteDict(relatedness_dict_file) if relatedness_dict_file else dict()
        try:
            lucene.initVM()
            logging.info("Lucene index directory: %s", lucene_dir)
            index_dir = lucene.SimpleFSDirectory(lucene.File(lucene_dir))
            self.analyzer = lucene.ClassicAnalyzer(lucene.Version.LUCENE_35)
            self.index_writer = lucene.IndexWriter(index_dir, self.analyzer, True, lucene.IndexWriter.MaxFieldLength.UNLIMITED)
            if not erase:
                self.index_searcher = lucene.IndexSearcher(index_dir)
        except Exception:
            logging.warn("Lucene disabled")

    def builder(self):
        return ExpertFindingBuilder(self)

    def author_entity_frequency_and_popularity(self, author_id):
        """
        Returns how many authors's papers have cited the entities cited by a specific author.
        """
        return self.db.execute(u'''
            SELECT e.entity, author_freq, SUM(e.frequency) AS entity_popularity,  years, max_rho
            FROM entities AS e,
            (
                SELECT entity, COUNT(DISTINCT(document_id)) as author_freq, GROUP_CONCAT(year) as years, MAX(rho) AS max_rho
                FROM entity_occurrences
                WHERE author_id == ? AND rho > ?
                GROUP BY entity
            ) as d_e
            WHERE d_e.entity == e.entity GROUP BY e.entity
            ''', (author_id, DEFAULT_MIN_SCORE)).fetchall()


    def get_authors_count(self, institution):
        """
        Returns how many authors are part of an institution.
        """
        return self.db.execute(u'''SELECT COUNT(*) FROM authors WHERE institution==?''', (institution,)).fetchall()[0][0]
 
    def ef_iaf_author(self, author_id):
        """
        Given an author, retrieve the entities cited by him, their EF and IAF.
        """
        total_papers = self.data_layer.total_papers()
        author_entity_frequency = self.author_entity_frequency_and_popularity(author_id)
        author_papers = self.author_papers_count(author_id)
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
        entity_popularities = self.data_layer.entity_popularity(entities)

        ef_iaf_dict = dict()
        for entity in entity_popularities:
            entity_name = entity['entity_name']
            entity_popularity = entity['entity_popularity']
            ef_iaf_dict[entity_name] = 1.0/len(entities) * log(total_papers/float(entity_popularity))

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
            author_to_entities_count[author_id] = dict((entity['entity_name'], entity['document_count']) for entity in author['entities'])

        return author_to_entities_count

    def institution_papers_count(self, institution):
        return self.db.execute(u'''
            SELECT document_count
            FROM "institutions"
            WHERE institution=?''', (institution,)).fetchall()[0][0]

    def author_id(self, author_name):
        return [r[0] for r in self.db.execute(u'''SELECT author_id FROM authors WHERE name=?''', (author_name,)).fetchall()]

    def institution(self, author_id):
        return self.db.execute(u'''SELECT institution FROM authors WHERE author_id=?''', (author_id,)).fetchall()[0][0]

    def grouped_entities(self, author_id, year=None, min_freq=None):
        contraints = []
        if year is not None:
            contraints.append('year=%d' % year)
        if min_freq is not None:
            contraints.append('COUNT(*)>=%d' % min_freq)
        having = "HAVING {}".format(" AND ".join(contraints)) if contraints else ""
        return self.db.execute(u'''SELECT entity, year, AVG(rho), MIN(rho), MAX(rho), GROUP_CONCAT(rho), COUNT(*)
           FROM entity_occurrences
           WHERE author_id=?
           GROUP BY entity, year
           {}
           ORDER BY year, COUNT(*) DESC'''.format(having), author_id).fetchall()

    def papers_count(self):
        return self.db.execute(u'''
            SELECT author_id, COUNT(DISTINCT(document_id))
            FROM "entity_occurrences"
            GROUP BY author_id''').fetchall()

    def print_documents_quantiles(self):
        papers_count = zip(*self.papers_count())[1]
        print "number of documents: {}".format(sum(papers_count))
        print "number of authors: {}".format(len(papers_count))
        quantiles = stats.mstats.mquantiles(papers_count, prob=[n / 10.0 for n in range(10)])
        print "quantiles:", quantiles
        for i in range(len(quantiles)):
            begin = int(quantiles[i])
            end = int(quantiles[i + 1]) - 1 if i < len(quantiles) - 1 else max(papers_count)
            print "{} authors have {}-{} documents with abstract".format(sum(1 for c in papers_count if begin <= c <= end), begin, end)


    def authors_completion(self, terms):
        """
        Returns author names autocompletion for terms.
        """
        return self.db.execute(u'''SELECT * FROM "authors" WHERE name LIKE ? LIMIT 50''', (u"%{}%".format(terms),)).fetchall()

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
        e_a_f = self.author_entity_frequency(author_id)
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


    def find_expert(self, input_query, scoring_functions=scoring.ENTITIES_SCORING_FUNCTIONS):
        logging.debug(u"Processing query: {}".format(input_query))
        start_time = time.time()
        query_entities =  list(set(a.entity_title for a in entities(input_query)))
        logging.debug(u"Found the following entities in the query: {}".format(u",".join(query_entities)))
        authors = self.data_layer.citing_authors(query_entities) 
        logging.debug(u"Found %d authors that matched the query, computing score for each of them." % len(authors))

        results = {}
        for scoring_f in scoring_functions:
            start_time = time.time()
            scoring_f_name = scoring_f.__name__.replace("_score", "")
            results[scoring_f_name] = scoring.score(self, scoring_f, query_entities, authors)
            runtime = time.time() - start_time
            results["time_" + scoring_f_name] = runtime
            logging.info("Query completed in %.3f sec", runtime)

        results["query_entities"] = list(query_entities)
        return results

    def find_expert_lucene(self, input_query, scoring_functions=scoring.LUCENE_SCORING_FUNCTIONS):
        logging.debug(u"Processing Lucene query: {}".format(input_query))
        start_time = time.time()
        query = lucene.QueryParser(lucene.Version.LUCENE_35, "text", self.analyzer).parse(input_query)
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
        
        results = {}
        for scoring_f in scoring_functions:
            results[scoring_f.func_name.replace("_score", "")] = scoring_f(query_result)
        
        return results