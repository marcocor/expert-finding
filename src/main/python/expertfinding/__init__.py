from astroid.__pkginfo__ import author
import cgi
from collections import Counter
import logging
from math import log, exp
import math
from numpy import mean
import os
import pyfscache
import re
from scipy import stats
import sqlite3
from sqlitedict import SqliteDict
import string
import tagme
import time

import expertfinding


__all__ = []

DEFAULT_MIN_SCORE = 0.20

def legit_document(doc_body):
    return doc_body is not None and len(doc_body) > 10


def entities(text):
    return tagme.annotate(text).annotations if text else []


def set_cache(cache_dir):
    cache = pyfscache.FSCache(cache_dir)
    expertfinding.entities = cache(expertfinding.entities)


def _annotated_text_generator(text, annotations):
    prev = 0
    for a in sorted(annotations, key=lambda a: a.begin):
        yield cgi.escape(text[prev:a.begin])
        yield u"<span class='annotation' entity='{}' score='{}'>{}</span>".format(cgi.escape(a.entity_title or ""), a.score, cgi.escape(text[a.begin: a.end]))
        prev = a.end
    yield text[prev:]


def annotated_text(text, annotations):
    return "".join(_annotated_text_generator(text, annotations))


def join_entities_sql(entities):
    return u", ".join(u"'{}'".format(t.replace("'", "''")) for t in entities)

def weighted_geom_mean(vals_weights):
    return exp(sum(w * log(v) for v, w in vals_weights) / sum(w for _, w in vals_weights))

def _str_titles(t1, t2):
    return unicode(sorted([t1, t2])).encode("utf-8")

class ExpertFindingBuilder(object):

    def __init__(self, ef):
        self.ef = ef
        self.ef.db.execute('''CREATE TABLE IF NOT EXISTS authors
             (author_id PRIMARY KEY, name, institution)
             ''')
        self.ef.db.execute('''CREATE TABLE IF NOT EXISTS entity_occurrences
             (entity, author_id, document_id, year, rho,
             FOREIGN KEY(author_id) REFERENCES authors(author_id))''')
        self.ef.db.execute('''CREATE TABLE IF NOT EXISTS documents
             (author_id, document_id, year, body,
             FOREIGN KEY(author_id) REFERENCES authors(author_id))''')
        self.ef.db.execute('''CREATE TABLE IF NOT EXISTS institutions
             (institution PRIMARY KEY, document_count)''')
        self.ef.db.execute('''CREATE TABLE IF NOT EXISTS entities
             (entity, institution, frequency, PRIMARY KEY (entity, institution))''')
        self.ef.db.execute('''CREATE INDEX IF NOT EXISTS entities_author_id_index ON entity_occurrences (author_id)''')
        self.ef.db.execute('''CREATE INDEX IF NOT EXISTS entities_entity_index ON entities (entity)''')
        self.ef.db.execute('''CREATE INDEX IF NOT EXISTS entity_occurrences_entity_index ON entity_occurrences (entity)''')

    def add_documents(self, input_f, papers_generator, min_year=None, max_year=None):
        papers = list(papers_generator)
        logging.info("%s: Number of papers (total): %d" % (os.path.basename(input_f), len(papers)))

        papers = [p for p in papers if
                  (min_year is None or p.year >= min_year) and (max_year is None or p.year <= max_year)]

        logging.info("%s: Number of papers (filtered) %d" % (os.path.basename(input_f), len(papers)))
        if papers:
            logging.info("%s: Number of papers (filtered) with abstract: %d" % (os.path.basename(input_f), sum(1 for p in papers if legit_document(p.abstract))))
            logging.info("%s: Number of papers (filtered) with DOI but no abstract %d" % (os.path.basename(input_f), sum(1 for p in papers if not legit_document(p.abstract) and p.doi)))

        document_id = self._next_paper_id()
        for p in papers:
            self._add_author(p.author_id, p.name, p.institution)
            if (legit_document(p.abstract)):
                ent = entities(p.abstract)
                self._add_entities(p.author_id, document_id, p.year, p.institution, ent)
                self._add_document_body(p.author_id, document_id, p.year, p.abstract, ent)
                document_id += 1
        self.ef.db_connection.commit()

    def entities(self, author_id):
        return self.ef.db.execute('''SELECT year, entity, rho FROM entity_occurrences WHERE author_id=?''', (author_id,)).fetchall()

    def _add_entities(self, author_id, document_id, year, institution, annotations):
        self.ef.db.executemany('INSERT INTO entity_occurrences VALUES (?,?,?,?,?)', ((a.entity_title, author_id, document_id, year, a.score) for a in annotations))
        unique_entities = set(a.entity_title for a in annotations)
        self.ef.db.executemany('''INSERT OR IGNORE INTO entities VALUES (?,?,0)''', ((e, institution) for e in unique_entities))
        self.ef.db.executemany('''UPDATE entities
                                  SET frequency = frequency + 1
                                  WHERE entity=? AND institution=?''', ((e, institution) for e in unique_entities))
        self.ef.db.execute('''INSERT OR IGNORE INTO institutions VALUES (?,0)''', (institution,))
        self.ef.db.execute('''UPDATE institutions
                              SET document_count = document_count + 1
                              WHERE institution=?''', (institution,))

    def _add_document_body(self, author_id, document_id, year, body, annotations):
        annotated_t = annotated_text(body, annotations)
        self.ef.db.execute('INSERT INTO documents VALUES (?,?,?,?)', (author_id, document_id, year, annotated_t))

    def _next_paper_id(self):
        return self.ef.db.execute('SELECT IFNULL(MAX(document_id), -1) FROM entity_occurrences').fetchall()[0][0] + 1

    def _add_author(self, author_id, name, institution):
        self.ef.db.execute('INSERT OR IGNORE INTO authors VALUES (?,?,?)', (author_id, name, institution))


class ExpertFinding(object):

    def __init__(self, storage_db, erase=False, relatedness_dict_file=None):
        if erase and os.path.isfile(storage_db):
            os.remove(storage_db)
        self.db_connection = sqlite3.connect(storage_db)
        self.db = self.db_connection.cursor()
        self.rel_dict = SqliteDict(relatedness_dict_file) if relatedness_dict_file else dict()

    def builder(self):
        return ExpertFindingBuilder(self)

    def author_entity_frequency(self, author_id):
        """
        Returns how many authors's papers have cited the entities cited by a specific author.
        """
        return self.db.execute(u'''
            SELECT entity, COUNT(DISTINCT(document_id)) as author_freq, GROUP_CONCAT(year) as years, MAX(rho) AS max_rho
            FROM entity_occurrences
            WHERE author_id == ? AND rho > ?
            GROUP BY entity
            ''', (author_id, DEFAULT_MIN_SCORE)).fetchall()

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

    def entity_popularity(self, entities):
        """
        """
        return self.db.execute(u'''
            SELECT entity, SUM(frequency) AS entity_popularity
            FROM entities
            WHERE entity IN ({})
            GROUP BY entity
            '''.format(join_entities_sql(entities))).fetchall()

    def get_authors_count(self, institution):
        """
        Returns how many authors are part of an institution.
        """
        return self.db.execute(u'''SELECT COUNT(*) FROM authors WHERE institution==?''', (institution,)).fetchall()[0][0]

    def total_papers(self):
        return self.db.execute(u'''SELECT COUNT(*) FROM documents''').fetchall()[0][0]
            
    def ef_iaf_author(self, author_id):
        """
        Given an author, retrieve the entities cited by him, their EF and IAF.
        """
        total_papers = self.total_papers()
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
        total_papers = self.total_papers()
        query_entity_popularity = dict(self.entity_popularity(entities))
        return dict((e, 1.0/len(entities) * log(total_papers/float(query_entity_popularity[e]))) for e in query_entity_popularity.keys())


    def author_papers_count(self, author_id):
        return self.db.execute(u'''SELECT COUNT(DISTINCT(document_id)) FROM entity_occurrences WHERE author_id=?''', (author_id,)).fetchall()[0][0]

    def institution_papers_count(self, institution):
        return self.db.execute(u'''
            SELECT document_count
            FROM "institutions"
            WHERE institution=?''', (institution,)).fetchall()[0][0]

    def author_id(self, author_name):
        return [r[0] for r in self.db.execute(u'''SELECT author_id FROM authors WHERE name=?''', (author_name,)).fetchall()]

    def document(self, doc_id):
        return self.db.execute(u'''SELECT author_id, document_id, year, body FROM documents WHERE document_id=?''', (doc_id,)).fetchone()

    def documents(self, author_id, entities):
        return self.db.execute(u'''
            SELECT document_id, year, entity, COUNT(*)
            FROM entity_occurrences
            WHERE author_id=? AND entity IN ({})
            GROUP BY document_id, entity'''.format(join_entities_sql(entities)), (author_id,)).fetchall()

    def institution(self, author_id):
        return self.db.execute(u'''SELECT institution FROM authors WHERE author_id=?''', (author_id,)).fetchall()[0][0]

    def name(self, author_id):
        return self.db.execute(u'''SELECT name FROM authors WHERE author_id=?''', (author_id,)).fetchall()[0][0]

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

    def citing_authors(self, entities):
        """
        Returns the list of authors citing any of the entities passed by arguments.
        """
        result = self.db.execute(u'''SELECT DISTINCT(author_id)
            FROM "entity_occurrences"
            WHERE entity IN ({}) AND rho > ?'''.format(join_entities_sql(entities)), (DEFAULT_MIN_SCORE,)).fetchall()
        return [t[0] for t in result]

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

    def relatedness_geom(self, query_entities, author_id):
        e_a_f = self.author_entity_frequency(author_id)
        author_entity_to_ec = dict((t[0], t[1]) for t in e_a_f)
        author_entity_to_maxrho = dict((t[0], t[3]) for t in e_a_f)
        
        alpha = 0.1
        self._prefetch_relatedness(query_entities, author_entity_to_ec.keys())
        
        relatedness_weights = {}
        for q_entity in query_entities:
            q_entity_relatedness = [(a_entity, self.rel_dict[_str_titles(q_entity, a_entity)]) for a_entity in author_entity_to_ec.keys()] 
            val_weights = [(1.0 - r + alpha, author_entity_to_ec[a_entity] * author_entity_to_maxrho[a_entity]) for a_entity, r in q_entity_relatedness]
            relatedness_weights[q_entity] = val_weights

        return mean([1 - weighted_geom_mean(relatedness_weights[q_entity]) + alpha for q_entity in relatedness_weights])


    def find_expert(self, query, scoring):
        logging.debug(u"Processing query: {}".format(query))
        start_time = time.time()
        query_entities =  set(a.entity_title for a in entities(query))
        logging.debug(u"Found the following entities in the query: {}".format(u",".join(query_entities)))
        authors = self.citing_authors(query_entities) 
        logging.debug(u"Found %d authors that matched the query, computing score for each of them." % len(authors))
        results = []
        for author_id in authors:
            score = scoring(self, query_entities, author_id)
            name = self.name(author_id)
            results.append({"name":name, "author_id":author_id, "score":score})
            logging.debug(u"%s score=%.3f", name, score)
        runtime = time.time() - start_time
        logging.info("Query completed in %.3f sec" % (runtime,))
        return sorted(results, key=lambda t: t["score"], reverse=True), runtime, query_entities
