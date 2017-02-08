
from math import log
import expertfinding
import logging
import os
import re
import string
import tagme
import sqlite3
import pyfscache

__all__ = []

DEFAULT_MIN_SCORE = 0.15
FLUSH_EVERY = 100


def legit_abstract(abstract):
    return abstract is not None and len(abstract) > 10


def entities(text):
    return [(a.entity_title, a.score) for a in tagme.annotate(text).annotations]


def set_cache(cache_dir):
    cache = pyfscache.FSCache(cache_dir)
    expertfinding.entities = cache(expertfinding.entities)


class ExpertFinding(object):

    def __init__(self, storage_db, erase=True):
        if erase and os.path.isfile(storage_db):
            os.remove(storage_db)
        self.db_connection = sqlite3.connect(storage_db)
        self.db = self.db_connection.cursor()
        self.db.execute('''CREATE TABLE IF NOT EXISTS authors
             (author_id PRIMARY KEY, name, institution)
             ''')
        self.db.execute('''CREATE TABLE IF NOT EXISTS entities
             (entity, author_id, paper_id, year, rho,
             FOREIGN KEY(author_id) REFERENCES authors(author_id))''')

    def read_papers(self, input_f, papers_generator, min_year=None, max_year=None):
        papers = list(papers_generator)
        logging.info("%s: Number of papers (total): %d" % (os.path.basename(input_f), len(papers)))

        papers = [p for p in papers if
                  (min_year is None or p.year >= min_year) and (max_year is None or p.year <= max_year)]

        logging.info("%s: Number of papers (filtered) %d" % (os.path.basename(input_f), len(papers)))
        if papers:
            logging.info("%s: Number of papers (filtered) with abstract: %d" % (os.path.basename(input_f), sum(1 for p in papers if legit_abstract(p.abstract))))
            logging.info("%s: Number of papers (filtered) with DOI but no abstract %d" % (os.path.basename(input_f), sum(1 for p in papers if not legit_abstract(p.abstract) and p.doi)))

        paper_id = self._next_paper_id()
        for p in papers:
            self._add_author(p.author_id, p.name, p.institution)
            if (legit_abstract(p.abstract)):
                ent = entities(p.abstract)
                self._add_entities(p.author_id, paper_id, p.year, ent)
                paper_id += 1
        self.db_connection.commit()

    def author_entity_frequency(self, author_id, popularity_by_institution=None):
        """
        Returns how many authors's papers have cited the entities cited by a specific author.
        """
        return self.db.execute('''
            SELECT entity, author_id, entity_popularity, COUNT(*) as entity_author_frequency, MAX(max_rho), GROUP_CONCAT(year) as years
            FROM (
                SELECT i.entity, e.author_id, entity_popularity, MAX(rho) AS max_rho, e.year
                FROM (
                    SELECT entity, COUNT(*) as entity_popularity
                    FROM (
                        SELECT entity, institution
                        FROM entities as e, authors as a
                        WHERE e.author_id == a.author_id AND a.institution==?
                        GROUP BY paper_id, entity
                    )
                    GROUP BY entity
                ) AS i, entities AS e
                WHERE i.entity == e.entity AND e.author_id == ?
                GROUP BY e.entity, e.paper_id
                ORDER BY e.year
            )
            GROUP BY entity
            ORDER BY entity_author_frequency DESC''', (popularity_by_institution, author_id,)).fetchall()

    def get_authors_count(self, institution):
        """
        Returns how many authors are part of an institution.
        """
        return self.db.execute('''SELECT COUNT(*) FROM authors WHERE institution==?''', (institution,)).fetchall()[0][0]

    def ef_iaf(self, author_id):
        institution = self.institution(author_id)
        institution_papers = self.institution_papers_count(institution)
        author_entity_frequency = self.author_entity_frequency(author_id, institution)
        author_papers = self.author_papers_count(author_id)
        return sorted(((
                 entity,
                 entity_author_freq / float(author_papers),
                 log(institution_papers/float(entity_popularity)),
                 entity_author_freq / float(author_papers) * log(institution_papers/float(entity_popularity)),
                 max_rho,
                 [int(y) for y in years.split(",")],
                ) for entity, author_id, entity_popularity, entity_author_freq, max_rho, years in author_entity_frequency), key=lambda t: t[3], reverse=True)

    def author_papers_count(self, author_id):
        return self.db.execute('''SELECT COUNT(DISTINCT(paper_id)) FROM entities WHERE author_id=?''', (author_id,)).fetchall()[0][0]

    def institution_papers_count(self, institution):
        return self.db.execute('''
            SELECT COUNT(DISTINCT(e.paper_id))
            FROM "entities" as e, authors as a
            WHERE e.author_id == a.author_id AND a.institution=?''', (institution,)).fetchall()[0][0]

    def author_id(self, author_name):
        return [r[0] for r in self.db.execute('''SELECT author_id FROM authors WHERE name=?''', (author_name,)).fetchall()]

    def institution(self, author_id):
        return self.db.execute('''SELECT institution FROM authors WHERE author_id=?''', (author_id,)).fetchall()[0][0]

    def name(self, author_id):
        return self.db.execute('''SELECT name FROM authors WHERE author_id=?''', (author_id,)).fetchall()[0][0]

    def grouped_entities(self, author_id, year=None, min_freq=None):
        contraints = []
        if year is not None:
            contraints.append('year=%d' % year)
        if min_freq is not None:
            contraints.append('COUNT(*)>=%d' % min_freq)
        having = "HAVING {}".format(" AND ".join(contraints)) if contraints else ""
        return self.db.execute('''SELECT entity, year, AVG(rho), MIN(rho), MAX(rho), GROUP_CONCAT(rho), COUNT(*)
           FROM entities
           WHERE author_id=?
           GROUP BY entity, year
           {}
           ORDER BY year, COUNT(*) DESC'''.format(having), author_id).fetchall()

    def entities(self, author_id):
        return self.db.execute('''SELECT year, entity, rho FROM entities WHERE author_id=?''', (author_id,)).fetchall()

    def _add_entities(self, author_id, paper_id, year, entities):
        self.db.executemany('INSERT INTO entities VALUES (?,?,?,?,?)', ((entity, author_id, paper_id, year, rho) for entity, rho in entities))

    def _next_paper_id(self):
        return self.db.execute('SELECT IFNULL(MAX(paper_id), -1) FROM entities').fetchall()[0][0] + 1

    def _add_author(self, author_id, name, institution):
        self.db.execute('INSERT OR IGNORE INTO authors VALUES (?,?,?)', (author_id, name, institution))

    def papers_count(self):
        return self.db.execute('''
            SELECT author_id, COUNT(DISTINCT(paper_id))
            FROM "entities"
            GROUP BY author_id''').fetchall()

    def print_abstract_quantiles(self):
        papers_count = zip(*self.papers_count())[1]
        print "number of papers: {}".format(sum(papers_count))
        print "number of authors: {}".format(len(papers_count))
        quantiles = stats.mstats.mquantiles(papers_count, prob=[n / 10.0 for n in range(10)])
        print "quantiles:", quantiles
        for i in range(len(quantiles)):
            begin = int(quantiles[i])
            end = int(quantiles[i + 1]) - 1 if i < len(quantiles) - 1 else max(papers_count)
            print "{} authors have {}-{} papers with abstract".format(sum(1 for c in papers_count if begin <= c <= end), begin, end)
