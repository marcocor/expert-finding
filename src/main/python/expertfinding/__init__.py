
from collections import namedtuple, Counter
from scipy import stats
import expertfinding
import logging
import os
import pickledb
import re
import string
import tagme
import unicodecsv as csv
import sqlite3
import pyfscache

Paper = namedtuple('Paper', ['author_id', 'name', 'institution', 'year', 'abstract', 'doi'])

__all__ = []

INPUT_ENCODING = "windows-1252"
DEFAULT_MIN_SCORE = 0.15
FLUSH_EVERY = 100


def normalize_author(name_field, lastname_field):
    return re.sub(r"\W+", " ", string.capwords("{} {}".format(name_field, lastname_field)).strip())


def legit_abstract(abstract):
    return abstract is not None and len(abstract) > 10


def entities(text):
    return  [(a.entity_title, a.score) for a in tagme.annotate(text).annotations]

def set_cache(cache_dir):
    cache = pyfscache.FSCache(cache_dir)
    expertfinding.entities = cache(expertfinding.entities)


class ExpertFinding(object):
    
    def __init__(self, storage_db, erase=True):
        self.papers_count = Counter()
        self.abstract_count = Counter()
        self.id_to_entities = dict()
        if erase and os.path.isfile(storage_db):
            os.remove(storage_db)
        self.db_connection = sqlite3.connect(storage_db)
        self.db = self.db_connection.cursor()
        self.db.execute('''CREATE TABLE IF NOT EXISTS authors
             (author_id PRIMARY KEY, name, institution)
             ''')
        self.db.execute('''CREATE TABLE IF NOT EXISTS entities
             (entity, author_id, year, rho,
             FOREIGN KEY(author_id) REFERENCES authors(author_id))''')

    
    def _papers_generator(self, filename, encoding):
        with open(filename, 'rb') as input_f:
            for i, l in enumerate(csv.reader(input_f, delimiter=';', encoding=encoding)):
                if len(l) != 20:
                    logging.debug("Discarding line %d %d %s" % (i, len(l), l))
                    continue
                yield Paper(l[0], normalize_author(l[2], l[1]), l[4], int(l[6]), l[13], l[11])
                
    def read_papers(self, input_f, min_year=None, max_year=None, encoding=INPUT_ENCODING):
        papers = list(self._papers_generator(input_f, encoding))
        logging.info("%s: Number of papers (total): %d" % (os.path.basename(input_f), len(papers)))
        
        papers = [p for p in papers if
                  (min_year is None or p.year >= min_year) and (max_year is None or p.year <= max_year)]

        logging.info("%s: Number of papers (filtered) %d" % (os.path.basename(input_f), len(papers)))
        if papers:
            logging.info("%s: Papers with abstract %.1f%%" % (os.path.basename(input_f), sum(1 for p in papers if legit_abstract(p.abstract)) * 100 / len(papers)))
            logging.info("%s: Papers with DOI but no abstract %.1f%%" % (os.path.basename(input_f), sum(1 for p in papers if not legit_abstract(p.abstract) and p.doi) * 100 / len(papers)))

        for p in papers:
            self._add_author(p.author_id, p.name, p.institution)
            self.papers_count[p.author_id] += 1
            if (legit_abstract(p.abstract)):
                ent = entities(p.abstract)
                self._add_entities(p.author_id, p.year, ent)
                self.abstract_count[p.author_id] += 1
        self.db_connection.commit()

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
           ORDER BY year, COUNT(*) DESC
           {}'''.format(having), author_id).fetchall()


    def entities(self, author_id):
        return self.db.execute('''SELECT year, entity, rho FROM entities WHERE author_id=?''', (author_id,)).fetchall()


    def _add_entities(self, author_id, year, entities):
        self.db.executemany('INSERT INTO entities VALUES (?,?,?,?)', ((entity, author_id, year, rho) for entity, rho in entities))


    def _add_author(self, author_id, name, institution):
        self.db.execute('INSERT OR IGNORE INTO authors VALUES (?,?,?)', (author_id, name, institution))


    def print_abstract_quantiles(self):
        print "number of abstracts: {}".format(sum(self.abstract_count.values()))
        quantiles = stats.mstats.mquantiles([self.abstract_count[a_id] for a_id in self.abstract_count], prob=[n / 10 for n in range(10)])
        print quantiles
        for i in range(len(quantiles)):
            begin = int(quantiles[i])
            end = int(quantiles[i + 1]) - 1 if i < len(quantiles) - 1 else max(self.abstract_count.values())
            print "{} authors have {}-{} papers with abstract".format(sum(1 for a_id in self.abstract_count if begin <= self.abstract_count[a_id] <= end), begin, end)


    def print_entity_stats(self):
        all_entities = Counter(e for entities in self.id_to_entities.values() for e in entities)
        print "most common entities"
        for title, freq in all_entities.most_common(100):
            print title, freq
        print "number of entities: {}".format(sum(all_entities.values()))
        print "number of distinct entities: {}".format(len(all_entities))


    def print_authors_stats(self):
        for name, a_id in self.name_to_id.iteritems():
            print "{} (ID {}) papers: {} abstracts:{}".format(name, a_id, self.papers_count[a_id], self.abstract_count[a_id]) 


    def print_author_stats(self, author_name):
        for title, freq in Counter(self.id_to_entities[self.name_to_id[author_name]]).most_common():
            print title, freq
    
    
