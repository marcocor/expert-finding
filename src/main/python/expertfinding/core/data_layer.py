import os
import sqlite3
import cgi
import logging
import pymongo
from bson.objectid import ObjectId

logger = logging.getLogger("EF_log")

try:
    import lucene
except ImportError:
    logger.error("Cannot import Lucene")


DEFAULT_MIN_SCORE = 0.20


def beautify_str(s):
    return s.replace("\n", " ").replace("\r", " ").encode('ascii', 'ignore')


def legit_document(doc_body):
    return doc_body is not None and len(doc_body) > 10


def _annotated_text_generator(text, annotations):
    prev = 0
    for a in sorted(annotations, key=lambda a: a.begin):
        yield cgi.escape(text[prev:a.begin])
        yield u"<span class='annotation' entity='{}' score='{}'>{}</span>".format(cgi.escape(a.entity_title or ""), a.score, cgi.escape(text[a.begin: a.end]))
        prev = a.end
    yield text[prev:]


def annotated_text(text, annotations):
    return "".join(_annotated_text_generator(text, annotations))


class DataLayer():
    """
    Manages all interactions with DB and data
    """

    def __init__(self, exf, entities_fun, database_name, erase):
        self.exf = exf
        self.entities = entities_fun
        self.db_connection_ = pymongo.MongoClient()
        self.db_ = self.db_connection_[database_name]

        if erase:
            self.db_connection_.drop_database(database_name)

    def initialize_db(self):
        """
        Initialize ExpertFinding DB, creates indexes etc..
        """
        self.db_.authors.create_index(
            [('author_id', pymongo.ASCENDING)], unique=True)
        self.db_.entities.create_index(
            [('entity_name', pymongo.ASCENDING)], unique=True)

    def add_papers_from_author(self, author_info, papers):
        """
        Indexes a list of documents/papers (papers) from the same author (described in author_info)
        """

        logger.info(
            "Indexing documents from author [%s] (%s)", author_info["name"], author_info["author_id"])

        author = self._get_author(author_info)
        for paper in papers:
            if legit_document(paper.abstract):
                self._add_document(author, paper)

        self.db_.authors.find_one_and_replace({
            "author_id": author['author_id']
        }, author, upsert=True)

    def _add_document(self, author, document):
        ent = self.entities(document.abstract)
        # Mongo document ID
        doc_id = self._add_document_body(document, ent)

        author['documents'].append(doc_id)

        self._add_entities(author, doc_id, document, ent)
        self._add_lucene_document(doc_id, document)

    def _add_document_body(self, document, annotations):
        document_entities = self._get_document_entities(annotations)
        annotated_t = annotated_text(document.abstract, annotations)
        result = self.db_.documents.insert_one({
            "author_id": document.author_id,
            "entities": document_entities.values(),
            "year": document.year,
            "text": annotated_t
        })

        return result.inserted_id

    def _add_entities(self, author, document_id, document, annotations):
        document_entities = self._get_document_entities(annotations)

        self._add_entities_to_author(author, document, document_entities)
        self._add_entities_to_collection(
            author, document_id, document, document_entities)

    def _add_entities_to_author(self, author, document, document_entities):
        author_entities = author['entities']

        found = False

        for entity_id in document_entities:
            for entity in author_entities:
                if entity['entity_id'] == entity_id:
                    entity['score'] = max(
                        document_entities[entity_id]['score'], entity['score'])
                    entity['document_count'] += 1
                    entity['years'].append(document.year)
                    found = True

            if not found:
                author_entities.append({
                    'entity_id': entity_id,
                    'entity_name': document_entities[entity_id]["entity_name"],
                    'score': document_entities[entity_id]['score'],
                    'document_count': 1,
                    'years': [document.year]
                })

            found = False
            author['entities'] = author_entities

    def _add_entities_to_collection(self, author, document_id, document, document_entities):
        for entity_id in document_entities:
            entity = self.db_.entities.find_one({'entity_id': entity_id})

            if entity is None:
                # if the entity is not yet present in the collection,
                # it is added (including information on the usage by author)
                entity = {
                    'entity_id': entity_id,
                    'entity_name': document_entities[entity_id]['entity_name'],
                    'institutions': [document.institution],
                    'occurrences': [{
                        'author_id': author['author_id'],
                        'count': document_entities[entity_id]['count'],
                        'score': document_entities[entity_id]['score'],
                        'years': [document.year]
                    }],
                    'documents': [document_id]
                }
            else:
                # otherwise need to add the institution of current author, the considered document
                # and check if the author has already cited this entity before
                entity['institutions'].append(document.institution)
                entity['institutions'] = list(set(entity['institutions']))
                entity['documents'].append(document_id)
                found = False
                for author_occurrence in entity['occurrences']:
                    if author_occurrence['author_id'] == author['author_id']:
                        author_occurrence['count'] += document_entities[entity_id]['count']
                        author_occurrence['score'] = max(
                            document_entities[entity_id]['score'],
                            author_occurrence['score']
                        )
                        author_occurrence['years'].append(document.year)
                        author_occurrence['years'] = list(
                            set(author_occurrence['years']))
                        found = True
                        break
                if not found:
                    entity['occurrences'].append({
                        'author_id': author['author_id'],
                        'count': 1,
                        'score': document_entities[entity_id]['score'],
                        'years': [document.year]
                    })

            self.db_.entities.find_one_and_replace(
                {'entity_id': entity_id}, entity, upsert=True)

    def _get_author(self, author_info):
        author = self.db_.authors.find_one(
            {"author_id": author_info["author_id"]})
        if author is None:
            author = {
                "author_id": author_info["author_id"],
                "institution": author_info["institution"],
                "name": author_info["name"],
                "entities": [],
                "documents": []
            }
        return author

    def _get_document_entities(self, annotations):
        doc_entities = dict()
        # dictionary entity_title -> "count": occurrences, "entity":
        # entity_title, "score": rho from tagme
        for e in annotations:
            prev = doc_entities.get(e.entity_id, {
                                    'entity_name': e.entity_title,
                                    'entity_id': e.entity_id,
                                    'count': 0,
                                    'score': e.score
                                    })
            prev['count'] += 1
            prev['score'] = max(prev['score'], e.score)
            doc_entities[e.entity_id] = prev

        return doc_entities

    def get_document_containing_entities(self, author_id, entities):
        """
        Return the list of documents containing one or more of entities (specified by wiki title)
        """
        res = self.db_.documents.find({
            "author_id": author_id,
            "entities": {
                "$elemMatch": {
                    "entity_name": {
                        "$in": entities
                    }
                }
            }
        }, {
            "year": 1,
            "entities": {
                "$elemMatch": {
                    "entity_name": {
                        "$in": entities
                    }
                }
            }
        })

        return res

    def get_document(self, document_id):
        """
        Returns the document associated with document_id
        """
        res = self.db_.documents.find_one({
            "_id": ObjectId(document_id)
        })

        return res

    def get_author_name(self, author_id):
        """
        Returns name of an author (author_id)
        """
        author = self.db_.authors.find_one(
            {"author_id": author_id}, {'name': True})
        return author['name']

    def complete_author_name(self, author_name):
        res = self.db_.authors.find(
            {"name": {"$regex": ".*{}.*".format(author_name), "$options": "i"}})
        return res

    def get_author_papers_count(self, author_id):
        """
        Returns #documents written by an author (author_id)
        """
        res = self.db_.authors.aggregate([{
            '$match': {
                'author_id': author_id
            }
        }, {
            '$project': {
                'author_id': 1,
                'document_count': {
                    '$size': '$documents'
                }
            }
        }])

        return res.next()['document_count']

    def author_entity_frequency(self, author_id):
        """
        Returns list of entities cited by a specific author (author_id)
        together with their frequency (#documents in which are contained and max rho)
        """
        res = self.db_.authors.aggregate([{
            '$match': {
                'author_id': author_id
            }
        }, {
            '$project': {
                'entities': {
                    '$filter': {
                        'input': '$entities',
                        'as': 'entity',
                        'cond': {
                            '$gte': ['$$entity.score', DEFAULT_MIN_SCORE]
                        }
                    }
                }
            }
        }, {
            '$unwind': '$entities'
        }, {
            '$project': {
                '_id': None,
                'entity_name': '$entities.entity_name',
                'entity_id': '$entities.entity_id',
                'document_count': '$entities.document_count',
                'years': '$entities.years',
                'max_rho': '$entities.score'
            }
        }])

        return res

    def authors_entity_frequency(self, authors):
        """
        Returns list of entities cited by a list of authors (author_id)
        together with their frequency (#documents in which are contained and max rho)
        """
        res = self.db_.authors.aggregate([{
            '$match': {
                'author_id': {
                    '$in': authors
                }
            }
        }, {
            '$project': {
                'author_id': 1,
                'pr_entities': 1
            }
        }, {
            '$unwind': '$pr_entities'
        }, {
            '$group': {
                '_id': '$author_id',
                'document_count': {
                    '$sum': '$entities.document_count'
                },
                'max_rho': {
                    '$max': '$entities.score'
                },
                'entities': {
                    '$push': '$pr_entities'
                }
            }
        }])

        return res

    def citing_authors(self, entities):
        """
        Returns the list of author (author_id) that cited
        at least one of the entities considered (with rho >= some threshold)
        """
        res = self.db_.authors.find({
            "pr_entities": {
                "$elemMatch": {
                    "entity_id": {
                        "$in": entities
                    }
                }
            }
        })

        # res = self.db_.authors.find({
        #     "entities": {
        #         "$elemMatch": {
        #             "entity_id": {
        #                 "$in": entities
        #             },
        #             "score": {
        #                 "$gte": DEFAULT_MIN_SCORE
        #             }
        #         }
        #     }
        # })
        return list([citing_author['author_id'] for citing_author in res])


    def all_authors(self):
        """
        Returns the entire list of authors
        """

        res = self.db_.authors.find({}, {
            "author_id": True
        })
        return [author["author_id"] for author in list(res)]

    def entity_popularity(self, entities):
        """
        Returns for each entity #documents which contain it
        """
        res = self.db_.entities.aggregate([{
            '$match': {
                'entity_id': {
                    '$in': entities
                }
            }
        }, {
            '$project': {
                'entity_name': 1,
                'entity_id': 1,
                'entity_popularity': {
                    '$size': '$occurrences'
                }
            }
        }])

        return res

    def get_author_max_rho(self, author_id, entities):
        """
        Retrieves max rho associated to each entity for the given author,
        entities are specified by wiki title
        """
        entity_to_max_rho = {}
        res = self.db_.authors.find_one({
            "author_id": author_id
        })

        if not res:
            logger.error("Cannot find author %s", author_id)
            return entity_to_max_rho

        for entity in res["entities"]:
            if entity["entity_name"] in entities:
                entity_to_max_rho[entity["entity_id"]] = entity["score"]

        return entity_to_max_rho

    def total_authors(self):
        """
        Returns #documents indexed
        """
        res = self.db_.authors.count()
        return res

    def total_papers(self):
        """
        Returns #documents indexed
        """
        res = self.db_.documents.count()
        return res

    def author_entities(self, author_id, min_rho):
        """
        Retrieves entities associated with author_id with rho ge than min_rho
        """
        author = self._get_author({"author_id": author_id})
        return dict((
            entity["entity_id"], {
                "entity_id": entity["entity_id"],
                "entity_name": entity["entity_name"],
                "document_count": entity["document_count"],
                "score": entity["score"]
            }) for entity in author["entities"] if entity["score"] >= min_rho)

    def _add_lucene_document(self, doc_id, document):
        try:
            doc = lucene.Document()
            doc.add(lucene.Field("document_id", str(doc_id),
                                 lucene.Field.Store.YES, lucene.Field.Index.ANALYZED))
            doc.add(lucene.Field("author_id", beautify_str(
                document.author_id), lucene.Field.Store.YES, lucene.Field.Index.ANALYZED))
            doc.add(lucene.Field("author_name", beautify_str(document.name),
                                 lucene.Field.Store.YES, lucene.Field.Index.ANALYZED))
            doc.add(lucene.Field("year", str(document.year),
                                 lucene.Field.Store.YES, lucene.Field.Index.ANALYZED))
            doc.add(lucene.Field("institution", beautify_str(
                document.institution), lucene.Field.Store.YES, lucene.Field.Index.ANALYZED))
            doc.add(lucene.Field("text", beautify_str(document.abstract),
                                 lucene.Field.Store.YES, lucene.Field.Index.ANALYZED))
            self.exf.index_writer.addDocument(doc)
        except NameError:
            pass
