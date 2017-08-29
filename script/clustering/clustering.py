#!/usr/bin/python
import sys
import getopt
import pymongo
from argparse import ArgumentParser

import tagme

import numpy as np
import math
import requests

import networkx as nx
import traceback

from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn.datasets.samples_generator import make_blobs
from sklearn.preprocessing import StandardScaler
import hdbscan
import time
import matplotlib.pyplot as plt
import seaborn as sns

import multiprocessing
import logging
logging.basicConfig(level=logging.INFO)

import shelve

tagme.GCUBE_TOKEN = "e7441420-84be-4d0a-a72a-f54db53f522d-843339462"
"""
This script is used to check which of the authors cited in a QREL file are not indexed
"""
API_ENDPOINT = "http://localhost:7000"
DATABASE_NAME = "TU_15"
MIN_RHO = 0.20
# MIN number of entities to be considered for outlier elimination
MIN_ENTITIES_TRESHOLD = 40
MAX_CLUTERING_FACTOR = 0.7

_damping_factor = 0.15

_cache_file = "./clustering.shelf"
entities_rel_dict = shelve.open(_cache_file)
# _cache = pyfscache.FSCache(_cache_dir)


_db_connection_ = pymongo.MongoClient()
_db_ = _db_connection_[DATABASE_NAME]
_request_sleep_time=0.05

def _issue_request(path, params):
    response = None
    while response is None:
        try:
            return requests.post(path, params=params).json()
        except:
            time.sleep(_request_sleep_time)
            continue

def relatedness(srcWikiID, dstWikiID, method="jaccard"):
    key = '{}-{}-{}'.format(srcWikiID, dstWikiID, method)
    if not entities_rel_dict.has_key(key):
        response = _issue_request("{}/rel".format(API_ENDPOINT), params={"srcWikiID": srcWikiID, "dstWikiID": dstWikiID, "method": method})
        entities_rel_dict[key] = response["relatedness"]
    return entities_rel_dict[key]


def get_authors_list():
    res = _db_.authors.find({}, {"author_id": 1})
    return [author["author_id"] for author in res]


def get_author_entities(author_id):
    author = _db_.authors.find_one({"author_id": author_id})
    return [entity for entity in author["entities"] if entity["score"] >= MIN_RHO]


def set_author_entities(author_id, pr_entities_list):
    author = _db_.authors.find_one({"author_id": author_id})
    author["pr_entities"] = pr_entities_list

    _db_.authors.find_one_and_replace({
        "author_id": author['author_id']
    }, author)


def fill_matrix(entities_list):
    n_entities = len(entities_list)
    # rel_matrix = np.ones((n_entities, n_entities)) - np.identity(n_entities)
    rel_matrix = np.identity(n_entities)

    for i in range(n_entities):
        for j in range(i + 1, n_entities):
            e1, e2 = entities_list[i]["entity_id"], entities_list[j]["entity_id"]
            if e2 < e1:
                e1, e2 = e2, e1
            rel_matrix[i][j] = rel_matrix[j][i] = relatedness(
                e1, e2, "milnewitten")
    return rel_matrix


def cluster(entities_list):
    rel_matrix = 1. - fill_matrix(entities_list)
    # rel_matrix = StandardScaler().fit_transform(rel_matrix)
    # db = DBSCAN(eps=.5, min_samples=3, metric="precomputed").fit(rel_matrix, [e["score"] * math.log(1 + e["document_count"]) for e in entities_list])

    # db = DBSCAN(eps=.7, min_samples=3, metric="precomputed").fit(rel_matrix, [e["score"] * math.log(1 + e["document_count"]) for e in entities_list])

    # db = DBSCAN(eps=.4, min_samples=3, metric="precomputed").fit(rel_matrix, [e["document_count"] for e in entities_list])
    # db = DBSCAN(eps=.4, min_samples=3, metric="precomputed").fit(rel_matrix, [e["score"] for e in entities_list])
    # db = DBSCAN(eps=.4, min_samples=3, metric="precomputed").fit(rel_matrix)
    # core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
    # core_samples_mask[db.core_sample_indices_] = True
    # db = hdbscan.HDBSCAN(metric='precomputed', allow_single_cluster=True).fit(rel_matrix, [e["score"] * math.log(1 + e["document_count"]) for e in entities_list])
    estimated_cluster_size = int(math.log(float(len(entities_list))))
    clustered = hdbscan.HDBSCAN(metric='precomputed', min_cluster_size=estimated_cluster_size, min_samples=3, allow_single_cluster=True).fit(rel_matrix)
    # cluster.outlier_scores_
    try:
        sns.distplot(clustered.outlier_scores_[np.isfinite(clustered.outlier_scores_)], rug=True)
        sns.clustermap(rel_matrix)
    except:
        pass

    labels = clustered.labels_

    # Number of clusters in labels, ignoring noise if present.
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    print(rel_matrix)
    print(labels)
    print('Estimated number of clusters: %d' % n_clusters_)
    plt.show()
    # cluster_to_entities = dict()
    # for i in range(len(labels)):
    # 	e_list = cluster_to_entities.get(labels[i], [])
    # 	e_list.append(entities_list[i]["entity_name"])
    # 	cluster_to_entities[labels[i]] = e_list

    # for cluster_id, cluster_el in cluster_to_entities.iteritems():
    # 	print "\nCluster {}\n{}".format(cluster_id, cluster_el)

    return [entities_list[i] for i in range(len(entities_list)) if labels[i] >= 0]


def pagerank(entities_list):
    rel_matrix = fill_matrix(entities_list)
    rel_graph = nx.from_numpy_matrix(rel_matrix)

    personalization = dict([(i, entities_list[i]["score"] * math.log(
        1 + entities_list[i]["document_count"])) for i in range(len(entities_list))])
    pr = nx.pagerank(rel_graph, alpha=_damping_factor, max_iter=200, personalization=personalization)
    for i in range(len(entities_list)):
        entities_list[i]["pr_score"] = pr[i]
    entities_list = sorted(entities_list, key=lambda t: t["pr_score"], reverse=True)

    return entities_list


def process_author(author_id):
    clustered = 0
    entities_list = get_author_entities(author_id)
    if len(entities_list) >= MIN_ENTITIES_TRESHOLD:
        print "Author {}".format(author_id)
        clustered_entities = cluster(entities_list)
        if float(len(clustered_entities)) / len(entities_list) >= MAX_CLUTERING_FACTOR:
            # if we did not removed too many outliers, clustered entities are saved 
            entities_list = clustered_entities
            clustered = 1
    if entities_list:
        entities_list = pagerank(entities_list)
    set_author_entities(author_id, entities_list)
    return (author_id, clustered)

def main():
    # pool = multiprocessing.pool.ThreadPool()
    authors = get_authors_list()
    clustered_authors = 0
    print "Authors to be processed: {}".format(len(authors))
    for i, author_id in enumerate(authors):
        (author_id, clustered) = process_author(author_id)
        clustered_authors += clustered
        logging.info("\r==== Done: %s ==== PROGRESS: %d/%d ==== Clustered: %d/%d ====", author_id, i+1, len(authors), clustered_authors, len(authors))
    entities_rel_dict.close()

    # try:
    #     for i, (author_id, clustered) in enumerate(pool.imap_unordered(process_author, authors)):
    #         clustered_authors += clustered
    #         logging.info("\r==== Done: %s ==== PROGRESS: %d/%d ==== Clustered: %d/%d ====", author_id, i+1, len(authors), clustered_authors, len(authors))
    # except KeyboardInterrupt:
    #     pool.terminate()
    #     pool.join()
    # except Exception as e:
    #     logging.error('Uncaught exception in worker process:\n')
    #     traceback.print_exc()
    #     raise e
    # finally:
        # entities_rel_dict.close()
# def main():
# 	# parser = ArgumentParser()
# 	# parser.add_argument("-a", "--author_id", required=True, action="store", help="Author id to cluster")
# 	# args = parser.parse_args()


# 	for author_id in get_authors_list():
# 		print "Author {}".format(author_id)
# 		entities_list = get_author_entities(author_id)
# 		if len(entities_list) >= MIN_ENTITIES_TRESHOLD:
# 			entities = cluster(entities_list)
# 			entities = pagerank(entities)
# 			set_author_entities(author_id, entities)
if __name__ == "__main__":
    sys.exit(main())
