# encoding: utf-8

import os
import signal
import sys
import io
import traceback
import codecs
import logging
logging.basicConfig(level=logging.CRITICAL)
logger = logging.getLogger("EF_log")
logger.setLevel(logging.INFO)
import time
from argparse import ArgumentParser
from multiprocessing import Pool
from random import random
from subprocess import check_output
import tagme
import errno

from expertfinding.core import ExpertFinding, scoring


def random_score(*args):
    return random()

def initialize_ef_processor(database_name, lucene_dir, scoring_structure_, rel_dict_file, wiki_api_endpoint, cache_dir):
    global exf, scoring_structure
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    exf = ExpertFinding(lucene_dir=lucene_dir, database_name=database_name, relatedness_dict_file=rel_dict_file, wiki_api_endpoint=wiki_api_endpoint, cache_dir=cache_dir)
    scoring_structure = scoring_structure_


def ef_processor(data):
    global exf, scoring_structure
    query_id, query = data
    hits, runtime, query_entities = exf.find_expert(query, scoring_structure)
    return query_id, (hits, runtime, query_entities)


def topics_generator(filename):
    with codecs.open(filename, encoding="utf-8") as topics_f:
        for line in topics_f:
            topic_id, t_desc = line.strip().split("\t")
            yield u"topic-{}".format(topic_id), t_desc


def qrels_generator(filename):
    with codecs.open(filename, encoding="utf-8") as qrels_f:
        for line in qrels_f:
            topic_id, _, author_id, rank = line.strip().split("\t")
            yield topic_id, author_id, rank

def write_results(results, benchmark_name, dataset, qrels, scoring):
    results_filename_base = os.path.join("scores/{}_{}_{}/".format(benchmark_name, dataset[:3], time.strftime("%d_%m_%y")), "output")
    if not os.path.exists(os.path.dirname(results_filename_base)):
            try:
                os.makedirs(os.path.dirname(results_filename_base))
            except OSError as exc: # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

    results_filename = results_filename_base + ".results"
    runtime_filename = results_filename_base + ".runtime"
    query_entities_filename = results_filename_base + ".queryentities"
    scoring_definition_filename = results_filename_base + ".scoring"

    with open(scoring_definition_filename, "w") as scoring_f:
        scoring_f.write(scoring)

    with open(results_filename, "w") as results_f, open(runtime_filename, "w") as runtime_f, open(query_entities_filename, "w") as query_entities_f:
        for q_id in results:
            hits, runtime, query_entities = results[q_id]
            for hit in hits:
                results_f.write("{} 0 {} 0 {} {}\n".format(q_id, hit["author_id"], hit["score"], benchmark_name))
            runtime_f.write("{} {}\n".format(q_id, runtime))
            query_entities_f.write(u"{} {}\n".format(q_id, u"; ".join(query_entities)).encode("utf-8"))

    evaluation = check_output(["trec_eval", "-c", "-q", "-M", "1000", "-m", "all_trec", qrels, results_filename])
    with open(results_filename_base + ".eval", "w") as eval_f:
        eval_f.write(evaluation)

def main():
    parser = ArgumentParser()
    parser.add_argument("-l", "--lucene_dir", required=False, action="store", help="Lucene index root directory")
    parser.add_argument("-d", "--database_name", required=True, action="store", help="MongoDB database name")
    parser.add_argument("-c", "--cache_dir", required=True,action="store", help="Cache directory")
    parser.add_argument("-r", "--relatedness_dict", required=True, action="store", help="Relatedness persistent dictionary file")
    parser.add_argument("-g", "--gcube_token", required=True, action="store", help="Tagme authentication gcube token")
    parser.add_argument("-t", "--topics", required=True, action="store", help="Topic id-description mapping file")
    parser.add_argument("-q", "--qrels", required=True, action="store", help="Qrel file")
    parser.add_argument("-f", "--scoring", required=True, action="store", help="Name of scoring functions tu test")
    parser.add_argument("-w", "--wiki_api_endpoint", required=True, action="store", help="Wikipedia API endpoint")
    parser.add_argument("-n", "--benchmark_name", required=True, action="store", help="Benchmark name")
    args = parser.parse_args()

    tagme.GCUBE_TOKEN = args.gcube_token

    topics = dict((topic_id, t_desc) for topic_id, t_desc in topics_generator(args.topics))

    queries = list(set(sorted((topic_id, topics[topic_id]) for topic_id, _, _ in qrels_generator(args.qrels))))
    scoring_structure_ = scoring.ScoringStructure(args.scoring)

    
    pool = Pool(initializer=initialize_ef_processor, initargs=(args.database_name, args.lucene_dir, scoring_structure_, args.relatedness_dict, args.wiki_api_endpoint, args.cache_dir))
    try:
        results = []
        for i, query_result in enumerate(pool.imap_unordered(ef_processor, queries)):
            results.append(query_result)
            logger.info("\r=====PROGRESS=====: %d/%d", i, len(queries))

        # results = dict(pool.map(ef_processor, queries))
        results = dict(results)
        dataset = os.path.split(args.qrels)[-1].replace(".qrel", "")
        write_results(results, args.benchmark_name, dataset, args.qrels, args.scoring)
    except KeyboardInterrupt:
        pool.terminate()
        pool.join()
    except Exception as e:
        logger.error('Uncaught exception in worker process:\n')
        traceback.print_exc()
        raise e



    # initialize_ef_processor(args.database_name, args.lucene_dir, scoring_structure_, args.relatedness_dict, args.wiki_api_endpoint, args.cache_dir)
    # results = []
    # for i, query in enumerate(queries):
    #     results.append(ef_processor(query))
    #     logger.info("\r=====PROGRESS=====: %d/%d", i+1, len(queries))
    # results = dict(results)
    # dataset = os.path.split(args.qrels)[-1].replace(".qrel", "")
    write_results(results, args.benchmark_name, dataset, args.qrels, args.scoring)
    
    global exf
    exf.close()     
if __name__ == "__main__":
    sys.exit(main())
