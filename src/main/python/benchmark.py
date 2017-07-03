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
logger.setLevel(logging.DEBUG)
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


SCORING_FUNCTIONS = {foo.__name__: foo
                     for foo in [
                         #scoring.cossim_efiaf_score,
                         scoring.efiaf_score,
                         scoring.eciaf_score,
                         scoring.log_ec_ef_iaf_score,
                         #scoring.relatedness_geom,
                         scoring.lucene_max_score,
                         scoring.lucene_mean_score,
                         scoring.lucene_max_eciaf_score,
                         scoring.lucene_max_eciaf_norm_score,
                         scoring.lucene_max_eciaf_norm_rel_score,
                         random_score,
                         ]
                    }

def initialize_ef_processor(storage_db, lucene_dir, database_name, scoring_f, rel_dict_file):
    global exf, scoring_foo
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    exf = ExpertFinding(storage_db=storage_db, lucene_dir=lucene_dir, database_name=database_name, relatedness_dict_file=rel_dict_file)
    scoring_foo = scoring_f


def ef_processor(data):
    global exf, scoring_foo
    scoring_name = scoring_foo.__name__.replace("_score", "")
    query_id, query = data
    res = exf.find_expert(query, [scoring_foo])
    hits, runtime = res[scoring_name], res["time_{}".format(scoring_name)]
    # Using Lucene as scoring function, query_entities is not defined
    query_entities = res.get("query_entities", [])
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

def write_results(results, scoring_foo, dataset, qrels):
    results_filename_base = os.path.join("scores/{}_{}_{}/".format(scoring_foo.func_name, dataset[:3], time.strftime("%d_%m_%y")), "output")
    if not os.path.exists(os.path.dirname(results_filename_base)):
            try:
                os.makedirs(os.path.dirname(results_filename_base))
            except OSError as exc: # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

    results_filename = results_filename_base + ".results"
    runtime_filename = results_filename_base + ".runtime"
    query_entities_filename = results_filename_base + ".queryentities"

    with open(results_filename, "w") as results_f, open(runtime_filename, "w") as runtime_f, open(query_entities_filename, "w") as query_entities_f:
        for q_id in results:
            hits, runtime, query_entities = results[q_id]
            for hit in hits:
                results_f.write("{} 0 {} 0 {} {}\n".format(q_id, hit["author_id"], hit["score"], scoring_foo.func_name))
            runtime_f.write("{} {}\n".format(q_id, runtime))
            query_entities_f.write(u"{} {}\n".format(q_id, u"; ".join(query_entities)).encode("utf-8"))

    evaluation = check_output(["trec_eval", "-c", "-q", "-M", "1000", "-m", "all_trec", qrels, results_filename])
    with open(results_filename_base + ".eval", "w") as eval_f:
        eval_f.write(evaluation)

def main():
    parser = ArgumentParser()
    parser.add_argument("-s", "--storage_db", required=True, action="store", help="Storage DB file")
    parser.add_argument("-l", "--lucene_dir", required=False, action="store", help="Lucene index root directory")
    parser.add_argument("-d", "--database_name", required=True, action="store", help="MongoDB database name")
    parser.add_argument("-r", "--relatedness_dict", required=True, action="store", help="Relatedness persistent dictionary file")
    parser.add_argument("-g", "--gcube_token", required=True, action="store", help="Tagme authentication gcube token")
    parser.add_argument("-t", "--topics", required=True, action="store", help="Topic id-description mapping file")
    parser.add_argument("-q", "--qrels", required=True, action="store", help="Qrel file")
    parser.add_argument("-f", "--scoring", required=True, action="store", nargs="+", help="Name of scoring functions tu test", choices=SCORING_FUNCTIONS.keys())
    args = parser.parse_args()

    tagme.GCUBE_TOKEN = args.gcube_token

    topics = dict((topic_id, t_desc) for topic_id, t_desc in topics_generator(args.topics))

    queries = list(set(sorted((topic_id, topics[topic_id]) for topic_id, _, _ in qrels_generator(args.qrels))))

    for scoring_foo in [SCORING_FUNCTIONS[scoring_f_name] for scoring_f_name in args.scoring]:
        pool = Pool(initializer=initialize_ef_processor, initargs=(args.storage_db, args.lucene_dir, args.database_name, scoring_foo, args.relatedness_dict))
        # initialize_ef_processor(args.storage_db, args.lucene_dir, args.database_name, scoring_foo, args.relatedness_dict)
        # results = dict(ef_processor(query) for query in queries)

        try:
            results = dict(pool.map(ef_processor, queries))
            dataset = os.path.split(args.qrels)[-1].replace(".qrel", "")
            write_results(results, scoring_foo, dataset, args.qrels)
        except KeyboardInterrupt:
            pool.terminate()
            pool.join()
        except Exception as e:
            logger.error('Uncaught exception in worker process:\n')
            traceback.print_exc()
            raise e


if __name__ == "__main__":
    sys.exit(main())
