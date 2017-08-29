import sys
import codecs
import signal
import tempfile
import traceback
import csv
import logging
import os
import time
import errno
logging.basicConfig(level=logging.CRITICAL)
logger = logging.getLogger("EF_log")
logger.setLevel(logging.INFO)
from argparse import ArgumentParser
from expertfinding.core import ExpertFinding, scoring
from subprocess import check_output
from StringIO import StringIO
from multiprocessing import Pool

_PARAMETER_ESTIMATION_PRECISION = 50

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


def generate_possible_values(num_parameters, delta=1, sum_up_to=10):
    if num_parameters == 1:
        return [[sum_up_to]]

    vals = [x for x in xrange(0, sum_up_to, delta)] + [sum_up_to]
    combinations = []

    for value in vals:
        diff = sum_up_to - value
        rest_combinations = generate_possible_values(num_parameters - 1, delta, diff)
        for comb in rest_combinations:
            combinations.append([value] + comb) 

    return combinations

def initialize_ef_processor(database_name, lucene_dir, rel_dict_file, wiki_api_endpoint, cache_dir):
    global exf, scoring_structure
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    exf = ExpertFinding(lucene_dir=lucene_dir, database_name=database_name, relatedness_dict_file=rel_dict_file, wiki_api_endpoint=wiki_api_endpoint, cache_dir=cache_dir)


def ef_processor(data):
    global exf, scoring_structure
    query_id, query = data
    hits = exf.find_expert_(query)
    return query_id, hits

def apply_combination(combination, initial_results, num_parameters):
    final_results = []
    for query_id, hits in initial_results:
        weighted_results = [(hits[i], combination[i]) for i in range(0, num_parameters)]
        merged_results = scoring.normalize_merge(weighted_results)
        final_results.append((query_id, merged_results))
    return final_results

def gridsearch_generator(cached_results, num_parameters):
    combinations = generate_possible_values(num_parameters=num_parameters, delta=1, sum_up_to=_PARAMETER_ESTIMATION_PRECISION)
    # results = []
    logger.info("Computing results for each combination")
    logger.info("======================================")
    for i, combination in enumerate(combinations):
        combination_results = apply_combination(combination, cached_results, num_parameters)
        logger.info("\r=====PROGRESS=====: %d/%d", i+1, len(combinations))
        yield (combination, combination_results)

def evaluate(qrels, results_filename):
    evaluation = check_output([
        "trec_eval", "-c", "-M", "1000",
        "-m", "runid",
        "-m", "num_q",
        "-m", "num_ret",
        "-m", "num_rel",
        "-m", "num_rel_ret",
        "-m", "map",
        "-m", "recip_rank",
        "-m", "P",
        "-m", "recall",
        "-m", "ndcg_cut",
        qrels, results_filename])
    return evaluation

def results_to_evaluation((combination, combination_results)):
    with tempfile.NamedTemporaryFile("w") as results_f:
        for q_id, hits in combination_results:
            for hit in hits:
                results_f.write("{} 0 {} 0 {} {}\n".format(q_id, hit["author_id"], hit["score"], "gridsearch"))
        results_f.flush()
        evaluation = evaluate(qrels, results_f.name)
        results_f.close()

        reader = dict((row[0].strip(), row[2]) for row in csv.reader(StringIO(evaluation), delimiter='\t'))
        return "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                ";".join(str(val) for val in combination),
                reader["map"],
                reader["recip_rank"],
                reader["P_5"],
                reader["P_10"],
                reader["P_20"],
                reader["recall_5"], 
                reader["recall_10"],
                reader["recall_20"],
                reader["recall_30"],
                reader["recall_100"],
                reader["recall_500"],
                reader["ndcg_cut_100"])

def write_results(qrels_, all_results, benchmark_name):
    global qrels
    qrels = qrels_
    results_filename = os.path.join("scores/gridsearch_{}_{}/".format(benchmark_name, time.strftime("%d_%m_%y")), "estimation.csv")
    if not os.path.exists(os.path.dirname(results_filename)):
        try:
            os.makedirs(os.path.dirname(results_filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    pool = Pool()
    try:
        all_evaluations = []
        # all_evaluations = pool.map(results_to_evaluation, all_results)
        for i, evaluation in enumerate(pool.imap_unordered(results_to_evaluation, all_results)):
            all_evaluations.append(evaluation)
            # logger.info("\r=====PROGRESS=====: %d/%d", i, 176000)

    except KeyboardInterrupt:
        pool.terminate()
        pool.join()
    except Exception as e:
        logger.error('Uncaught exception in worker process:\n')
        traceback.print_exc()
        raise e


    with open(results_filename, "w") as evaluation_f:
        evaluation_f.write("\n".join(all_evaluations))
        evaluation_f.close()

def main():
    num_parameters = 4
    parser = ArgumentParser()
    parser.add_argument("-l", "--lucene_dir", required=False, action="store", help="Lucene index root directory")
    parser.add_argument("-d", "--database_name", required=True, action="store", help="MongoDB database name")
    parser.add_argument("-c", "--cache_dir", required=True,action="store", help="Cache directory")
    parser.add_argument("-r", "--relatedness_dict", required=True, action="store", help="Relatedness persistent dictionary file")
    parser.add_argument("-g", "--gcube_token", required=True, action="store", help="Tagme authentication gcube token")
    parser.add_argument("-t", "--topics", required=True, action="store", help="Topic id-description mapping file")
    parser.add_argument("-q", "--qrels", required=True, action="store", help="Qrel file")
    parser.add_argument("-w", "--wiki_api_endpoint", required=True, action="store", help="Wikipedia API endpoint")
    parser.add_argument("-n", "--benchmark_name", required=True, action="store", help="Benchmark name")
    args = parser.parse_args()

    topics = dict((topic_id, t_desc) for topic_id, t_desc in topics_generator(args.topics))
    queries = list(set(sorted((topic_id, topics[topic_id]) for topic_id, _, _ in qrels_generator(args.qrels))))
    initialize_ef_processor(
        args.database_name,
        args.lucene_dir,
        args.relatedness_dict,
        args.wiki_api_endpoint,
        args.cache_dir
    )

    pool = Pool(
        initializer=initialize_ef_processor,
        initargs=(
            args.database_name,
            args.lucene_dir,
            args.relatedness_dict,
            args.wiki_api_endpoint,
            args.cache_dir)
    )

    cached_results = []
    try:
        logger.info("Computing scores for queries/authors")
        logger.info("====================================")
        for i, query_result in enumerate(pool.imap_unordered(ef_processor, queries)):
            cached_results.append(query_result)
            logger.info("\r=====PROGRESS=====: %d/%d", i, len(queries))
    except KeyboardInterrupt:
        pool.terminate()
        pool.join()
    except Exception as e:
        logger.error('Uncaught exception in worker process:\n')
        traceback.print_exc()
        raise e

    # cached_results = []
    # logger.info("Computing scores for queries/authors")
    # logger.info("====================================")
    # for i, query in enumerate(queries):
    #     cached_results.append(ef_processor(query))
    #     logger.info("\r=====PROGRESS=====: %d/%d", i+1, len(queries))

    results = gridsearch_generator(cached_results, num_parameters)
    write_results(args.qrels, results, args.benchmark_name)


if __name__ == "__main__":
    sys.exit(main())
