# encoding: utf-8

from argparse import ArgumentParser
import codecs
import logging
from subprocess import check_output
import sys
import tagme

from expertfinding import ExpertFinding as EF


SCORING_FUNCTIONS = {foo.func_name: foo
                     for foo in [
                         EF.cossim_efiaf_score,
                         EF.efiaf_score,
                         EF.eciaf_score,
                         EF.log_ec_ef_iaf_score
                         ]
                    }


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


def main():
    parser = ArgumentParser()
    parser.add_argument("-s", "--storage_db", required=True, action="store", help="Storage DB file")
    parser.add_argument("-g", "--gcube_token", required=True, action="store", help="Tagme authentication gcube token")
    parser.add_argument("-t", "--topics", required=True, action="store", help="Topic id-description mapping file")
    parser.add_argument("-q", "--qrels", required=True, action="store", help="Qrel file")
    parser.add_argument("-f", "--scoring", required=True, action="store", nargs="+", help="Name of scoring functions tu test", choices=SCORING_FUNCTIONS.keys())
    args = parser.parse_args()

    tagme.GCUBE_TOKEN = args.gcube_token

    exf = EF(args.storage_db, False)
    
    topics = dict((topic_id, t_desc) for topic_id, t_desc in topics_generator(args.topics))

    query_ids = sorted(topic_id for topic_id, _, _ in qrels_generator(args.qrels))
    
    for scoring_foo in [SCORING_FUNCTIONS[scoring_f_name] for scoring_f_name in args.scoring]: 
        results = dict(
            (
                q_id,
                exf.find_expert(topics[q_id], scoring_foo)[0],
            ) for q_id in query_ids)
        results_filename = "{}.results".format(scoring_foo.func_name)
        with open(results_filename, "w") as results_f:
            for q_id in results:
                for hit in results[q_id]:
                    results_f.write("{} 0 {} 0 {} {}\n".format(q_id, hit["author_id"], hit["score"], scoring_foo.func_name))
        evaluation = check_output(["trec_eval", "-c", "-q", "-M", "1000", "-m", "all_trec", args.qrels, results_filename])
        print evaluation
        with open("{}.eval".format(scoring_foo.func_name), "w") as eval_f:
            eval_f.write(evaluation)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    sys.exit(main())
