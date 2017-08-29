#!/usr/local/bin/python2.7
# encoding: utf-8

from __future__ import division

from argparse import ArgumentParser
import logging
logging.basicConfig(level=logging.CRITICAL)
logger = logging.getLogger("EF_log")
logger.setLevel(logging.DEBUG)
import sys
from expertfinding.core import ExpertFinding, scoring
import tagme

SCORING_FUNCTION = scoring.lucene_max_eciaf_norm_score


def main():
    '''Command line options.'''
    parser = ArgumentParser()
    parser.add_argument("-d", "--database_name", required=True, action="store", help="MongoDB database name")
    parser.add_argument("-g", "--gcube_token", required=True, action="store", help="Tagme authentication gcube token")
    parser.add_argument("-l", "--lucene_dir", required=True, action="store", help="Lucene index root directory")
    parser.add_argument("-w", "--wiki_api_endpoint", required=True, action="store", help="Wikipedia API endpoint")
    parser.add_argument("-c", "--cache_dir", required=True, action="store", help="Cache directory")
    parser.add_argument("-f", "--scoring", required=True, action="store", help="Name of scoring functions tu test")
    args = parser.parse_args()

    tagme.GCUBE_TOKEN = args.gcube_token
    exf = ExpertFinding(database_name=args.database_name,
                        lucene_dir=args.lucene_dir,
                        wiki_api_endpoint=args.wiki_api_endpoint,
                        cache_dir=args.cache_dir,)
    scoring_structure = scoring.ScoringStructure(args.scoring)

    while True:
        query = raw_input("Query:")
        results, _, _ = exf.find_expert(input_query=query, scoring_structure=scoring_structure)
        i = 0
        for result in results:
            logger.info("{}) {} ({}) score={:.3f}".format(
                i, result["name"], result["author_id"], result["score"]))
            i += 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
