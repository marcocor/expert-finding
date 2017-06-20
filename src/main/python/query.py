#!/usr/local/bin/python2.7
# encoding: utf-8

from __future__ import division

from argparse import ArgumentParser
import logging
logging.basicConfig(level=logging.DEBUG)
import sys
from expertfinding.core import ExpertFinding, scoring
import tagme

SCORING_FUNCTION = scoring.lucene_max_eciaf_norm_score

def main():
    '''Command line options.'''
    parser = ArgumentParser()
    parser.add_argument("-s", "--storage_db", required=True, action="store", help="Storage DB file")
    parser.add_argument("-d", "--database_name", required=True, action="store", help="MongoDB database name")
    parser.add_argument("-g", "--gcube_token", required=True, action="store", help="Tagme authentication gcube token")
    parser.add_argument("-l", "--lucene_dir", required=True, action="store", help="Lucene index root directory")
    args = parser.parse_args()

    tagme.GCUBE_TOKEN = args.gcube_token
    exf = ExpertFinding(storage_db=args.storage_db, database_name=args.database_name, lucene_dir=args.lucene_dir, erase=False)

    while True:
        query = raw_input("Query:")
        res = exf.find_expert(input_query=query, scoring_functions=[SCORING_FUNCTION])
        scoring_f_name = SCORING_FUNCTION.__name__.replace("_score", "")
        for result in res[scoring_f_name]:
            logging.info("{} ({}) score={:.3f}".format(result["name"], result["author_id"], result["score"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
