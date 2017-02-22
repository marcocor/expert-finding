#!/usr/local/bin/python2.7
# encoding: utf-8

from __future__ import division

from argparse import ArgumentParser
import logging
import sys
from expertfinding import ExpertFinding
import tagme

def main():
    '''Command line options.'''
    parser = ArgumentParser()
    parser.add_argument("-s", "--storage_db", required=True, action="store", help="Storage DB file")
    parser.add_argument("-g", "--gcube_token", required=True, action="store", help="Tagme authentication gcube token")
    args = parser.parse_args()

    tagme.GCUBE_TOKEN = args.gcube_token

    exf = ExpertFinding(args.storage_db, False)
    while True:
        query = raw_input("Query:")
        res = exf.find_expert(query)
        for name, a_id, score in res:
            print "{} ({}) score={:.3f}".format(name, a_id, score)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    sys.exit(main())
