#!/usr/local/bin/python2.7
# encoding: utf-8

from __future__ import division

from argparse import ArgumentParser
import logging
import sys
from expertfinding.core import ExpertFinding
from collections import Counter


def main():
    '''Command line options.'''
    parser = ArgumentParser()
    parser.add_argument("-s", "--storage_db", required=True, action="store", help="Storage DB file")
    parser.add_argument("-d", "--database_name", required=True, action="store", help="MongoDB database name")
    args = parser.parse_args()

    exf = ExpertFinding(args.storage_db, args.database_name, False)
    exf.print_documents_quantiles()
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
