#!/usr/local/bin/python2.7
# encoding: utf-8

from __future__ import division

from argparse import ArgumentParser
import logging
import sys
from expertfinding import ExpertFinding
import expertfinding
from collections import Counter


def main():
    '''Command line options.'''
    parser = ArgumentParser()
    parser.add_argument("-s", "--storage_db", required=True, action="store", help="Storage DB file")
    args = parser.parse_args()

    exf = ExpertFinding(args.storage_db, False)
    exf.print_documents_quantiles()
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
