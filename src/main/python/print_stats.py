#!/usr/local/bin/python2.7
# encoding: utf-8

from __future__ import division

from argparse import ArgumentParser
import logging
logging.basicConfig(level=logging.CRITICAL)
import sys
from expertfinding.core import ExpertFinding
from collections import Counter


def main():
    '''Command line options.'''
    parser = ArgumentParser()
    parser.add_argument("-d", "--database_name", required=True, action="store", help="MongoDB database name")
    args = parser.parse_args()

    exf = ExpertFinding(database_name=args.database_name, erase=False)
    exf.print_documents_quantiles()
    return 0


if __name__ == "__main__":
    logging.getLogger("EF_log").setLevel(logging.DEBUG)
    sys.exit(main())
