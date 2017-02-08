#!/usr/local/bin/python2.7
# encoding: utf-8
'''
expertfinding.main -- shortdesc
expertfinding.main is a description
It defines classes_and_methods

@author:     user_name
@copyright:  2016 organization_name. All rights reserved.
@license:    license
@contact:    user_email
@deffield    updated: Updated
'''

from __future__ import division

from argparse import ArgumentParser
import logging
import numpy
import pyfscache
import sys

from expertfinding import ExpertFinding
import expertfinding
from expertfinding.preprocessing import datasetreader
import unicodecsv as csv
from glob import glob

MIN_YEAR, MAX_YEAR = 2006, 2017


def main():
    '''Command line options.'''
    parser = ArgumentParser()
    parser.add_argument("-i", "--input", required=True, action="store", help="Input file(s)")
    parser.add_argument("-f", "--input_format", required=True, action="store", help="Format of input file(s)", choices=datasetreader.SUPPORTED_FORMATS)
    parser.add_argument("-c", "--cache_dir", required=True, action="store", help="Cache directory")
    parser.add_argument("-s", "--storage_db", required=True, action="store", help="Storage DB file")
    parser.add_argument("-g", "--gcube_token", required=True, action="store", help="Tagme authentication gcube token")
    args = parser.parse_args()

    expertfinding.set_cache(args.cache_dir)

    ef = ExpertFinding(args.storage_db)

    for input_f in glob(args.input):
        ef.read_papers(input_f, datasetreader.paper_generator(input_f, args.input_format), MIN_YEAR, MAX_YEAR)

    a_id = ef.author_id("Paolo Ferragina")[0]
    print a_id, ef.name(a_id), ef.institution(a_id)
    print ef.entities(a_id)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
