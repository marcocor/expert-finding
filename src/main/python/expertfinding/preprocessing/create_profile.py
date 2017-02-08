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
    parser.add_argument("-n", "--names", required=True, action="store", nargs="+", help="Names of people to profile")
    parser.add_argument("-s", "--storage_db", required=True, action="store", help="Storage DB file")
    args = parser.parse_args()

    exf = ExpertFinding(args.storage_db, False)
    for name in args.names:
        a_id = exf.author_id(name)[0]
        print a_id, exf.name(a_id), exf.institution(a_id)
        for entity, freq, iaf, ef_iaf, max_rho, years in exf.ef_iaf(a_id)[:50]:
            years_freq = ", ".join("{}{}".format(y, "(x{})".format(freq) if freq > 1 else "") for y, freq in sorted(Counter(years).items(), key=lambda p: p[0]))
            print u"{} freq={:.1%} importance={:.2f} entity_rarity={:.2f} max_rho={:.3f} years={}".format(entity, freq, ef_iaf*100, iaf, max_rho, years_freq)
        print
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
