from collections import namedtuple
import logging
import re
import string

import unicodecsv as csv 


INPUT_ENCODING_UNIPI = "windows-1252"
INPUT_ENCODING_TU = "utf-8"
SUPPORTED_FORMATS = ['unipi', 'tu']

Paper = namedtuple('Paper', ['author_id', 'name', 'institution', 'year', 'abstract', 'doi'])


def normalize_author(name_field, lastname_field):
    return re.sub(r"\W+", " ", string.capwords("{} {}".format(name_field, lastname_field)).strip())


def paper_generator_unipi(filename, encoding=INPUT_ENCODING_UNIPI):
    with open(filename, 'rb') as input_f:
        for i, l in enumerate(csv.reader(input_f, delimiter=';', encoding=encoding)):
            if len(l) != 20:
                logging.debug("Discarding line %d %d %s" % (i, len(l), l))
                continue
            yield Paper(l[0], normalize_author(l[2], l[1]), l[4], int(l[6]), l[13], l[11])


def paper_generator_tu(filename, encoding=INPUT_ENCODING_TU):
    with open(filename) as f:
        r = csv.reader(f, encoding=encoding)
        for doc_id, author_id, text in r:
            yield Paper(author_id, "Name of {}".format(author_id), "Institution of {}".format(author_id), 2017, text, None)


def paper_generator(i_file, i_format):
    if i_format not in SUPPORTED_FORMATS:
        raise Exception("Format '{}' not supported".format(i_format))
    if i_format == 'unipi':
        return paper_generator_unipi(i_file)
    if i_format == 'tu':
        return paper_generator_tu(i_file)
