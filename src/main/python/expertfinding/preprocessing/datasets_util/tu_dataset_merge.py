import codecs
import os
import unicodecsv as csv


TRANSLATED_TEXT_DIR = "data/tu-translated/tu-translated-text/"
ASSOCIATIONS = "data/tu-expert-collection/data/assoc/all.assoc"
OUTPUT = "data/tu-translated/full.csv"

def load_document(doc_id):
    path = os.path.join("{}{}.txt".format(TRANSLATED_TEXT_DIR, doc_id))
    if not os.path.exists(path):
        return None
    with codecs.open(path, encoding="utf-8") as f:
        return f.read()

def load_assoc():
    with open(ASSOCIATIONS) as f:
        return dict(list(reversed(line.strip().split(" ")[0:2])) for line in f)


doc_to_author = load_assoc()

with open(OUTPUT, "w") as f:
    w = csv.writer(f, encoding="utf-8")

    for doc_id in doc_to_author:
        doc_data = load_document(doc_id)
        if doc_data:
            w.writerow([doc_id, doc_to_author[doc_id], doc_data])