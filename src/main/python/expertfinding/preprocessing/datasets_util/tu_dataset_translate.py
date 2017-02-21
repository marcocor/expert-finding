import codecs
import glob
from lxml import etree
from langdetect import detect
import re
from collections import Counter
import subprocess
import os
import time

OUTDIR = "data/tu-translated/tu-translated-text/"
DOC_FILES = "data/tu-expert-collection/data/xml-dump/*/*.xml"
TOPIC_NL="data/tu-expert-collection/topics/topics_nl.csv"
TOPIC_TRANS_OUT="data/tu-translated/tu-translated-queries.csv"


def nice_string(s):
    if len(s) > 83:
        s = s[:80] + "..."
    return re.sub(r"\s", " ", s)


if not os.path.exists(OUTDIR):
    os.mkdir(OUTDIR)

lang_count = Counter()
for fn in glob.glob(DOC_FILES):
    tree = etree.parse(fn)
    text = " ".join(tree.xpath("//*[not(self::docno) and not(self::title)]/text()"))
    docno = tree.xpath("/*/docno/text()")
    if docno:
        docno = docno[0]
    else:
        docno = "NONE_" + os.path.basename(fn)

    outfile = os.path.join(OUTDIR, docno+".txt")
    if os.path.exists(outfile):
        print "File {} already exists.".format(outfile)
    else:
        try:
            lang = detect(text)
        except:
            print "could not find language for", nice_string(text)
            lang = "broken"

        lang_count[lang] += 1
    
        text_small = text[:5000]

        if lang != "en":
            print "translating", nice_string(text), lang
            p = subprocess.Popen('trans -brief -target en'.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            text_small, stderr = p.communicate(input=text_small)
            text_small = text_small.decode("utf-8")
        else:
            print "copying", nice_string(text), lang

        with codecs.open(os.path.join(OUTDIR, docno+".txt"), "w", "utf-8") as translated_f:
            translated_f.write(text_small)

        time.sleep(1)

with open(TOPIC_TRANS_OUT, "w") as topic_out:
    with open(TOPIC_NL) as topic_in:
        for line in topic_in:
            topic_id, topic_desc = re.match(r"([^;]*);(.*)", line.strip()).group(1,2)

            print "translating dk->en", nice_string(topic_desc)
            p = subprocess.Popen('trans -brief -source nl -target en'.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            topic_trans, stderr = p.communicate(input=topic_desc)
            topic_out.write("{};{}\n".format(topic_id, re.sub("\s", " ", topic_trans).strip()))
            time.sleep(1)

print lang_count
