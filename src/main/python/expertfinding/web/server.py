'''
Created on Feb 21, 2017

@author: marco
'''

from argparse import ArgumentParser
from collections import Counter
from flask import Flask, jsonify, request, redirect
import flask
import logging
import os
import re
import sys
import tagme

from expertfinding import ExpertFinding


app = Flask(__name__, static_folder=os.path.join("..", "..", "..", "resources", "web"), static_path="/static")

@app.route('/')
def index():
    return redirect('/static/index.html')


@app.route('/document')
def get_document():
    global exf
    docid = int(request.args.get('d'))
    author_id, ret_doc_id, year, body = exf.document(docid)
    return jsonify(author_id = author_id, ret_doc_id = ret_doc_id, year = year, body = body)

@app.route('/documents')
def get_documents():
    global exf
    author_id = request.args.get('a')
    entities = flask.json.loads(request.args.get("e"))
    docid_to_year = dict()
    docid_to_entities = dict()
    for document_id, year, entity, entity_count in exf.documents(author_id, entities):
        docid_to_year[document_id] = year
        if document_id not in docid_to_entities:
            docid_to_entities[document_id] = []
        docid_to_entities[document_id].append({"entity": entity, "count": entity_count})
    
    return jsonify(dict((docid, {
                                    "year": docid_to_year[docid],
                                    "entities": docid_to_entities[docid]
                               }
                       ) 
                       for docid in docid_to_year
                      )
                 )

@app.route('/query')
def find_expert():
    global exf
    query = request.args.get('q')
    res_efiaf, time_efiaf, _ = exf.find_expert(query, ExpertFinding.efiaf_score)
    res_eciaf, time_eciaf, _ = exf.find_expert(query, ExpertFinding.eciaf_score)
    res_log_ec_ef_iaf, time_log_ec_ef_iaf, _ = exf.find_expert(query, ExpertFinding.log_ec_ef_iaf_score)
    res_cosim_efiaf, time_cosim_efiaf, query_entities = exf.find_expert(query, ExpertFinding.cossim_efiaf_score)
    return jsonify(experts_eciaf = res_eciaf,
                   time_eciaf = time_eciaf,
                   experts_efiaf = res_efiaf,
                   time_efiaf = time_efiaf,
                   experts_log_ec_ef_iaf = res_log_ec_ef_iaf,
                   time_log_ec_ef_iaf = time_log_ec_ef_iaf,
                   experts_cossim_efiaf = res_cosim_efiaf,
                   time_cossim_efiaf = time_cosim_efiaf,
                   query_entities = list(query_entities),
                   )

@app.route('/completion')
def complete_name():
    global exf
    query = re.sub(r"[%\s]+", "%", request.args.get('q'))
    return jsonify(authors=[{"id": author_id,
                             "name": name,
                             "institution": institution,
                            }
                            for author_id, name, institution in exf.authors_completion(query)])

@app.route('/author')
def author_info():
    global exf
    author_id = request.args.get('id')
    entity_freq = [{"entity": entity,
      "frequency": author_freq,
      "years": sorted(Counter([int(y) for y in years.split(",")]).items())
      }
    for entity, author_freq, years, _ in exf.author_entity_frequency(author_id)]
    
    entity_freq.sort(key=lambda e: e["frequency"], reverse=True)
    
    return jsonify(
        id=author_id,
        name=exf.name(author_id),
        papers_count=exf.author_papers_count(author_id),
        entities=entity_freq,
        )

def main():
    global exf
    '''Command line options.'''
    parser = ArgumentParser()
    parser.add_argument("-s", "--storage_db", required=True, action="store", help="Storage DB file")
    parser.add_argument("-g", "--gcube_token", required=True, action="store", help="Tagme authentication gcube token")
    args = parser.parse_args()

    tagme.GCUBE_TOKEN = args.gcube_token

    exf = ExpertFinding(args.storage_db, False)
    return app.run(host="0.0.0.0")
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    app.logger.addHandler(logging.StreamHandler())
    app.logger.setLevel(logging.DEBUG)
    sys.exit(main())
