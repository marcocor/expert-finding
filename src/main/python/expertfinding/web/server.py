'''
Created on Feb 21, 2017

@author: marco
'''

from argparse import ArgumentParser
from flask import Flask, jsonify, request
import sys
import tagme
import logging
from expertfinding import ExpertFinding
import os

app = Flask(__name__, static_folder=os.path.join("..", "..", "..", "resources", "web"), static_path="/static")

@app.route('/query')
def find_expert():
    global exf
    query = request.args.get('q')
    res_efiaf, time_efiaf = exf.find_expert(query, ExpertFinding.efiaf_score)
    res_cosim_efiaf, time_cosim_efiaf = exf.find_expert(query, ExpertFinding.cossim_efiaf_score)
    return jsonify(experts_efiaf = res_efiaf,
                   time_efiaf = time_efiaf,
                   experts_cossim_efiaf = res_cosim_efiaf,
                   time_cossim_efiaf = time_cosim_efiaf,
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
