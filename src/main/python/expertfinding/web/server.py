'''
Created on Feb 21, 2017

@author: marco
'''

from argparse import ArgumentParser
from flask import Flask, jsonify, request
import sys
import tagme

from expertfinding import ExpertFinding

app = Flask(__name__)

@app.route('/query')
def find_expert():
    global exf
    query = request.args.get('q')
    res = exf.find_expert(query)
    return jsonify(experts = res)

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
    sys.exit(main())