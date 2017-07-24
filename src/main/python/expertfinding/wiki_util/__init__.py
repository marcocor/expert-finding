import requests
import logging
logger = logging.getLogger("EF_log")
import expertfinding
import time

API_ENDPOINT=None
_request_sleep_time=0.05

def set_cache(cache):
    expertfinding.core.wiki_util.rank_single = cache(_rank_single)
    expertfinding.core.wiki_util.text = cache(_text)
    expertfinding.core.wiki_util.rel = cache(_rel)

def rank(srcWikiIDs, method="jaccard"):
    entities_list = []
    for srcWikiID in srcWikiIDs:
        entities_list += rank_single(srcWikiID, method)
    return entities_list

def _rank_single(srcWikiID, method):
    response = _issue_request("{}/rank".format(API_ENDPOINT), params={"srcWikiID": srcWikiID, "method": method})
    return response["rankedEntities"]

def _text(text):
    response = _issue_request("{}/text".format(API_ENDPOINT), params={"text": text})
    return response["rankedEntities"]


def _rel(srcWikiID, dstWikiID, method="jaccard"):
    response = _issue_request("{}/rel".format(API_ENDPOINT), params={"srcWikiID": srcWikiID, "dstWikiID": dstWikiID, "method": method})
    return response["relatedness"]

def multi_rel(wikiIDCouples, method="Jaccard"):
    return [rel(src, dst, method) for (src, dst) in wikiIDCouples]


def _issue_request(path, params):
    response = None
    while response is None:
        try:
            return requests.post(path, params=params).json()
        except:
            time.sleep(_request_sleep_time)
            continue