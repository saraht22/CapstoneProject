from owner_elasticsearch import *
from countminsketch import *
import re
from collections import Counter
import sys
import time

def search(es_object, index_name, search):
    res = es_object.search(index=index_name, body=search)
    pprint(res)
    return res


def query_all():
    return {"query": {"match_all": {}}}


def match_query(str):
    return {"query": {"match": {"text": str}}}


def term_query(user):
    return {"query": {"term": {"user": user}}}


def bool_query(user, str):
   return {"query": {"bool": {"must": { "term": { "user": user }}, "must_not": { "match": {"text":text}}}}}

def tokenize(string):
    words = re.compile('(\w+)')
    for m in re.finditer(words, string):
        yield m.group(1)


def conduct_countMinTopK(data, k):
    freq = {}
    for doc in data:
        for tok in tokenize(doc['_source']['text']):
            cm_sketch.update(tok, 1)
            freq[tok] = cm_sketch.estimate(tok)
    d = Counter(freq)
    print(d.most_common(k))


es = connect_elasticsearch()
cm_sketch = CountMinSketch(1024, 3)
if es is not None:
    print("1. do full text match query.\n 2. do term query.\n 3 do bool query.\n 4 conduct count min alogrithm find top 5.\n 0 exit\n")
    while True:
        cmd = input("enter choice:")
        if (cmd == '1'):
            text = input("enter the text you want to match:")
            match_search = match_query(text)
            print("\n*****result of match query*******\n")
            search(es, twt_index, json.dumps(match_search))
        elif(cmd == '2'):
            text = input("enter username:")
            term_search = term_query(text)
            print("\n*****result of term query********\n")
            search(es, twt_index, json.dumps(term_search))
        elif(cmd == '3'):
            user = input("must have user:")
            text = input("must not have text:")
            print("\n*****result of bool query ********\n")
            bool_search = bool_query("charles_tindol", "Thank")
            search(es, twt_index, json.dumps(bool_query))
        elif(cmd == '4'):
            cont = 0
            while True:
                if(cont> 100):
                    break
                match_search = match_query("Trump")
                result = search(es, twt_index, json.dumps(match_search))
                print("\n*******top 5 using countmin algorithm\n")
                conduct_countMinTopK(result['hits']['hits'], 5)
                cont=cont+1
                time.sleep(2) # Delay 
        else:
            break