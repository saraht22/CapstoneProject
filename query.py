from owner_elasticsearch import *
from countminsketch import *
import re
from collections import Counter


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
    return {'query': {'bool': {
        'must': {
            "term": {
                "user": user
            }
        },
        "must_not": {
            'match': {'text': str}
        },
    }
    }
    }


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
    term_search = term_query("2020chooselove")
    bool_search = bool_query("2020chooselove", "happy")
    match_search = match_query("trump")
    print("\n*****result of term query********\n")
    result1 = search(es, twt_index, json.dumps(term_search))
    print("\n*****result of bool query********\n")
    result2 = search(es, twt_index, json.dumps(bool_search))
    print("\n*****result of match query*******\n")
    result3 = search(es, twt_index, json.dumps(match_search))
    print("\n*******top 5 using countmin algorithm\n")
    conduct_countMinTopK(result3['hits']['hits'], 5)
