from elasticsearch import Elasticsearch
import json
from pprint import pprint

twt_index = 'tweets'
twt_type = 'capstone'


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Connected!!!')
    else:
        print('it could not connect!')
    return _es


def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, doc_type=twt_type, body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "capstone": {
                "dynamic": "strict",
                "properties": {
                    "user": {
                        "type": "text",
                        "index": "not_analyzed"

                    },
                    "text": {
                        "type": "text"
                    },
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created
