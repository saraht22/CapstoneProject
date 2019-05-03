import json
import tweepy
import threading, logging, time
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from elasticsearch import Elasticsearch
from owner_elasticsearch import *

# Filter tweets with specific format
def cleantweet(data):
    # raw format of tweets
    rawtweet = json.loads(data)
    tweet = {}
    tweet["user"] = rawtweet["user"]["screen_name"]
    # If in record, append full text; else append record
    if "extended_tweet" in rawtweet:
        tweet["text"] = rawtweet["extended_tweet"]["full_text"]
    else:
        tweet["text"] = rawtweet["text"]
    return json.dumps(tweet)

# Main part of Stream Producer
class MyListener(StreamListener):

    def __init__(self, api):
        self.api = api
        self.count = 0

    # self.producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def on_data(self, data):
        result = cleantweet(data)
        if es is not None:
            if create_index(es, twt_index):
                out = store_record(es, twt_index, result)
                print('Data indexed successfully')
        self.count = self.count + 1
        if (self.count > 1000):
            return False
        return True

    def on_error(self, status):
        print(status)
        return True


es = connect_elasticsearch()
# Neccessary credentials required to aunthenticate the connection to access twitter from the API.
consumer_key = '20mDwSArebE3pFRe5sVuSkIpz'
consumer_secret = 'owjSWC1Se17f2T7Ju9nhobCcmNkaP0HX8NithD4YJrmoeHMYXb'
access_token = '891506843591335937-z4ncLq1auGs3MgFvd29RTATJGziASmy'
access_secret = '1L2xcb49ZXbkRppxPX7hQ6iutomjKpAsMh0uAjDeHMsX0'
authorize = OAuthHandler(consumer_key, consumer_secret)
authorize.set_access_token(access_token, access_secret)
api = tweepy.API(authorize)
twitter_stream = Stream(authorize, MyListener(api))
twitter_stream.filter(track=['#trump'])
