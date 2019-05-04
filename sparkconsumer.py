from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
from owner_elasticsearch import *
es = connect_elasticsearch()
create_index(es,twt_index)
sc = SparkContext(appName ="twitters")
ssc = StreamingContext(sc, 10)   # Create a streami
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})
parsed = kafkaStream.map(lambda v: json.loads(v[1]))
authors_dstream = parsed.map(lambda tweet: store_record(es,twt_index,tweet))