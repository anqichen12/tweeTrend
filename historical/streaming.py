import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext, saveToCassandra
from pyspark.sql.functions import unix_timestamp
import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import *
from datetime import datetime


artist_list = set()
with open('../data/lower-artist.csv', 'r') as f:
    count =1
    for i in f:
        cur = i.split("\n")[0]
        if count in range(100):
            artist_list.add(cur)
        count+=1

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


@udf('string')

def func(text):
    for word in artist_list:
    	if word in text.lower():
    		return word

@udf('string')

def transfer_time(text):
    #return "2018-06-25"
    return datetime.today().strftime("%Y-%m-%d %H:%M:%S")

def process(rdd):
    spark = getSparkSessionInstance(rdd.context.getConf())
    tweetsDataFrame = spark.read.json(rdd)
    df = tweetsDataFrame.withColumn('hashtag', func(tweetsDataFrame.text))
    df = df.withColumn('date',transfer_time(tweetsDataFrame.time))
    df.createOrReplaceTempView("historicaltweets")
    df = spark.sql("SELECT MAX(date) AS date,hashtag,count(*) AS count FROM historicaltweets WHERE hashtag IS NOT NULL GROUP BY hashtag ORDER BY count DESC")
    rdd = df.rdd.map(tuple)
    rdd.saveToCassandra("twitter","tweet")
    df.show()

if __name__ == "__main__":
    sc = CassandraSparkContext(appName="tweet")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc,600)
    topic_name = "twitter"
    streamFromKafka = KafkaUtils.createDirectStream(ssc, [topic_name],{"metadata.broker.list":'*'})
    lines = streamFromKafka.map(lambda x: x[1])
    lines.count().pprint()
    lines.foreachRDD(process)
    #text_counts = lines.map(lambda tweet: (tweet['hashtag'],1)).reduceByKey(lambda x,y: x + y)
    ssc.start() 
    ssc.awaitTermination()



