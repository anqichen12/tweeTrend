import sys
import json
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext, saveToCassandra
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
from pyspark.sql.functions import unix_timestamp
import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import *

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process(rdd):
    spark = getSparkSessionInstance(rdd.context.getConf())
    tweetsDataFrame = spark.read.json(rdd)
    tweetsDataFrame.show()
    tweetsDataFrame.createOrReplaceTempView("spotify")
    df = spark.sql("SELECT name, MAX(popularity) as popularity, MAX(followers) as followers, COUNT(*) FROM spotify GROUP BY name")
    df.show()

if __name__ == "__main__":
    sc = CassandraSparkContext(appName="tweet")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc,60)
    topic_name = "spotify"
    streamFromKafka = KafkaUtils.createDirectStream(ssc, [topic_name],{"metadata.broker.list":'*'})#for i in range(50):
    lines = streamFromKafka.map(lambda x: x[1])
    lines.foreachRDD(process)
    lines.count().map(lambda x:'messages in this batch: %s' % x).pprint()
    ssc.start() 
    ssc.awaitTermination()
