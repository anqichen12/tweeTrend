import time
from datetime import datetime, timedelta
import re
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import explode
from kafka import KafkaConsumer, KafkaProducer
import jsonpickle
from kafka import SimpleProducer, KafkaClient
import json
import unicodedata

def normalize_timestamp(t):
    mytime = datetime.strptime(unicode(t),'%a %b %d %H:%M:%S +0000 %Y')
    mytime -= timedelta(hours=4)   # the twieets are timestamped in GMT timezone, while I am in -4 timezone
    return mytime.strftime('%Y-%m-%d %H:%M:%S')

def get_timestamp(string):
    string1 = string.split('created_at')[1].split('\"')[2]
    return normalize_timestamp(string1)


def get_text(string):
    string1 = string.split('\"')[7]
    return unicodedata.normalize('NFKD', string1).encode('ascii','ignore')


def send_to_kafka(rows):
    topic_name = "twitter"
    producer = KafkaProducer(bootstrap_servers='*')
    count = 1
    for row in rows:
        if count==1:
            text = row.encode("utf-8")
            count+=1
            continue
        if count==2:
            time = row.encode("utf-8")
            msg = {"text":text, "time":time}
            producer.send(topic_name,json.dumps(msg))
            count=1
        producer.flush()

if __name__ == "__main__":    
    conf = SparkConf().setAppName("jsonParser")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sparkContext=sc)
    sourcelocation = "*"
    conf = SparkConf().setAppName("jsonParser")
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "*")
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "*")
    lines = sc.textFile(sourcelocation)
    text_rdd = lines.map(lambda p: Row(text=get_text(p), time=get_timestamp(p)))
    text_rdd.foreach(send_to_kafka)
    sc.stop()
