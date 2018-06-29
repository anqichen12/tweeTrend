import tweepy
import time
from kafka import KafkaConsumer, KafkaProducer
import jsonpickle
from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from datetime import datetime, timedelta

# twitter setup
consumer_key = "*"
consumer_secret = "*"
access_token = "*"
access_token_secret = "*"
# Creating the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)
# Creating the API object by passing in auth information
api = tweepy.API(auth)


def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    mytime -= timedelta(hours=4)   # the tweets are timestamped in GMT timezone, while I am in -4 timezone
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

topic_name = "twitter"

producer = KafkaProducer(bootstrap_servers='*',batch_size=100000,linger_ms=100)

class StdOutListener(StreamListener):
    def on_status(self, status):
        msg_info = {'text':status.text,'time':normalize_timestamp(str(status.created_at))}
        producer.send(topic_name, json.dumps(msg_info))

    def on_error(self, status_code):
        if status_code == 420:
            return False


if __name__ == "__main__":
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    artist_list = set()
    with open('../data/lower-artist.csv', 'r') as f:
        count =1
        for i in f:
            cur = i.split("\n")[0]
            if count in range(400):
                artist_list.add(cur)
            count+=1
    stream.filter(track=artist_list)
        


