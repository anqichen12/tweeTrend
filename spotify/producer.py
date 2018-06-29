import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import explode
from kafka import KafkaConsumer, KafkaProducer
from kafka import SimpleProducer, KafkaClient
import json


def produce(artist_list, topic_name):
    while True:
        for artist in artist_list:
            res = sp.search(q='artist:' + artist, type='artist')
            msg_info = {'name':res['artists']['items'][0]['name'],
		        'followers':res['artists']['items'][0]['followers']['total'],
		        'popularity':res['artists']['items'][0]['popularity'],
		        'url':res['artists']['items'][0]['external_urls']['spotify']}
            producer.send(topic_name, json.dumps(msg_info))



if __name__ == "__main__":
    #spotify setup
    spotify = spotipy.Spotify()
    client_id = '*'
    client_secret = '*'
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    artist_list = set()

    with open('../data/lower-artist.csv', 'r') as f:
        count =1
    	for i in f:
            cur = i.split("\n")[0]
            if count in range(100):
                artist_list.add(cur)
            count+=1

    producer = KafkaProducer(bootstrap_servers='*')
    topic_name = 'spotify'
    produce(artist_list, topic_name)
