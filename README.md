tweeTrend
======================================

## Use Case

tweeTrend is a data pipeline to monitor and analyze artist trend in streaming tweets and compare the artist trend with spotify by finding top 10 artists in real-time manner. 
 
Twitter plays a significant role and has been a crucial social network tool nowadays. Some insights could be obtained from tweets. The project is going to investigate artist trend in tweets. Sometimes artist trend could be inconsistant between twitter and spotify. Twitter indicates a general trend of artist covering larger fields such as in movie, news, or fashion, while music websites like spotify, iTunes limits the field to only music. Taking Rihanna for example, her movie Ocean's 8 was released in June. By comparing the trend in twitter and spotify, it is found there is a huge increase in tweets mentioning Rihanna, while the follower count of Rihanna in spotify keeps nearly the same. Will the trend of artists in twitter have impact on music website? The project could be also applied to investigate the impact of artists in twitter on album sells, products that artists advertised, and even movies that artists take part in. 

# tweeTrend workflow
## The data pipeline is as shown below:
![pipeline](/data/data-pipeline.png?raw=true "pipeline")

## Data Source

There are two types of data source. One is historical data (100GB), which is pulled from data stored in AWS S3 bucket. The historical data is used to increase kafka producer throughput, since there is a limit in twitter API usage. Another one is real-time data, which is pulled from twitter API and spotify API. The list of artists is limited to 200 popular artists which is referred from Billboard and Spotify.  


## Data Ingestion

I used kafka producer to ingest data from twitter API, spotify API and historical twitter data. Twitter data is preprocessed by changing format of timestamp, changing unicode format of text, etc.  I changed historical data's timestamp to act as real-time tweets.

There are two topics I created for kafka producer:

1. twitter

kafka-topics.sh --create --zookeeper *:2181 --replication-factor 2 --partitions 3 --topic tweet

2. spotify

kafka-topics.sh --create --zookeeper *:2181 --replication-factor 2 --partitions 3 --topic spotifynew

I processed twitter and spotify data as json format, and send them to kafka producers. 

twitter:

text  | created_at

spotify:

name  |  followers  |   genre   |   timestamp


## Data Streaming

I used Spark streaming and Kafka integration to do data stream processing.

There are three lines of data streaming.

1.

stream-stream join of data coming from tweet and spotifynew topics:

I passed the tweet and spotifynew stream by 60 seconds, which I found could be the best performance of kafka.

Then I aggregated twitter stream to calculate total count grouped by each artist name, then joined two streams by artist name, and save the dataframe in cassandra. 

2. 

tweet and spotify stream seperately:

I passed Spark context along with the batch duration 3600 seconds. The two streams is created for comparing the trend of twitter and spotify in every one hour.

3.

tweet stream:

I passed Spark context along with the batch duration 1 second. The stream is created for visualizing real-time twitter count every second.

## Database

I used cassandra to store database and created twitter keyspace with replication_factor of 4. There are 4 tables in total I created.

table result:

name (PRIMARY KEY)  |  count (CLUSTERING COLUMN) |  time  |  followers

table live:

name (PRIMARY KEY)  |  time (CLUSTERING COLUMN) |  count 

table twittertrend:

name (PRIMARY KEY)  |  time (CLUSTERING COLUMN) |  count

table spotifytrend:

name (PRIMARY KEY)  |  time (CLUSTERING COLUMN) |  follwers

## Frontend

The frontend is implemented by flask. The charts were created by D3.js and chart.js.

port 5000 main page:

On the main page, user can choose two functions: view top 10  or trend compare between twitter and spotify

1. top 10

User can find most popular artists in twitter in last 10 minutes or 1 hour in a chosen genre.

2. trend

User can find trend of a chosen artist in twitter (tweets that mentioned artist) and spotify (artist's followers) in last 5 hours.

port 8050 main page:

The page shows a dynamic graph of real-time tweet count for a chosen artist every second.

## Demo

webpage 1: https://youtu.be/qmN_arTKqfE
webpage 2: https://youtu.be/poqRaxzI28M


