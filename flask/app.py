from flask import Flask, flash, redirect, render_template, request, session, abort, jsonify
from cassandra.cluster import Cluster
import time
import collections
import heapq
import functools
from datetime import datetime, timedelta

clusterTweets = Cluster(['*'])
sessionTweets = clusterTweets.connect('twitter')


app = Flask(__name__)
 
@app.route("/")
def index():
	return render_template("index.html")

@app.route('/',methods=['POST'])
def get_result():
        way = request.form["way"]
        name = request.form["name"]
        method = request.form["method"]
        genre = request.form["genre"]
        SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
        json_url = os.path.join(SITE_ROOT, "", "genre.json")
        dic = json.load(open(json_url))
        artists = dic[genre]
        if way == 'top 10':
            if method == '10 minutes':
                qtime = datetime.now()- timedelta(minutes=10)-timedelta(hours=4)
            elif method =='1 hour':
                qtime = datetime.now()- timedelta(hours=1)-timedelta(hours=4)
            qtime = qtime.strftime("%Y-%m-%d %H:%M:%S")
            query = "select name, time, sum(count) as count, followers from result where time >= '" + qtime + "' group by name ALLOW FILTERING"
            response = sessionTweets.execute(query)
            count_list = []
            for val in response:
                if val.name in artists:
                    count_list.append(count_tuple(val.count,val.name,val.followers))
            def getKey(item):
                return item.count
            r = sorted(count_list,key = getKey, reverse=True)
            res_count = []
            res_followers = []
            count = 0
            for i in range(10):
                if len(count_list)>i:
                    item = r[i]
                    res_count.append({"name": item.name, "count": item.count})
                    res_followers.append({"name": item.name, "followers": (item.followers)})
                else:
                    break
            for val in r:
                if method == '10 minutes':
                    return render_template("10minutes_top10.html", c=res_count, f=res_followers)
                elif method =='1 hour':
                    return render_template("1hour_top10.html", c=res_count, f=res_followers)
        if way =='trend':
                qtime = datetime.now()- timedelta(hours=9)
                qtime = qtime.strftime("%Y-%m-%d %H:%M:%S")
                query1 = "select time, count from twittertrend where time >= '" + qtime + "' and name = '" + name + "' ORDER BY time DESC LIMIT 5 ALLOW FILTERING"
                response1 = sessionTweets.execute(query1)
                query2 = "select time, followers from spotifytrend where time >= '" + qtime + "' and name = '" + name + "' ORDER BY time DESC LIMIT 5 ALLOW FILTERING"
                response2 = sessionTweets.execute(query2)
                res_count = []
                res_followers = []
                t1 = qtime
                stand_count = [[]]
                stand_followers = [[]]
                for val in response1:
                        stand_count[0].append(val.count)
                for val in response2:
                        stand_followers[0].append(val.followers)
                stand_count = np.array(stand_count)
                stand_followers = np.array(stand_followers)
                count_scaled = preprocessing.normalize(stand_count)
                followers_scaled = preprocessing.normalize(stand_followers)
                for i in range(len(count_scaled[0])):
                        t = datetime.strptime(t1,"%Y-%m-%d %H:%M:%S")
                        res_count.append({"time":(t+timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),"count":count_scaled[0][i]})
                        res_followers.append({"time":(t+timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),"followers":followers_scaled[0][i]})
                        t1 = datetime.strptime(t1,"%Y-%m-%d %H:%M:%S")+timedelta(hours=1)
                        t1 = t1.strftime("%Y-%m-%d %H:%M:%S")
                print(res_count)
                return render_template("1hour_trend.html", c=res_count, f=res_followers)

class count_tuple:
        def __init__(self, count, name, followers):
                self.followers = followers
                self.count = count
                self.name = name
        def __gt__(self, other):
                if self.count == other.count:
                        return self.name < other.name
                return self.count < other.count
        def __eq__(self, other):
                return self.count == other.count and self.name == other.name



if __name__ == "__main__":
	app.run()
