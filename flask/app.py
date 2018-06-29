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

@app.route('/', methods=['POST'])
def get_email():
	method = request.form["method"]
	time = request.form["time"]
	if method == '10 minutes':
		qtime = datetime.now()- timedelta(minutes=10)
		#qtime = datetime.strptime(qtime, "%Y-%m-%d %H:%M:%S")
		qtime = qtime.strftime("%Y-%m-%d %H:%M:%S")
		stmt = "select name, sum(count) as count from realtime where time >= '" + qtime + "' group by name LIMIT 10 ALLOW FILTERING"
		#stmt = "select name, sum(count) as count from realtime where time > '" + qtime + "' group by date, name LIMIT 10 ALLOW FILTERING"
		response = sessionTweets.execute(stmt)
		count_list = []
		heapq.heapify(count_list)
		count = 0;
		for val in response:
			count+=1
			if count<=10:
				heapq.heappush(count_list, pair(val.count, val.name))
			else:
				if val.count > count_list[0].count:
					heapq.heappop(count_list)
					heapq.heappush(count_list, pair(val.count, val.name))
		res = []
		for i in range(0,10):
			item = heapq.heappop(count_list)
			res.append({"name": item.name, "count": item.count})
		return render_template("10minutes.html", output=res)
	elif method =='1 hour':
		qtime = datetime.now()- timedelta(hours=1)
		#qtime = datetime.strptime(qtime, "%Y-%m-%d %H:%M:%S")
		qtime = qtime.strftime("%Y-%m-%d %H:%M:%S")
		stmt = "select name, sum(count) as count from realtime where time >= '" + qtime + "' group by name LIMIT 10 ALLOW FILTERING"
		#stmt = "select name, sum(count) as count from realtime where time > '" + qtime + "' group by date, name LIMIT 10 ALLOW FILTERING"
		response = sessionTweets.execute(stmt)
		count_list = []
		heapq.heapify(count_list)
		count = 0;
		for val in response:
			count+=1
			if count<=10:
				heapq.heappush(count_list, pair(val.count, val.name))
			else:
				if val.count > count_list[0].count:
					heapq.heappop(count_list)
					heapq.heappush(count_list, pair(val.count, val.name))
		res = []
		for i in range(0,10):
			item = heapq.heappop(count_list)
			res.append({"name": item.name, "count": item.count})
		return render_template("1hour.html", output=res)
	#return jsonify(res=res)

class pair:
	def __init__(self, count, name):
		self.count = count
		self.name = name
	def __gt__(self, other):
		if self.count == other.count:
			return self.name < other.name
		return self.count < other.count
	def __eq__(self, other):
		return self.count == other.count and self.name == other.name



# @app.route("/gettrend")
# def trend():
#   rowsTweets = sessionTweets.execute("select name, MAX(count) as count from tweets group by name limit 10")
#   text = []
#   for val in rowsTweets:
#       text.append(val)
#     jsonresponse = [{"artist": x.name, "count": x.count} for x in text]
#     return jsonify(data=jsonresponse)
 
#@app.route('/twitter', methods=['POST'])
#def twitter_or_spotify():
	#if request.method == 'POST': 
		#if request.form.get('media')=='twitter':
#   select = request.form['media']
#   return select 

	#method = request.form["method"]
	#return render_template("test.html")
	#time = request.form["time"]
	#if method == 'twitter':
		#q = "SELECT * FROM pagerank where date = '%s' LIMIT 10" % keywords 
		#response = session.execute(q)
		#response_list = []
		#for val in response:
			#response_list.append(val)
		#jsonresponse = [{"date": x.date, "rank": x.rank, "topic": x.topic} for x in response_list]
		


if __name__ == "__main__":
	app.run()
