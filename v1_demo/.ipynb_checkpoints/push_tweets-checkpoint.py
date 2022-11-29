import elasticsearch
import json
import pandas as pd
from datetime import datetime
import ast
import re
from elasticsearch import Elasticsearch
from elasticsearch import helpers
		


class dataFeeder:
	def __init__(self,query,since,till,event):
		self.query = query
		self.since = since
		self.event = event
		self.till = till
		
		self.to_take = ['id','es_date','content','retweetCount','likeCount',
        'lang','source','hashtags','user_username','user_id', 'user_displayname', 
        'user_description',  'user_verified', 'user_created',
       'user_followersCount', 'user_friendsCount', 'user_statusesCount',
       'user_favouritesCount','user_location','vaderSentiment', 'usernames', 'subjectivity','mentions','event']
		
	def filterKeys(self,document):
		
		return {key: document[key] for key in self.to_take }
	
	def safe_date(self,date_value):
		return (
			pd.to_datetime(date_value) if not pd.isna(date_value)
				else  datetime(1970,1,1,0,0)
		)
		
	def push_data(self):
		
		
		df = pd.read_csv(f'./data/{self.event}/csv/{self.since}_{self.till}.csv')
		df['es_date'] = df['date'].apply(self.safe_date)
		
		df = df[self.to_take]
		df = df.dropna()
		
		df_iter = df.iterrows()
		index, document = next(df_iter)
		
		es_client = Elasticsearch([{'host': 'localhost', 'port': 9200,'http_auth' : ("elastic", "yesYoyo@123")}])
		def doc_generator(df):
			df_iter = df.iterrows()
			for index, document in df_iter:
				yield {
						"_index": 'dashboard_demo',
						"_type": "_doc",
						"_id" : f"{document['id']}",
						"_source": self.filterKeys(document),
					}
		#     raise StopIteration
		helpers.bulk(es_client, doc_generator(df))


		
		
