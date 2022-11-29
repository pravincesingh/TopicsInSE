import elasticsearch  
import json
import pandas as pd
from datetime import datetime
import ast
import re
from elasticsearch import Elasticsearch
from elasticsearch import helpers
		


class dataFeeder:
	def __init__(self,query,since,until,till,event):
		self.query = query
		self.since = since
		self.until = until
		self.event = event
		self.till = till
		
		self.to_take = ['id','es_date','content','retweetCount','likeCount',
        'lang','source','hashtags','user_username','user_id', 'user_displayname', 
        'user_description',  'user_verified', 'user_created',
       'user_followersCount', 'user_friendsCount', 'user_statusesCount',
       'user_favouritesCount','user_location','vaderSentiment', 'usernames','status', 'subjectivity','mentions','event']
		
	def filterKeys(self,document):
		
		return {key: document[key] for key in self.to_take }
	
	def safe_date(self,date_value):
		return (
			pd.to_datetime(date_value) if not pd.isna(date_value)
				else  datetime(1970,1,1,0,0)
		)
		
	def push_data(self):
		
		
		df = pd.read_csv(f'./data/{self.event}/csv/Topic_{self.query}_{self.since}_{self.until}.csv')
		df['es_date'] = df['date'].apply(self.safe_date)
		
		df = df[self.to_take]
		df = df.dropna()
		
		df_iter = df.iterrows()
		index, document = next(df_iter)
		
		ELASTIC_PASSWORD="Shashank@2002"
    #CLOUD_ID="Social_Media_Analyzer:YXAtc291dGgtMS5hd3MuZWxhc3RpYy1jbG91ZC5jb20kMzZlZGRhZGZkNDQwNDg1YjhiZjk0ZjE4ZGU0M2Y5OTUkN2U3ZmY0MjM0NWVkNDVhNWI5ZjU3N2QxYjk2MTZmYmM=:9243"
    #es_client = Elasticsearch(cloud_id=CLOUD_ID,basic_auth=("RamDarapureddy", ELASTIC_PASSWORD))
		#es_client = Elasticsearch([{'host': 'localhost', 'port': 9200,'http_auth' : ("Shashankn7261", "Shashank@2002")}])
		es_client = Elasticsearch(cloud_id="Social_Media_Analyzer:YXAtc291dGgtMS5hd3MuZWxhc3RpYy1jbG91ZC5jb20kMzZlZGRhZGZkNDQwNDg1YjhiZjk0ZjE4ZGU0M2Y5OTUkN2U3ZmY0MjM0NWVkNDVhNWI5ZjU3N2QxYjk2MTZmYmM=:9200",basic_auth=("Shashankn7261", ELASTIC_PASSWORD))
    #es_client = Elasticsearch(['https://social-media-analyzer.kb.ap-south-1.aws.elastic-cloud.com:9200'],http_auth=("Shashankn7261","Shashank@2002"),ca_certs="super.cer", timeout = 600)
  	#es_client = Elasticsearch([{'host': 'https://social-media-analyzer.kb.ap-south-1.aws.elastic-cloud.com', 'port': 9243, 'scheme':"https", 'http_auth' : ("Shashankn7261", "Shashank@2002")}])
		#es_client = Elasticsearch([{'host':'https://social-media-analyzer.kb.ap-south-1.aws.elastic-cloud.com','http_auth':('Shashankn7261', 'Shashank@2002'),'scheme':'https','port':443,'http_compress':True}])
		def doc_generator(df):
			df_iter = df.iterrows()
			for index, document in df_iter:
				yield {
						"_index": 'topic_agnipath',
						"_type": "_doc",
						"_id" : f"{document['id']}",
						"_source": self.filterKeys(document),
					}
		#     raise StopIteration
		#helpers.bulk(es_client, doc_generator(df))
		#index.bulk(es_client,doc_generator(df))
    

		
		
