import os 
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from wordcloud import WordCloud, STOPWORDS
import nltk
#from nltk.sentiment.vader import SentimentIntensityAnalyzer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from deep_translator import GoogleTranslator
import ast
from textblob import TextBlob
import argparse
import datetime
from collect_tweets import snscrape 
from preprocess_tweets import preprocess
# from preprocess_tweets_Modified import preprocess
from push_tweets import dataFeeder
import time
class event:
	
	def eventExists(self,event):
		if event in os.listdir(f'./data/'):
			return True
		return False
	def createFolder(Self,event):
		try:
			os.system(f"mkdir ./data/{event}")
		except:
			print("data folder does not exist")
			
		os.system(f"mkdir ./data/{event}/jsons")
		os.system(f"mkdir ./data/{event}/csv")
		
			
		
	def __init__(self,event):
		self.event = event
		if self.eventExists(event) == False:
			print("Creating new event ...")
			self.createFolder(event)
		
	def update_status(self,file,status_code):
		status = pd.read_csv("status.csv")
		status = status[['file','status']]
		status = status[status['file']!=file]
		status = status.append({"file":file,"status":status_code},ignore_index=True)
		status.to_csv('status.csv')

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Process some integers.')
	parser.add_argument('--query', help='query for tweet collection',required = True)
	parser.add_argument('--since', help='starting date for tweet collection', required = True)
	parser.add_argument('--until', help='starting date for tweet collection', required = True)
	parser.add_argument('--event', help='event for which data is being collected', required = True)
	
	args = vars(parser.parse_args())


	query = str(args['query'])
	since = str(args['since'])
	until = str(args["until"])
	till = str(time.time()).replace('.',"-")
	
	print("-----------------------------------")
	print(f"Query : {query}\nSince : {since}\nUntil : {until}")
	print("-----------------------------------")
	
	
	curr_event = str(args['event'])
	
	eventObj = event(curr_event)
	
	tweetCollector  = snscrape(query,since,until,till,curr_event)
	preprocessor  = preprocess(query,since,until,till,curr_event)
	feeder   =  dataFeeder(query,since,until,till,curr_event)
	
	
	start_collect=time.time()
	tweetCollector.collect_tweets()
	end_collect=time.time()
	print('Time taken to collect tweet are ', end_collect-start_collect) 
  
	eventObj.update_status(f"{curr_event}_{since}_{till}","collected")
	
	print("-----------------------------------")
	print("Preprocessing...")
	start_preprocess=time.time()
	preprocessor.preprocess()
	end_preprocess=time.time()
	print('Time taken to preprocess tweets are ', end_preprocess-start_preprocess)
	eventObj.update_status(f"{curr_event}_{since}_{till}","preprocessed")
	print("-----------------------------------")
	print("Pushing Data")
	
	feeder.push_data()
	eventObj.update_status(f"{curr_event}_{since}_{till}","pushed")
	
	print("Succesful")
	
	
	
	
	
	

	