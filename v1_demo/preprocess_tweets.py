import os
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from wordcloud import WordCloud, STOPWORDS
import nltk
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from deep_translator import GoogleTranslator
import ast
from textblob import TextBlob
import tqdm
import ast

class preprocess:
	def __init__(self,query,since,till,event):
		self.query = query
		self.since = since
		self.till = till
		self.event = event
		
	def remove_pattern(self,input_txt, pattern):
		r = re.findall(pattern, input_txt)
		for i in r:
			input_txt = re.sub(i, '', input_txt)        
		return input_txt
	
	def genCleanText(self):
		cleantext = []
		for text in self.tweets_df['renderedContent']:
			cleantext.append(self.cleanText(text))
		return cleantext
	
	def sentiment_scores(self,sentence):
		# Create a SentimentIntensityAnalyzer object.
		sid_obj = SentimentIntensityAnalyzer()

		# polarity_scores method of SentimentIntensityAnalyzer
		# object gives a sentiment dictionary.
		# which contains pos, neg, neu, and compound scores.
		sentiment_dict = sid_obj.polarity_scores(sentence)
		# decide sentiment as positive, negative and neutral
		return sentiment_dict['compound']
	
	def genVaderSentiment(self):
		scores = []
		for text in self.tweets_df['translatedText']:
			scores.append(self.sentiment_scores(text))
		return scores
	
	def subjectivity_scores(self,sentence):
		score = TextBlob(sentence)
		return score.sentiment.subjectivity
	
	def genSubjectivity(self):
		scores = []
		for text in self.tweets_df['translatedText']:
			scores.append(self.subjectivity_scores(text))
		return scores
	
	def generateUsernames(self):
		usernames = []
		for user in self.tweets_df.user:
			usernames.append(user['username'])
		return usernames

	def genTranslatedText(self):
		translatedText = []
		for text in tqdm.tqdm(self.tweets_df['cleanText']):
			translatedText.append(GoogleTranslator(source='auto', target='en').translate(text))
		return translatedText

	def cleanText(self,tweets):
		#remove twitter Return handles (RT @xxx:)
		tweets = np.vectorize(self.remove_pattern)(tweets, "RT @[\w]*:") 

		#remove twitter handles (@xxx)
		tweets = np.vectorize(self.remove_pattern)(tweets, "@[\w]*")

		#remove URL links (httpxxx)
		tweets = np.vectorize(self.remove_pattern)(tweets, "https?://[A-Za-z0-9./]*")

		#remove special characters, numbers, punctuations (except for #)
		tweets = np.core.defchararray.replace(tweets, "[^a-zA-Z]", " ")



		return np.array2string(tweets)[1:-2]

	
	def add_user_fields(self):
		user_cols = [f"user_{x}" for x in list(self.tweets_df.user.to_list()[0].keys())[1:]]
		for u in user_cols:
			self.tweets_df[u] = self.tweets_df['user'].apply(lambda x : x[u[5:]])
			
	def get_mentions(self,tweet):
		return re.findall("@([a-zA-Z0-9_]{1,50})", tweet)
			
	def get_hashtags(self,tweet):
		return re.findall("#([a-zA-Z0-9_]{1,50})", tweet)
		
	def safe_date(safe,date_value):
		return (
			pd.to_datetime(date_value) if not pd.isna(date_value)
				else  datetime(1970,1,1,0,0)
		)
	
	def preprocess(self):
		self.tweets_df = pd.read_json(f'./data/{self.event}/jsons/{self.since}_{self.till}.json', lines=True)
		
		self.tweets_df['cleanText'] = self.genCleanText()
		self.tweets_df['translatedText'] = self.genTranslatedText()
		self.tweets_df['vaderSentiment'] = self.genVaderSentiment()
		self.tweets_df['subjectivity'] = self.genSubjectivity()
		self.tweets_df['usernames'] =self.generateUsernames()
		self.tweets_df['mentions'] = self.tweets_df['content'].apply(self.get_mentions)
		self.tweets_df['hashtags'] = self.tweets_df['content'].apply(self.get_hashtags)
		self.tweets_df['source'] = self.tweets_df['source'].apply(lambda s : s[s.find('>')+1:].replace('</a>',''))
		self.tweets_df['event'] = self.event
		self.tweets_df['es_date'] = self.tweets_df['date'].apply(self.safe_date)
		
		
		self.add_user_fields()
		
		try:
			self.tweets_df.to_csv(f'./data/{self.event}/csv/{self.since}_{self.till}.csv')
		except:
			print("error while creating csv")
			
			
		
		
		
		
