import pandas as pd
import os
class snscrape:
	def __init__(self,query,since,till,event):
		self.query = query
		self.since = since
		self.event = event
		self.till = till
		
	def collect_tweets(self):
		
		try:
			print("Collecting ...")
			os.system(f'snscrape --jsonl --since {self.since} twitter-search "{self.query}" > ./data/{self.event}/jsons/{self.since}_{self.till}.json')
		except:
			print("error while scraping")
		print("Collection done")	  
		return f"{self.event}_{self.since}_{self.till}"
	
	
	
					  
		
					
					  
		