# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tweepy
import time
          
def scrape(words,date_since,numtweet):
        # Creating DataFrame using pandas
        auth = tweepy.OAuthHandler("09Imum9N8uBSunln9mEExr9Dj","1za0MKHVCEcMTuhFAkalHn7mVY5PEeCUgrljXRYNvXZwBGxsv2")
        auth.set_access_token("1577915712899026947-7AooXjB4JjtECTNvFSgXBOTaXIM1GT","t4Y5VIwUsj3dfqINfEbBnnE11zUfTRpnfQeCFDLGBi5y0")
        api = tweepy.API(auth)

        db = pd.DataFrame(columns=['username','description','location','following','followers','totaltweets','retweetcount','text','hashtags'])

        tweets = tweepy.Cursor(api.search_tweets,words, lang="en",since_id=date_since,tweet_mode='extended').items(numtweet)

        list_tweets = [tweet for tweet in tweets]

        i = 1

        for tweet in list_tweets:
                username = tweet.user.screen_name
                description = tweet.user.description
                location = tweet.user.location
                following = tweet.user.friends_count
                followers = tweet.user.followers_count
                totaltweets = tweet.user.statuses_count
                retweetcount = tweet.retweet_count
                hashtags = tweet.entities['hashtags']
                
                try:
                        text = tweet.retweeted_status.full_text
                except AttributeError:
                        text = tweet.full_text
                hashtext = list()
                for j in range(0, len(hashtags)):
                        hashtext.append(hashtags[j]['text'])
                
                ith_tweet = [username, description, location, following, followers, totaltweets, retweetcount, text, hashtext]
                # tweet_dict.append({"Username" : username, "Description" : description, "Location" : location, "Following" : following, "Followers" : followers, "TotalTweets" : totaltweets, "Retweets" : retweetcount, "Hashtags" :  })
                db.loc[len(db)] = ith_tweet
                # printtweetdata(i, ith_tweet)
                i = i+1

        return db

if __name__ == '__main__':
#initialise keys here

        words = str(input("Enter the query:"))
        date_since = '2022-10-20'
        #date_until = '2022-09-27'
        numtweet = int(input("Enter the number of tweets:"))
        db = scrape(words, date_since,numtweet)
        db.to_csv('tweets.csv')
        db.to_json('tweets.json')
db.head()
print(date_since)
#print(date_until)
print("Successful")
# %%

'''def printtweetdata(n, ith_tweet):
        print()
        print(f"Tweet {n}:")
        print(f"Username:{ith_tweet[0]}")
        print(f"Description:{ith_tweet[1]}")
        print(f"Location:{ith_tweet[2]}")
        print(f"Following Count:{ith_tweet[3]}")
        print(f"Follower Count:{ith_tweet[4]}")
        print(f"Total Tweets:{ith_tweet[5]}")
        print(f"Retweet Count:{ith_tweet[6]}")
        print(f"Tweet Text:{ith_tweet[7]}")
        print(f"Hashtags Used:{ith_tweet[8]}")

# %%
class tweepy:
        def scrape(words, date_since, numtweet):
                # Creating DataFrame using pandas
                db = pd.DataFrame(columns=['username','description','location','following','followers','totaltweets','retweetcount','text','hashtags'])

                tweets = tweepy.Cursor(api.search_tweets,words, lang="en",since_id=date_since,tweet_mode='extended').items(numtweet)

                list_tweets = [tweet for tweet in tweets]

                i = 1

                for tweet in list_tweets:
                        username = tweet.user.screen_name
                        description = tweet.user.description
                        location = tweet.user.location
                        following = tweet.user.friends_count
                        followers = tweet.user.followers_count
                        totaltweets = tweet.user.statuses_count
                        retweetcount = tweet.retweet_count
                        hashtags = tweet.entities['hashtags']
                        
                        try:
                                text = tweet.retweeted_status.full_text
                        except AttributeError:
                                text = tweet.full_text
                        hashtext = list()
                        for j in range(0, len(hashtags)):
                                hashtext.append(hashtags[j]['text'])
                        
                        ith_tweet = [username, description,
                                location, following,
                                followers, totaltweets,
                                retweetcount, text, hashtext]
                        # tweet_dict.append({"Username" : username, "Description" : description, "Location" : location, "Following" : following, "Followers" : followers, "TotalTweets" : totaltweets, "Retweets" : retweetcount, "Hashtags" :  })
                        db.loc[len(db)] = ith_tweet
                        # printtweetdata(i, ith_tweet)
                        i = i+1

                return db
        
        if __name__ == '__main__':
        #initialise keys here

                auth = tweepy.OAuthHandler("xzySR3T3QpyBQW8hHVQyKqnDV","Ivg1uEAE2ys2sndVAEcM6tXoRSKQJs4X5cOFO0Nsa2rZ6fxrfw")
                auth.set_access_token("1379300535115784193-atWgLqAo1CJjBemZGZrqyCVA49B65y","Z0gLiS0esq4AaMnlh077RAUjw9wp8I8ARVU5WHcmokcS7")
                api = tweepy.API(auth)

                words = str(input())
                date_since = '2022-09-25'
                numtweet = int(input())
                db = scrape(words, date_since, numtweet)
                db.to_csv('tweets.csv')
                db.to_json('tweets.json')
        db.head()
        print("Successful")
    

# %%
if __name__ == '__main__':
        #initialise keys here

        auth = tweepy.OAuthHandler("xzySR3T3QpyBQW8hHVQyKqnDV","Ivg1uEAE2ys2sndVAEcM6tXoRSKQJs4X5cOFO0Nsa2rZ6fxrfw")
        auth.set_access_token("1379300535115784193-atWgLqAo1CJjBemZGZrqyCVA49B65y","Z0gLiS0esq4AaMnlh077RAUjw9wp8I8ARVU5WHcmokcS7")
        api = tweepy.API(auth)

        words = str(input())
        date_since = '2022-09-25'
        numtweet = int(input())
        db = tweepy(words, date_since, numtweet)
        db.to_csv('tweets.csv')
        db.to_json('tweets.json')

# %%
db.head()
print("Successful")
# %%
#unique_db = db.drop_duplicates(subset=['username'])
#location_db = unique_db.groupby(['location'])['location'].size().reset_index(name='counts')
#location_db['location'].replace('', np.nan, inplace=True)
#location_db.dropna(subset=['location'], inplace=True)

# %%
#location_db.plot(kind = 'bar',
#        x = 'location',
#        y = 'counts',
#        color = 'green')
  
# set the title
#plt.title('Unique users vs Location')
  
# show the plot
#plt.show()

# %%'''



