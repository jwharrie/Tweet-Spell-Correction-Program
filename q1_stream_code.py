from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import re

# Keys
ACCESS_TOKEN = '1271150897687457792-hpFrUxPPne8Psj7uT66Kxy1DH5JEX9'
ACCESS_TOKEN_SECRET = 'PzmP6FyCYE6Ym1ZCdYffLYNmMjhFyRKKQhQoObWe3wXv1'
CONSUMER_KEY = '7meXVgRbKYrHaJKVbguf1lpCZ'
CONSUMER_SECRET = 'KKGQS2bbmkuYCLB1ph6kWDxfVV6tWUHQIrk9kcZ3wNdnyAuaL6'

# Write name of text file or csv file you wish to write tweets to.
TEXTFILE = 'd1.txt'

tracklist = ['COVID-19']	# For filtering queries for D2
langlist = ['en']			# Used to help filter for English tweets.

# Tracks current amount of tweets successfully extracted.
tweet_count = 0

# Sets max amount of tweets to 1000
N_TWEETS = 1000
# Creates or clears TEXTFILE
f = open(TEXTFILE, 'w')
f.close()

# helps to stream
class StdOutListener(StreamListener):

	def on_data(self, data):
		global tweet_count
		global n_tweets
		global stream

		if tweet_count < N_TWEETS:

			try:
				print(tweet_count, data, "\n")
				tweet_data = json.loads(data)

				if 'retweeted_status' not in tweet_data and 'quoted_status' not in tweet_data:
					pattern1 = re.compile(r'\n')

					if tweet_data['truncated'] == False:
						tweet_txt = pattern1.sub(r'', tweet_data['text'])
					else:
						tweet_txt = pattern1.sub(r'', tweet_data['extended_tweet']['full_text'])

					pattern2 = re.compile(r'RT')
					tweet = pattern2.sub(r'', tweet_txt)
					f = open(TEXTFILE, 'a+')
					f.write(str(tweet_data['id']) + '\t' + tweet + '\n')
					tweet_count += 1
			except BaseException:
				print('Error:', tweet_count, data)

			return True
		else:
			stream.disconnect()

	def on_error(self, status):
		print(status)

L = StdOutListener()
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
stream = Stream(auth, L)

# Uncomment line below to extract tweets for D1
stream.sample(languages=langlist)

# Uncomment line below to extract tweets for D2
#stream.filter(track=tracklist, languages=langlist)
