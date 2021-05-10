from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

import json
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from unidecode import unidecode
import time

analyzer = SentimentIntensityAnalyzer()

c_key = ""
c_secret = ""
a_token = ""
a_secret = ""

conn = sqlite3.connect("tweet.db")
c = conn.cursor()


def create_table():
    try:
        c.execute("CREATE TABLE IF NOT EXISTS tweet_sentiment(unix REAL, tweet TEXT, sentiment TEXT)")
        c.execute("CREATE INDEX fast_unix ON sentiment(unix)")
        c.execute("CREATE INDEX fast_tweet ON sentiment(tweet)")
        c.execute("CREATE INDEX fast_sentiment ON sentiment(sentiment)")

        conn.commit()

    except Exception as e:
        print(str(e))


create_table()

class TweetStreamer(StreamListener):

    def on_data(self, data):
        try:
            data = json.loads(data)
            tweet = unidecode(data['text'])
            time_ms = data['timestamp_ms']
            vs = analyzer.polarity_scores(tweet)
            sentiment = vs['compound']
            print(time_ms, tweet, vs)
            c.execute("INSERT INTO tweet_sentiment (unix, tweet, sentiment) VALUES (?,?,?)", (time_ms, tweet, sentiment))
            conn.commit()

        except KeyError as e:
            print(str(e))
        return True

    def on_error(self, status):
        print(status)

while True:

    try:
        auth = OAuthHandler(c_key, c_secret)
        auth.set_access_token(a_token, a_secret)
        twitterStream = Stream(auth, TweetStreamer())
        twitterStream.filter(track=["covaxin", "covishield"])

    except Exception as e:
        print(str(e))
        time.sleep(5)
