import tweepy
import mongo 
import datetime as dt
import time
from tweepyrate import fetch_tweets, create_apps




ACCESS_TOKEN = '1132007433864396800-ejwacZCKOkHrwdlkAnoetGydIjAFyn'
ACCESS_SECRET = 'GZbnz4j2PieilPn8dwP9urmEbnI4MxfhJk4QdzZ54f9ET'
CONSUMER_KEY = 'VbCl8NHlJoJa7GhdEqqONLcRH'
CONSUMER_SECRET = '2rNyTkqQU3kiKV5et3IN8YTQSUBlYfM8eYAZK8WRlD7TEw3oWW'


# Setup access to API
def connect_to_twitter_OAuth():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    api = tweepy.API(auth)
    return api


# fuction to extract data from tweet object
def store_with_attributes(tweet_object, collection):
    # loop through tweet objects
    for tweet in tweet_object:
        tweet_id = tweet.id # unique integer identifier for tweet
        try:
            text = tweet.full_text
        except AttributeError:
            text = tweet.text
        favorite_count = tweet.favorite_count
        retweet_count = tweet.retweet_count
        created_at = tweet.created_at # utc time tweet created
        reply_to_status = tweet.in_reply_to_status_id # if reply int of orginal tweet id
        reply_to_user = tweet.in_reply_to_screen_name # if reply original tweetes screenname
        retweets = tweet.retweet_count # number of times this tweet retweeted
        favorites = tweet.favorite_count # number of time this tweet liked
        user = tweet._json
        
        try:
            # append attributes to list
            mongo.store({'tweet_id':tweet_id, 
                              'text':text, 
                              'favorite_count':favorite_count,
                              'retweet_count':retweet_count,
                              'created_at':created_at, 
                              'reply_to_status':reply_to_status, 
                              'reply_to_user':reply_to_user,
                              'retweets':retweets,
                              'favorites':favorites,
                              'user':user['id']}, collection)
        except Exception:
            continue
        try:
            mongo.store(user, 'users')
        except Exception as e:
            continue

# Create API object
api = connect_to_twitter_OAuth()

#Setup Users
mongo.setup_users()

query = 'FMI'
max_tweets = 1000
#argentina = api.geo_search(query="Argentina", granularity="country")
#argentina_id = argentina[0].id

# Setup Collection
mongo.setup(query)

while True:
    try:
        searched_tweets = [status for status in tweepy.Cursor(api.search, q=query + ' -filter:retweets', tweet_mode='extended').items(max_tweets)]
        store_with_attributes(searched_tweets, query)
    except tweepy.TweepError:
        print('exception raised, waiting 15 minutes to continue')
        print('(until:', dt.datetime.now()+dt.timedelta(minutes=15), ')')
        time.sleep(15*60)
