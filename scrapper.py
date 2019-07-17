import tweepy
import mongo 
import datetime as dt
import time

ACCESS_TOKEN = '1132007433864396800-2CKMMkc9qGv7TE7RaE1JeG4q0xFydA'
ACCESS_SECRET = 'ARou1yzchCVj75ixPMEaVgrf4oBXZAZRbEV8MQui0N82t'
CONSUMER_KEY = '1xWh5vv02vWhEUbDsRcEK8u2H'
CONSUMER_SECRET = 'HWDtbRreyL8F1m9r6Uj406qTWWcK3Y9VcfDv9qMMFrEIdW4Ni8'

# Setup access to API
def connect_to_twitter_OAuth():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    api = tweepy.API(auth)
    return api


# Create API object
api = connect_to_twitter_OAuth()

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
        # append attributes to list
        mongo.store({'tweet_id':tweet_id, 
                          'text':text, 
                          'favorite_count':favorite_count,
                          'retweet_count':retweet_count,
                          'created_at':created_at, 
                          'reply_to_status':reply_to_status, 
                          'reply_to_user':reply_to_user,
                          'retweets':retweets,
                          'favorites':favorites
                          'user':user['id']}, collection)
        mongo.store(user, 'users')

query = 'FMI'
max_tweets = 10
argentina = api.geo_search(query="Argentina", granularity="country")
argentina_id = argentina[0].id
while True:
    try:
        searched_tweets = [status for status in tweepy.Cursor(api.search, q=query + ' -filter:retweets' + ' place:' + argentina_id, tweet_mode='extended').items(max_tweets)]
        store_with_attributes(searched_tweets, query + 'test')
    except tweepy.TweepError:
        print('exception raised, waiting 15 minutes to continue')
        print('(until:', dt.datetime.now()+dt.timedelta(minutes=15), ')')
        time.sleep(15*60)
