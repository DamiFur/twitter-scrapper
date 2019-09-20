import tweepy
import mongo 
import time
import json
from tweepyrate import tweepyrate
import fire




class UserCollector:
    def __init__(self, isPositive, query):
        self.position = "positive" if isPositive else "negative"
        self.query = query

    def collect_users(self):
        with open("config/{}_users_{}.json".format(self.query, self.position)) as f:
            return json.load(f)

# fuction to extract data from tweet object
def store_with_attributes(tweet_object, query, collection):
    #TODO: Cambiar Collection por Query + Suffix y agregar que chequee que la query esté presente en lo que quiere guardar
    # loop through tweet objects
    stored = 0
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
        user = tweet.user._json
        try:
            if (not mongo.check_if_exists({"text": text}, collection)):
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
                                  'user':user['id'],
                                  'user_name':user['screen_name']}, collection)
                stored += 1
        except Exception as e:
            print("Exception storing in mongo: {}".format(str(e)))
            continue
        try:
            if not mongo.check_if_exists({"id": user["id"]}, 'users'):
                mongo.store(user, 'users')
        except Exception as e:
            print(e)
            continue
    print("Stored {} tweets for query {} in collection {}. {} were excluded".format(str(stored), query, collection, str(len(tweet_object) - stored)))

def get_query_args(collection, query, mode, ignore_ids):
    args = {
        "q": query,
        "mode": mode,
    }
    if ignore_ids:
        return args
    elif mode == "new":
        # Search for last tweet of this country, so we look for tweets
        # newer than it
        tweet = collection.find_one(
            {},
            sort=[("tweet_id", pymongo.DESCENDING)])

        if tweet:
            args["since_id"] = tweet["id"]
    elif mode == "past":
        # Search for first tweet of this country, so we look tweets older than
        # it
        tweet = collection.find_one(
            {},
            sort=[("tweet_id", pymongo.ASCENDING)])

        if tweet:
            args["max_id"] = tweet["id"]
            print("Tweet with max_id {} found".format(args["max_id"]))
        else:
            print("No tweet found with this search")
    else:
        raise ValueError("mode should be new or past")

    return args



def collect_with_query_and_users(keyword, queries=[], with_users=False, mode="all"):
    apps = tweepyrate.create_apps("config/my_apps.json")
    fetcher = tweepyrate.collector.Fetcher(apps[:len(apps)-2], 10, store_with_attributes, 1000)
    fetcher_users = tweepyrate.collector.Fetcher(apps[len(apps)-2:], 10, store_with_attributes, 1000)

    # setup de mongo

    mongo.setup(keyword)
    if with_users:
        positive_users = UserCollector(True, keyword).collect_users()
        negative_users = UserCollector(False, keyword).collect_users()
        mongo.setup(keyword + "-Positive")
        mongo.setup(keyword + "-Negative")

    for query in queries[1:len(queries)-1].split(","):

        args_for_all = get_query_args(query, query, "all", True)
        collector_all = tweepyrate.collector.Collector(store_with_attributes, keyword, fetcher, 60, **args_for_all)
        collector_all_past = tweepyrate.collector.PastTweetsCollector(store_with_attributes, keyword, fetcher, 10, **args_for_all)
        collector_all_new = tweepyrate.collector.NewTweetsCollector(store_with_attributes, keyword, fetcher, 10, **args_for_all)
        
        collector_all.start()
        collector_all_new.start()
        collector_all_past.start()

    if with_users:
            
        args_for_positive = get_query_args(query + "-Positive", query, "all", True)
        collector_positive = tweepyrate.collector.ByUsersCollector(store_with_attributes, keyword, fetcher_users, 30, True, positive_users, None, "all", **args_for_positive)
        collector_positive_new = tweepyrate.collector.ByUsersCollector(store_with_attributes, keyword, fetcher_users, 10, True, positive_users, mongo.get_last_tweet_id(query + "-Positive"), "new", **args_for_positive)
        collector_positive_past = tweepyrate.collector.ByUsersCollector(store_with_attributes, keyword, fetcher_users, 10, True, positive_users, mongo.get_first_tweet_id(query + "-Positive"), "past", **args_for_positive)

        args_for_negative = get_query_args(query + "-Negative", query, "all", True)
        collector_negative = tweepyrate.collector.ByUsersCollector(store_with_attributes, keyword, fetcher_users, 30, False, negative_users, None, "all", **args_for_negative)
        collector_negative_new = tweepyrate.collector.ByUsersCollector(store_with_attributes, keyword, fetcher_users, 10, True, negative_users, mongo.get_last_tweet_id(query + "-Negative"), "new", **args_for_positive)
        collector_negative_past = tweepyrate.collector.ByUsersCollector(store_with_attributes, keyword, fetcher_users, 10, True, negative_users, mongo.get_first_tweet_id(query + "-Negative"), "past", **args_for_positive)

    if with_users:
        collector_positive.start()
        collector_positive_new.start()
        collector_positive_past.start()

        collector_negative.start()
        collector_negative_new.start()
        collector_negative_past.start()



if __name__ == '__main__':
    fire.Fire(collect_with_query_and_users)
