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


def store_with_attributes_on_db(db):

    # fuction to extract data from tweet object
    def store_with_attributes(tweet_object, query, collection, no_need_to_check=False):
        #TODO: Cambiar Collection por Query + Suffix y agregar que chequee que la query esté presente en lo que quiere guardar
        # loop through tweet objects
        stored = 0
        for tweet in tweet_object:
            user = tweet.user._json
            tweet_json = tweet._json
            try:
                # append attributes to list
                db.store({'tweet':tweet_json,
                                  'user':user['id'],
                                  'query':query}, collection)
                stored += 1
            except Exception as e:
                #print("Exception storing in mongo: {}".format(str(e)))
                continue
            try:
                db.store(user, 'users')
                if tweet_json["retweeted"]:
                    db.store(tweet_json["retweeted_status"]["user"], 'users')
            except Exception as e:
                #print("Excepción guardando usuario: {}".format(e))
                continue
        if query != "streaming":
            print("Stored {} tweets for query {} in collection {}. {} were excluded".format(str(stored), query, collection, str(len(tweet_object) - stored)))

    return store_with_attributes


def get_query_args(collection, query, ignore_ids):
    args = {
        "q": query,
    }
    if ignore_ids:
        return args
    else:
        # Search for last tweet of this country, so we look for tweets
        # newer than it
        try:
            tweet = mongo.get_last_tweet_id(query, collection)
        except Exception as e:
            print("EXCEPTION {}".format(str(e)))
        if tweet:
            args["since_id"] = tweet["tweet"]["id"]
            print("Tweet with min_id {} found".format(args["since_id"]))
        # Search for first tweet of this country, so we look tweets older than
        # it
        
        try:
            tweet = mongo.get_first_tweet_id(query, collection)
        except Exception as e:
            print("EXCEPTION {}".format(str(e)))
        if tweet:
            args["max_id"] = tweet["tweet"]["id"]
            print("Tweet with max_id {} found".format(args["max_id"]))
        else:
            print("No tweet found with this search")

    return args

def collect_queries(keyword):
    with open("config/{}_queries.json".format(keyword)) as f:
       return json.load(f)

def collect_with_query_and_users(keywords=[], with_users=False, mode="all", apps_suffix="", database=""):
    db = mongo.Database(database)

    apps = tweepyrate.create_apps("config/my_apps{}.json".format(apps_suffix))
    fetcher = tweepyrate.collector.Fetcher(apps[1:], apps[:1], 10, store_with_attributes(db), 5000)


    for keyword in keywords:

        queries = collect_queries(keyword)
        print("Queries for keyword {}: {}".format(keyword, queries))
        # setup de mongo

        db.setup_tweets(keyword)
        db.setup_users()

        if with_users:
            positive_users = UserCollector(True, keyword).collect_users()
            negative_users = UserCollector(False, keyword).collect_users()
            db.setup(keyword + "-Positive")
            db.setup(keyword + "-Negative")

        for query in queries:

            args_for_time = get_query_args(keyword, query, False)
            args_for_all = get_query_args(keyword, query, True)
            collector_all = tweepyrate.collector.Collector(keyword, fetcher, 60, **args_for_all)
            collector_all_past = tweepyrate.collector.PastTweetsCollector(keyword, fetcher, 0, **args_for_time)
            collector_all_new = tweepyrate.collector.NewTweetsCollector(keyword, fetcher, 0, **args_for_time)
            
            #collector_all.start()
            collector_all_new.start()
            collector_all_past.start()

        if with_users:
                
            args_for_positive = get_query_args(keyword + "-Positive", "", True)
            collector_positive = tweepyrate.collector.ByUsersCollector(keyword, fetcher_users, 30, True, positive_users, None, "all", **args_for_positive)
            collector_positive_new = tweepyrate.collector.ByUsersCollector(keyword, fetcher_users, 10, True, positive_users, mongo.get_last_tweet_id(query + "-Positive"), "new", **args_for_positive)
            collector_positive_past = tweepyrate.collector.ByUsersCollector(keyword, fetcher_users, 10, True, positive_users, mongo.get_first_tweet_id(query + "-Positive"), "past", **args_for_positive)

            args_for_negative = get_query_args(keyword + "-Negative", "", True)
            collector_negative = tweepyrate.collector.ByUsersCollector(keyword, fetcher_users, 30, False, negative_users, None, "all", **args_for_negative)
            collector_negative_new = tweepyrate.collector.ByUsersCollector(keyword, fetcher_users, 10, True, negative_users, mongo.get_last_tweet_id(query + "-Negative"), "new", **args_for_positive)
            collector_negative_past = tweepyrate.collector.ByUsersCollector(keyword, fetcher_users, 10, True, negative_users, mongo.get_first_tweet_id(query + "-Negative"), "past", **args_for_positive)

            collector_positive.start()
            collector_positive_new.start()
            collector_positive_past.start()

            collector_negative.start()
            collector_negative_new.start()
            collector_negative_past.start()


        collector_stream = tweepyrate.collector.StreamingCollector(keyword, queries, fetcher, 15)
        collector_stream.start()

if __name__ == '__main__':
    fire.Fire(collect_with_query_and_users)
