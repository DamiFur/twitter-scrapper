import pymongo

class Database:
    def __init__(self, dbname):
        # TODO: Add Try/Except
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client[dbname]

    def setup_tweets(collection):
        try:
            self.db[collection].create_index("tweet.id", unique=True)
        except Exception as e:
            print("Exception creating index: {}".format(str(e)))

    def setup_user():
        try:
            self.db["users"].create_index("id", unique=True)
        except Exception as e:
            print("Exception creating index: {}".format(str(e)))

    def get_last_tweet_id(query, collection):
        tweet = self.db[collection].find_one({"query": query},sort=[("tweet.id", pymongo.DESCENDING)])
        return tweet

    def get_first_tweet_id(query, collection):
        tweet = self.db[collection].find_one({"query": query},sort=[("tweet.id", pymongo.ASCENDING)])
        return tweet

    def setup_users():
        self.db.users.create_index("id", unique=True, dropDups=True)

    def store(tweet, collection):
        self.db[collection].insert_one(tweet)

    def check_if_exists(query, collection):
        if self.db[collection].count(query) > 0:
            return True
        return False


    def removeDuplicates(collection):
        self.db[collection + "_unique"].create_index("tweet.id", unique=True)
        removed = 0
        for tweet in self.db[collection].find():
            try:
                if not check_if_exists({"tweet.id": tweet['tweet']['id']}, collection + "_unique"):
                    store(tweet, collection + "_unique")
                else:
                    removed += 1
            except Exception as e:
                print(e)
                print(tweet)
                continue
        print("Removed " + str(removed) + " tweets")


    def get_tweet_ids(collection):
        tweets = self.db[collection].find()
        tweet_ids = []
        for tweet in tweets:
            tweet_ids.append(tweet['tweet']['id'])

        print(tweet_ids)
