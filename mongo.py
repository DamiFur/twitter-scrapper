import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["elecciones-2019"]

def setup_tweets(collection):
    try:
        db[collection].create_index("tweet.id", unique=True)
    except Exception as e:
        print("Exception creating index: {}".format(str(e)))

def setup_user():
    try:
        db["users"].create_index("id", unique=True)
    except Exception as e:
        print("Exception creating index: {}".format(str(e)))

def get_last_tweet_id(query, collection):
    tweet = db[collection].find_one({"query": query},sort=[("tweet.id", pymongo.DESCENDING)])
    return tweet

def get_first_tweet_id(query, collection):
    tweet = db[collection].find_one({"query": query},sort=[("tweet.id", pymongo.ASCENDING)])
    return tweet

def setup_users():
    db.users.create_index("id", unique=True, dropDups=True)

def store(tweet, collection):
    db[collection].insert_one(tweet)

def check_if_exists(query, collection):
    if db[collection].count(query) > 0:
        return True
    return False


def removeDuplicates(collection):
    db[collection + "_unique"].create_index("tweet.id", unique=True)
    removed = 0
    for tweet in db[collection].find():
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
