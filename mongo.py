import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["tweets"]

def setup(collection):
    try:
        db[collection].create_index(
            [('text', pymongo.TEXT)],
            default_language='spanish')
    except:
        print("index already created")

def setup_users():
    db.users.create_index("id", unique=True, dropDups=True)

def store(tweet, collection):
    db[collection].insert_one(tweet)

def check_if_exists(text, collection):
    if db[collection].count({"text": text}) > 0:
        return True
    return False

def removeDuplicates(collection):
    db[collection + "_unique"].create_index(
        [('text', pymongo.TEXT)],
        default_language='spanish')
    removed = 0
    for tweet in db[collection].find():
        try:
            if not check_if_exists(tweet['text'], collection + "_unique"):
                store(tweet, collection + "_unique")
            else:
                removed += 1
        except Exception as e:
            print(e)
            print(tweet)
            continue
    print("Removed " + str(removed) + " tweets")
