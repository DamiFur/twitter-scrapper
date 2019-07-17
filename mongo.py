import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["tweets"]

def setup(collection):
    db[collection].create_index(
        [('full_text', pymongo.TEXT)],
        default_language='spanish')

def setup_users():
    db.users.create_index("id", unique=True, dropDups=True)

def store(tweet, collection):
    db[collection].insert_one(tweet)
