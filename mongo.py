import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["tweets"]

def store(tweet, collection):
    db[collection].insert_one(tweet)
