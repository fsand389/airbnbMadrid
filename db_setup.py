import pymongo

def connect_to_db():
    database = pymongo.MongoClient("mongodb://localhost:27017/")
    return database

def get_or_create_collection(database, db_name="TheDatabase", collection_name="listings"):
    # Check if database exists
    if db_name not in database.list_database_names():
        print(f"Creating database '{db_name}'...")
        db = database[db_name]
        return db[collection_name]
    else:
        db = database[db_name]
        return db[collection_name]
