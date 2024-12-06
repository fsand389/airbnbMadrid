import pymongo
import csv
import pprint



#Connect to Local MongoDB Server
database = pymongo.MongoClient("mongodb://localhost:27017/")

#creating variable with list of existing database names
dbList = database.list_database_names()

#Check if database already Exists 
if "TheDatabase" not in dbList:
    print("Creating new Database")
    mydb = database["TheDatabase"]
    print(database.list_database_names())

    #put csv info into a data dict 
    with open("listings.csv",'r', encoding='utf-8') as file:
        csv_read = csv.DictReader(file)
        data = [row for row in csv_read]

    #put data dict info into mongodb collection
    collection = mydb["listings"]
    if "listings" in mydb.list_collection_names():
        print("Collection already exists")
    collection.insert_many(data)
    
    
else:
    print("Database already exists")
    mydb = database["TheDatabase"]
    collection = mydb["listings"]
    
