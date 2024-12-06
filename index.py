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
    


#queries work 

# Query 1: Display exactly three documents with cleaner formatting
print("\nQuery 1: Displaying three documents from the listings collection\n")
query1 = collection.find({}, {"_id": 0, "name": 1, "beds": 1, "neighbourhood_group_cleansed": 1}).limit(3)

for i, document in enumerate(query1, start=1):
    print(f"Document {i}:")
    print(f"  Name: {document.get('name', 'N/A')}")
    print(f"  Beds: {document.get('beds', 'N/A')}")
    print(f"  Neighbourhood: {document.get('neighbourhood_group_cleansed', 'N/A')}")
    print("\n" + "-" * 40)  # Add a separator for clarity





