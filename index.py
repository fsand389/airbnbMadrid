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


# Query 2: Display exactly ten documents with cleaner formatting
print("\nQuery 2: Displaying ten documents from the listings collection\n")
query2 = collection.find({}, {"_id": 0, "name": 1, "beds": 1, "neighbourhood_group_cleansed": 1}).limit(10)

for i, document in enumerate(query2, start=1):
    print(f"Document {i}:")
    print(f"  Name: {document.get('name', 'N/A')}")
    print(f"  Beds: {document.get('beds', 'N/A')}")
    print(f"  Neighbourhood: {document.get('neighbourhood_group_cleansed', 'N/A')}")
    print("\n" + "-" * 40)  # Add a separator for clarity



# Query 3: Find Listings by Superhosts
print("\nQuery 3: Listings by Superhosts\n")

# Step 1: Find two superhost IDs
query3 = collection.find({'host_is_superhost': 't'}, {'_id': 0, 'host_id': 1}).limit(2)
superhost_ids = [host['host_id'] for host in query3]

# Step 2: Fetch listings for each superhost
for i, host_id in enumerate(superhost_ids, start=1):
    print(f"\nSuperhost {i} (Host ID: {host_id}):\n")
    listings = collection.find({'host_id': host_id}, {"_id": 0, "name": 1, "beds": 1, "neighbourhood_group_cleansed": 1, "review_scores_rating": 1})
    
    for j, listing in enumerate(listings, start=1):
        print(f"  Listing {j}:")
        print(f"    Name: {listing.get('name', 'N/A')}")
        print(f"    Beds: {listing.get('beds', 'N/A')}")
        print(f"    Neighbourhood: {listing.get('neighbourhood_group_cleansed', 'N/A')}")
        print(f"    Review Score: {listing.get('review_scores_rating', 'N/A')}")
    print("\n" + "-" * 50)  # Separator for clarity


# Query 4: Find and display unique host names
print("\nQuery 4: Unique Host Names\n")

# Retrieve all unique host names
query4 = collection.distinct("host_name")

# Group and display the unique host names
for i, host_name in enumerate(query4, start=1):
    print(f"Host {i}: {host_name}")
    if i % 10 == 0:  # Add a separator after every 10 host names for better readability
        print("\n" + "-" * 50)


# Query 5: Find listings with more than 2 beds in a specific neighborhood
print("\nQuery 5: Listings with more than 2 beds in a specific neighborhood\n")

# Define the neighborhood of interest
neighbourhood = "Chamartín"  # Replace with desired neighborhood

# Retrieve listings with >2 beds in the specified neighborhood, sorted by review score
query5 = collection.find(
    {
        "beds": {"$gt": 2},
        "neighbourhood_group_cleansed": neighbourhood
    },
    {"_id": 0, "name": 1, "beds": 1, "review_scores_rating": 1}
).sort("review_scores_rating", -1)

# Display the results
print(f"Listings with more than 2 beds in '{neighbourhood}', sorted by review_scores_rating:\n")
for i, listing in enumerate(query5, start=1):
    print(f"Listing {i}:")
    print(f"  Name: {listing.get('name', 'N/A')}")
    print(f"  Beds: {listing.get('beds', 'N/A')}")
    print(f"  Review Score: {listing.get('review_scores_rating', 'N/A')}")
    print("\n" + "-" * 50)



