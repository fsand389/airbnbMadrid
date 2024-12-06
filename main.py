from db_setup import connect_to_db, get_or_create_collection
from queries import query_1, query_2, query_3, query_4, query_5, query_6, query_7

def main():
    # Connect to MongoDB and get the collection
    database = connect_to_db()
    collection = get_or_create_collection(database)

    # Menu to run specific queries
    while True:
        print("\nSelect a Query to Run:")
        print("1. Display Three Documents")
        print("2. Display Ten Documents")
        print("3. Find Listings by Superhosts")
        print("4. Find Unique Host Names")
        print("5. Filter Listings with Beds and Ratings")
        print("6. Count the Number of Listings per Host")
        print("7. Average Review Scores by Neighborhood")
        print("0. Exit")
        
        try:
            choice = int(input("\nEnter the query number (0 to exit): "))
            if choice == 0:
                print("Exiting...")
                break
            elif choice == 1:
                query_1(collection)
            elif choice == 2:
                query_2(collection)
            elif choice == 3:
                query_3(collection)
            elif choice == 4:
                query_4(collection)
            elif choice == 5:
                query_5(collection)
            elif choice == 6:
                query_6(collection)
            elif choice == 7:
                query_7(collection)
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")

if __name__ == "__main__":
    main()
