from pymongo import MongoClient
import pandas as pd

# CONNECTION SETTINGS
MONGO_URI = "mongodb://admin:password@mongo:27017/"
DB_NAME = "analytics_db"
COLLECTION_NAME = "comments"


def get_database():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]


def print_section(title):
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)


# QUERY 1: Top 5 Frequent Comments
def show_top_comments(collection):
    print_section("TOP 5 MOST FREQUENT COMMENTS")
    pipeline = [
        {"$group": {"_id": "$content", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]

    results = list(collection.aggregate(pipeline))

    if not results:
        print("No data found.")
    else:
        print(f"{'COUNT':<10} | {'COMMENT TEXT'}")
        print("-" * 60)
        for row in results:
            print(f"{row['count']:<10} | {row['_id']}")


# QUERY 2: Comments Shorter Than 5 Characters
def show_short_comments(collection):
    print_section("SHORT COMMENTS (< 5 chars)")
    pipeline = [
        {"$project": {"content": 1, "length": {"$strLenCP": "$content"}}},
        {"$match": {"length": {"$lt": 5}}},
        {"$limit": 10}
    ]

    results = list(collection.aggregate(pipeline))

    if not results:
        print("No short comments found.")
    else:
        print(f"{'LENGTH':<10} | {'CONTENT'}")
        print("-" * 60)
        for row in results:
            print(f"{row['length']:<10} | {row['content']}")


# QUERY 3: Average Rating by Day
def show_avg_rating(collection):
    print_section("AVERAGE RATING BY DATE")
    pipeline = [
        {"$group": {"_id": "$created_date", "avg_score": {"$avg": "$score"}}},
        {"$sort": {"_id": 1}},
        {"$limit": 10}
    ]

    results = list(collection.aggregate(pipeline))

    if not results:
        print("No data found.")
    else:
        print(f"{'DATE':<25} | {'AVG SCORE'}")
        print("-" * 60)
        for row in results:
            # Safely handle None values to prevent crashes
            val = row.get('avg_score')
            if val is None:
                score = 0.0
            else:
                score = round(val, 2)

            # Clean up the date string for display
            date_str = str(row['_id']).replace(" 00:00:00", "")
            print(f"{date_str:<25} | {score}")


# MAIN EXECUTION
if __name__ == "__main__":
    try:
        db = get_database()
        col = db[COLLECTION_NAME]

        show_top_comments(col)
        show_short_comments(col)
        show_avg_rating(col)

        print("\n\n [SUCCESS] Analysis complete!")

    except Exception as e:
        print(f"\n [ERROR] Something went wrong: {e}")