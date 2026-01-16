from db_connector import DatabaseConnector
from query_runner import QueryRunner
from config import DB_CONFIG
import json
from datetime import datetime


def print_results(results):
    """Function for formatted output of results"""
    print("\n" + "=" * 50)
    print("QUERY RESULTS")
    print("=" * 50)

    # 1. Number of students in rooms
    print("\n1. Number of students in rooms:")
    for room in results["task_1_room_student_count"]:
        print(f"  - {room['room_name']}: {room['student_count']} students")

    # 2. Top 5 rooms with the lowest average age
    print("\n2. Top 5 rooms with the lowest average age:")
    for i, room in enumerate(results["task_2_top5_min_avg_age"], 1):
        print(f"  {i}. {room['room_name']}: average age {room['avg_age']:.1f} years")

    # 3. Top 5 rooms with the biggest age difference
    print("\n3. Top 5 rooms with the biggest age difference:")
    for i, room in enumerate(results["task_3_top5_max_age_diff"], 1):
        print(f"  {i}. {room['room_name']}: age difference {room['age_difference']:.1f} years")

    # 4. Rooms with students of different genders
    print("\n4. Rooms with students of different genders:")
    if results["task_4_mixed_gender_rooms"]:
        for room in results["task_4_mixed_gender_rooms"]:
            print(f"  - {room['room_name']}")
    else:
        print("  No such rooms found")


def save_to_json(results, filename="query_results.json"):
    """Save results to JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nResults saved to file: {filename}")


def main():
    db = None
    try:
        # Connect to database
        print("Connecting to database...")
        db = DatabaseConnector(**DB_CONFIG)

        # Create QueryRunner instance
        query_runner = QueryRunner(db)

        # Run all queries
        print("Executing queries...")
        results = query_runner.run_all_queries()

        # Print results
        print_results(results)

        # Save results to file
        save_to_json(results)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if db is not None:
            db.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()