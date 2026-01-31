# main.py
import argparse
import sys
from db_connector import DatabaseConnector
from data_loader import DataLoader
from query_runner import QueryRunner
from formatters import JsonFormatter, XmlFormatter
from config import DB_CONFIG, APP_SETTINGS, FILE_SETTINGS, QUERY_SETTINGS

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Student data loader and analyzer.")

    parser.add_argument(
        '--students',
        type=str,
        required=True,
        help="Path to students.json file"
    )
    parser.add_argument(
        '--rooms',
        type=str,
        required=True,
        help="Path to rooms.json file"
    )
    parser.add_argument(
        '--format',
        type=str,
        choices=['json', 'xml'],
        required=True,
        help="Output format: json or xml"
    )
    
    return parser.parse_args()
def main():
    args = parse_arguments()
    
    # Initialize database connection
    db = DatabaseConnector(**DB_CONFIG)
    
    try:
        # Create tables and indexes
        db.create_tables()
        
        # Initialize components
        data_loader = DataLoader(db)
        query_runner = QueryRunner(db)
        
        # Load data
        print("Loading data...")
        data_loader.load_rooms(args.rooms)
        data_loader.load_students(args.students)
        
        # Execute queries and collect results
        results = {}
        results['room_student_count'] = query_runner.get_room_student_count()
        results['top5_min_avg_age'] = query_runner.get_top5_min_avg_age()
        results['top5_max_age_diff'] = query_runner.get_top5_max_age_diff()
        results['mixed_gender_rooms'] = query_runner.get_mixed_gender_rooms()
        
        # Format output
        formatter = JsonFormatter() if args.format == 'json' else XmlFormatter()
        output = formatter.format(results)
        
        # Print results
        print("\nQuery Results:")
        print("-" * 50)
        print(output)
        
        # Save to file
        output_file = f"{FILE_SETTINGS['default_output_file']}.{args.format}"
        with open(output_file, 'w', encoding=FILE_SETTINGS['encoding']) as f:
            f.write(output)
        print(f"\nResults saved to {output_file}")
        
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()
        print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()