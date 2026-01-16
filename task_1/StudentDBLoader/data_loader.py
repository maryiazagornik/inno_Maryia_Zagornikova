# data_loader.py
import json
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class DataLoader:
    """
    Handles reading data from JSON files and loading it into the database.
    
    Attributes:
        connector (DatabaseConnector): Instance of DatabaseConnector for database operations.
    """

    def __init__(self, connector: 'DatabaseConnector') -> None:
        """Initialize DataLoader with a database connector.
        
        Args:
            connector: Database connector instance.
        """
        self.connector = connector
        logger.info("DataLoader initialized")

    def _read_json_file(self, filepath: Union[str, Path]) -> Optional[Union[List, Dict]]:
        """Read and parse JSON file.
        
        Args:
            filepath: Path to the JSON file.
            
        Returns:
            Parsed JSON data as list or dict, or None if error occurs.
        """
        try:
            filepath = Path(filepath)
            if not filepath.exists():
                raise FileNotFoundError(f"File not found: {filepath}")
                
            with filepath.open('r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    logger.info(f"Successfully read {len(data) if isinstance(data, list) else 1} records from {filepath}")
                    return data
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in {filepath}: {str(e)}")
                    raise ValueError(f"Invalid JSON format in {filepath}") from e
                    
        except Exception as e:
            logger.error(f"Error reading {filepath}: {str(e)}")
            raise

    def load_rooms(self, rooms_path: Union[str, Path]) -> None:
        """Load room data from JSON file into the database.
        
        Args:
            rooms_path: Path to the JSON file containing room data.
            
        Raises:
            ValueError: If the data is invalid or missing required fields.
        """
        try:
            rooms_data = self._read_json_file(rooms_path)
            if not rooms_data:
                logger.warning("No room data to load")
                return

            if not isinstance(rooms_data, list):
                raise ValueError("Expected a list of rooms")

            # Validate room data
            valid_rooms = []
            for i, room in enumerate(rooms_data, 1):
                if not all(key in room for key in ['id', 'name']):
                    logger.warning(f"Skipping invalid room at position {i}: missing required fields")
                    continue
                valid_rooms.append(room)

            if not valid_rooms:
                logger.warning("No valid room data to load")
                return

            query = """
                INSERT INTO rooms (id, name) 
                VALUES (%(id)s, %(name)s) 
                ON CONFLICT (id) DO NOTHING
            """

            self.connector.execute_many(query, valid_rooms)
            logger.info(f"Successfully loaded {len(valid_rooms)} rooms")
            
        except Exception as e:
            logger.error(f"Failed to load rooms: {str(e)}")
            raise

    def load_students(self, students_path: Union[str, Path]) -> None:
        """Load student data from JSON file into the database.
        
        Args:
            students_path: Path to the JSON file containing student data.
            
        Raises:
            ValueError: If the data is invalid or missing required fields.
        """
        try:
            students_data = self._read_json_file(students_path)
            if not students_data:
                logger.warning("No student data to load")
                return

            if not isinstance(students_data, list):
                raise ValueError("Expected a list of students")

            prepared_data = []
            skipped = 0
            
            for student in students_data:
                try:
                    # Validate required fields
                    if not all(key in student for key in ['id', 'name', 'birthday', 'sex', 'room']):
                        logger.warning(f"Skipping student with ID {student.get('id', 'unknown')}: missing required fields")
                        skipped += 1
                        continue
                        
                    # Convert and validate date
                    try:
                        bday_dt = datetime.fromisoformat(student['birthday'].rstrip('Z'))
                        student['birthday'] = bday_dt.date()
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Skipping student {student.get('id')}: invalid date format: {student['birthday']}")
                        skipped += 1
                        continue
                    
                    # Validate sex field
                    if student['sex'] not in ['M', 'F']:
                        logger.warning(f"Skipping student {student.get('id')}: invalid sex: {student['sex']}")
                        skipped += 1
                        continue
                    
                    # Prepare data for insertion
                    prepared_student = {
                        'id': student['id'],
                        'name': student['name'].strip(),
                        'birthday': student['birthday'],
                        'sex': student['sex'].upper(),
                        'room_id': student['room']
                    }
                    prepared_data.append(prepared_student)
                    
                except Exception as e:
                    logger.error(f"Error processing student {student.get('id', 'unknown')}: {str(e)}")
                    skipped += 1
                    continue

            if not prepared_data:
                logger.warning("No valid student data to load")
                return

            query = """
                INSERT INTO students (id, name, birthday, sex, room_id)
                VALUES (%(id)s, %(name)s, %(birthday)s, %(sex)s, %(room_id)s)
                ON CONFLICT (id) DO NOTHING
            """

            self.connector.execute_many(query, prepared_data)
            logger.info(f"Successfully loaded {len(prepared_data)} students. Skipped {skipped} invalid records.")
            
        except Exception as e:
            logger.error(f"Failed to load students: {str(e)}")
            raise