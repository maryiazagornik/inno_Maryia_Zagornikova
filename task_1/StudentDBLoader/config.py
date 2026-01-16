"""Configuration settings for the StudentDBLoader application."""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection settings
DB_CONFIG = {
    "dbname": os.getenv('DB_NAME', 'university_db'),
    "user": os.getenv('DB_USER', 'postgres'),
    "password": os.getenv('DB_PASSWORD', ''),  # Will be read from environment
    "host": os.getenv('DB_HOST', 'localhost'),
    "port": os.getenv('DB_PORT', '5432')
}

# Application settings
APP_SETTINGS = {
    "default_format": os.getenv('DEFAULT_FORMAT', 'json'),  # Default output format (json/xml)
    "log_level": os.getenv('LOG_LEVEL', 'INFO'),           # Logging level
    "max_retries": int(os.getenv('MAX_RETRIES', '3'))      # Max retries for DB operations
}

# File settings
FILE_SETTINGS = {
    "default_output_file": os.getenv('DEFAULT_OUTPUT_FILE', 'output'),
    "encoding": os.getenv('FILE_ENCODING', 'utf-8')
}

# Query settings
QUERY_SETTINGS = {
    "top_n": int(os.getenv('TOP_N_RESULTS', '5')),       # Default number of top-N results
    "batch_size": int(os.getenv('BATCH_SIZE', '1000'))   # Batch size for bulk operations
}

# Validate required settings
if not DB_CONFIG["password"]:
    raise ValueError("Database password is not set. Please set DB_PASSWORD in .env file")
