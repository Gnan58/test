import psycopg2
import os
import logging
from dotenv import load_dotenv
from psycopg2 import OperationalError
from psycopg2.extras import execute_values

# Configure the logger to write to a file
logging.basicConfig(filename='PeruRawDB.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load the environment variables
load_dotenv()

# Database connection parameters
db_config = {
    'dbname': os.getenv("DATABASE_NAME"),
    'user': os.getenv("USER"),
    'password': os.getenv("PASSWORD"),
    'host': os.getenv("DATABASE_HOST")
}

# connection to database
def create_connection():
    try:
        conn = psycopg2.connect(**db_config)
        logging.info("Connection to PostgreSQL database successful")
        return conn
    except OperationalError as e:
        logging.error(f"The error '{e}' occurred")
        return None

def create_table_if_not_exists(conn, table_name, df):
    try:
        # Convert tuple column names to string
        df.columns = ['_'.join(col) if isinstance(col, tuple) else col for col in df.columns]
        # Create the columns string for the CREATE TABLE query
        columns = ', '.join([f"{col} TEXT" for col in df.columns])
        # Create the CREATE TABLE query
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"

        # Execute the query
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
        logging.info(f"Table {table_name} created (if not exists)")
    except Exception as e:
        logging.error(f"Error in create_table_if_not_exists: {e}")

def insert_data(conn, table_name, df):
    try:
        with conn.cursor() as cur:
            # create the query string
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES %s"
            # create a list of tuples from the dataframe values
            values = list(df.itertuples(index=False, name=None))
            # use execute_values to perform the batch insertion
            execute_values(cur, insert_query, values)
            conn.commit()
        logging.info(f"Data inserted into {table_name}")
    except Exception as e:
        logging.error(f"Error in insert_data: {e}")
