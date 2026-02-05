import os
import psycopg2
from dotenv import load_dotenv
from tqdm.notebook import tqdm


def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database.
    
    Returns:
        connection: A psycopg2 connection object.
    """
    try:
        load_dotenv(dotenv_path="../util/.env")
    except Exception as e:
        print(f"Check the .env file in util: {str(e)}")

    return psycopg2.connect(
        host=os.environ.get("HOST"),
        dbname=os.environ.get("DBNAME"),
        user=os.environ.get("DBUSER"),
        password=os.environ.get("PASSWORD")
    )

def get_all_url():
    """
    Fetches all URLs from the 'Task' table in the database.
    
    Returns:
        list: A list of URLs, or None if an error occurs.
    """
    query = """SELECT url FROM "Task" t"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query)
        all_urls = [url[0] for url in cur.fetchall()]
        cur.close()
        conn.close()
        print(f"Fetched all URLs from 'Task' table.")
        return all_urls
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def get_all_file_name():
    """
    Fetches all file names from the 'Task' table in the database.
    
    Returns:
        list: A list of file names, or None if an error occurs.
    """
    query = """SELECT file_name FROM "Task" t"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query)
        all_filenames = [filename[0] for filename in cur.fetchall()]
        cur.close()
        conn.close()
        print(f"Fetched all file names from 'Task' table.")
        return all_filenames
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def get_max_db_id():
    """
    Fetches the maximum ID from the 'Task' table in the database.
    
    Returns:
        int: The maximum ID, or None if an error occurs.
    """
    query = """SELECT MAX(id) FROM "Task" t"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query)
        max_id = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"Maximum ID in 'Task' table: {max_id}")
        return max_id
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
