import psycopg2
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DIR_PATH = os.getenv("DIR_PATH")
if (None in [DB_NAME, DB_HOST, DB_PORT, DIR_PATH]):
    print("!! environment not setup properly !!\n!! please check it manually !!\n!! then retry !!")
    quit()

CONN = psycopg2.connect(database=DB_NAME,
                        host=DB_HOST, port=DB_PORT)
print("Opened database successfully")
CURSOR = CONN.cursor()

def reset_history(particular_day=None, block_index=None, transaction_index=None, fetch_start_day=None):
    CURSOR.execute(f"select particular_day_seconds from history limit 1;")
    stored_day = CURSOR.fetchone()[0]
    CURSOR.execute(f"UPDATE history SET (particular_day_seconds, block_index, transaction_index, starting_date) = ({particular_day}, {block_index}, {transaction_index}, {fetch_start_day}) where particular_day_seconds = {stored_day};")
        
    CURSOR.close()
    CONN.commit()
    CONN.close()

reset_history(0,0,0, "NULL")
print("Reset Successful!")