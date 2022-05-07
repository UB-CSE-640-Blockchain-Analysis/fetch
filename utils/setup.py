import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DIR_PATH = input("Enter the ABSOLUTE Directory path where you want to store transactions: ")
if DIR_PATH == "":
    print("Please rerun and enter a path.")
    quit()
yn = input("Confirm path = \""+DIR_PATH+"\"? (y/n) ").strip().lower()
if yn != "y" and yn != "yes" and yn != "":
    print("Okay, rerun the script!")
    quit()
 
DB_NAME = input("Enter the database name you want (default: cse640): ")
if DB_NAME == "":
    print("Using default name \"cse640\".")
    DB_NAME = "cse640"
else:
    yn = input("Confirm name = \""+DB_NAME+"\"? (y/n) ").strip().lower()
    if yn != "y" and yn != "yes" and yn != "":
        print("Okay, rerun the script!")
        quit()
 
DB_HOST = input("Enter the host address of database server (default: 127.0.0.1): ")
if DB_HOST == "":
    print("Using default host address \"127.0.0.1\".")
    DB_HOST = "127.0.0.1"
else:
    yn = input("Confirm host address = \""+DB_HOST+"\"? (y/n) ").strip().lower()
    if yn != "y" and yn != "yes" and yn != "":
        print("Okay, rerun the script!")
        quit()

DB_PORT = input("Enter the port number of database server (default: 5432): ")
if DB_PORT == "":
    print("Using default host address \"5432\".")
    DB_PORT = "5432"
else:
    yn = input("Confirm port number = \""+DB_PORT+"\"? (y/n) ").strip().lower()
    if yn != "y" and yn != "yes" and yn != "":
        print("Okay, rerun the script!")
        quit()

 

print(f"DIR_PATH = {DIR_PATH}")
print(f"DB_NAME = {DB_NAME}")
print(f"DB_HOST = {DB_HOST}")
print(f"DB_PORT = {DB_PORT}")

print("Creating database using above fields...")

CONN = psycopg2.connect(host=DB_HOST, port=DB_PORT)
CURSOR = CONN.cursor()
try:
    CONN.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    CURSOR.execute('CREATE DATABASE ' + DB_NAME)
    CURSOR.close()
    CONN.commit()
    CONN.close()
    CONN = psycopg2.connect(database=DB_NAME, host=DB_HOST, port=DB_PORT)
    CURSOR = CONN.cursor()
    CURSOR.execute('''CREATE TABLE "main_hash" (
                      "main_address" text PRIMARY KEY,
                      "secondary_addresses" text[]
                      );''')
    CURSOR.execute('''CREATE INDEX ON "main_hash" ("main_address");''')
    CURSOR.execute('''CREATE TABLE "secondary_hash" (
                      "secondary_address" text PRIMARY KEY,
                      "main_address" text
                      );''')
    CURSOR.execute('''CREATE INDEX ON "secondary_hash" ("secondary_address");''')
    CURSOR.execute('''CREATE TABLE "history" (
                      "particular_day_seconds" bigint,
                      "block_index" bigint,
                      "transaction_index" bigint,
                      "starting_date" date
                      );''')
    CURSOR.execute("INSERT INTO history VALUES (0, 0, 0, NULL);")
    CURSOR.close()
    CONN.commit()
    CONN.close()
    print("Done creating database successfully!")
except Exception as e:
    print(e)
    print("error occured, please initialize manually")
    quit()

print("Creating environment file...")
dotenv_file = open(".env", "w")
dotenv_file.write("DB_NAME="+DB_NAME+"\n")
dotenv_file.write("DB_HOST="+DB_HOST+"\n")
dotenv_file.write("DB_PORT="+DB_PORT+"\n")
dotenv_file.write("DIR_PATH="+DIR_PATH)
dotenv_file.close()

print("Done creating .env file.")
print("\nSetup Successfully completed.")