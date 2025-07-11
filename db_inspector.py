"""
db_inspector.py - A read-only utility to inspect the PlayOn database.
=====================================================================

Purpose:
--------
This script is designed to safely explore the contents of the PlayOn Home
`recording.db` file. It performs NO writes or modifications of any kind. Its
goal is to help identify where different types of data, like notifications,
might be stored by dumping the structure and a sample of data from every
table in the database.

How to Run:
-----------
1. Make sure PlayOn Home is completely closed (including the tray icon) to
   ensure the database file is not locked.
2. Open a command prompt or terminal in the directory where you've saved
   this script.
3. Run the script and redirect its output to a file:
   python db_inspector.py > output.txt

The script will print the schema and first 5 rows of every table it finds into
the output file. Look through the output for tables with names or columns that
suggest they might store notifications or event logs.
"""

import sqlite3
import os
import sys
import codecs

# Default path to the PlayOn database
DB_PATH_DEFAULT = r"C:\ProgramData\MediaMall\Recording\recording.db"

def inspect_database(db_path: str):
    """
    Connects to the SQLite database, lists all tables, and prints the
    schema and first 5 rows of each table.
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at '{db_path}'")
        print("Please ensure PlayOn Home is installed and the path is correct.")
        return

    print(f"Inspecting database: {db_path}\n")

    try:
        # Connect in read-only mode to be extra safe
        # The URI=True is necessary for read-only mode.
        db_uri = f"file:{db_path}?mode=ro"
        con = sqlite3.connect(db_uri, uri=True)
        cur = con.cursor()
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        print("Please ensure PlayOn Home is completely closed (including the tray icon).")
        return

    # Get a list of all tables in the database
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cur.fetchall()
    except sqlite3.Error as e:
        print(f"Error fetching table list: {e}")
        con.close()
        return

    if not tables:
        print("No tables found in the database.")
        con.close()
        return

    print(f"Found {len(tables)} tables. Dumping schema and sample data for each:\n")

    # For each table, print its schema and sample data
    for table_name_tuple in tables:
        table_name = table_name_tuple[0]
        print("-" * 60)
        print(f"Table: {table_name}")
        print("-" * 60)

        # Get and print the table schema
        try:
            cur.execute(f"PRAGMA table_info('{table_name}');")
            schema = cur.fetchall()
            print("Schema: (Column Index, Name, Type, Not Null, Default Value, Primary Key)")
            for col in schema:
                print(f"  {col}")
        except sqlite3.Error as e:
            print(f"  Error reading schema for table {table_name}: {e}")
            continue

        # Get and print the first 5 rows of data
        try:
            print("\nSample Data (first 5 rows):")
            cur.execute(f"SELECT * FROM '{table_name}' LIMIT 5;")
            rows = cur.fetchall()
            if not rows:
                print("  (No rows found)")
            else:
                for row in rows:
                    print(f"  {row}")
        except sqlite3.Error as e:
            print(f"  Error reading data from table {table_name}: {e}")
        
        print("\n")

    con.close()

if __name__ == "__main__":
    # Reconfigure stdout to use UTF-8 encoding to prevent UnicodeEncodeError
    # when redirecting output to a file on Windows. This is a robust way to
    # handle potential special characters in the database data.
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')

    inspect_database(DB_PATH_DEFAULT)
