# recording_inspector.py - ChatGPT July 2025
import sqlite3
import csv
import os

DB_PATH = "C:\\ProgramData\\MediaMall\\Recording\\recording.db"  # adjust if needed
OUTPUT_CSV = "failed_recordings.csv"
DEBUG = True

def main():
    if not os.path.exists(DB_PATH):
        print("ERROR: Database file not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    table = "RecordQueueItems"
    print(f"Inspecting table: {table}")
    
    try:
        query = f"""
        SELECT * FROM {table}
        WHERE Status LIKE '%fail%' OR Error LIKE '%fail%' OR
              Status LIKE '%network%' OR Error LIKE '%network%'
        LIMIT 1000
        """
        rows = cursor.execute(query).fetchall()
        headers = [d[0] for d in cursor.description]

        if not rows:
            print("No failed recordings found.")
            return

        with open(OUTPUT_CSV, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

        print(f"✅ {len(rows)} failed recordings written to {OUTPUT_CSV}")

    except Exception as e:
        print("Error while querying:", e)

    conn.close()

if __name__ == "__main__":
    main()
