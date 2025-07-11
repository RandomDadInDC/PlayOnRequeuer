"""playon_requeue.py  -  General PlayOn Home re-queue utility

Re-queue failed (Status = 4) and, optionally, partially-recorded (Status = 3)
items in a PlayOn Home database. Written for Windows.

Features
--------
1. Detect MediaMall / PlayOn processes; optional --kill flag to terminate them
   before modifying the DB.
2. Command-line arguments (stackable):
   - --title "TITLE"      (may be repeated)      - match SeriesTitle or Name.
   - --since KEYWORD       - filter by date; KEYWORD in {today, yesterday,
                             this-week, this-month, MM-DD-YY}. Default: none.
   - --movies-only         - only rows where Season IS NULL AND EpisodeNumber IS NULL.
   - --include-partial     - include Status 3 (partial) as well as Status 4.
   - --position POS        - where to insert:  beginning | end | after "Some Title".
   - --dry-run             - show what would be changed but don't write.
   - --kill                - automatically kill running MediaMall processes.
   - --all                 - requeue everything matching filters even if no title/date provided.
3. Safe SQLite edits (single transaction); ranks auto-calculated.

Example
-------
python playon_requeue.py --title "The Day of the Jackal" --movies-only --include-partial --since this-month --position end
python playon_requeue.py --title "Columbo" --since 06-01-24 --position beginning
python playon_requeue.py --title "Babylon 5" --position after --after-title "Babylon 5"
python playon_requeue.py --movies-only --since yesterday --include-partial --dry-run
python playon_requeue.py --title "Mythbusters" --since this-week --kill --position end
"""

import argparse
import sqlite3
import subprocess
import sys
import os
import re
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

DB_PATH_DEFAULT = r"C:\ProgramData\MediaMall\Recording\recording.db"
PROCESS_NAMES   = ("PlayOn", "MediaMallServer", "MediaMall", "SettingsManager", "POC-Downloader" )

def parse_since(token: str) -> datetime:
    token = token.lower()
    now   = datetime.now(timezone.utc)

    if token == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if token == "yesterday":
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return today - timedelta(days=1)
    if token in ("this-week", "week", "w"):
        start = now - timedelta(days=now.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0)
    if token in ("this-month", "month", "m"):
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    try:
        return datetime.strptime(token, "%m-%d-%y").replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Unrecognised --since value: {token}")

def find_running_pids() -> List[int]:
    pids = []
    try:
        output = subprocess.check_output("tasklist /FO CSV", text=True)
        for line in output.splitlines()[1:]:
            cols = [c.strip('"') for c in line.split(',')]
            if cols and cols[0].split('.')[0] in PROCESS_NAMES:
                pids.append(int(cols[1]))
    except subprocess.CalledProcessError:
        pass
    return pids

def build_where(args) -> Tuple[str, List]:
    clauses, params = [], []

    statuses = [4]
    if args.include_partial:
        statuses.append(3)
    placeholders = ",".join(["?"] * len(statuses))
    clauses.append(f"Status IN ({placeholders})")
    params.extend(statuses)

    if args.title:
        title_clauses = []
        for t in args.title:
            title_clauses.append("lower(SeriesTitle) = ? OR lower(Name) = ?")
            params.extend([t.lower(), t.lower()])
        clauses.append("(" + " OR ".join(title_clauses) + ")")

    if args.movies_only:
        clauses.append("Season IS NULL AND EpisodeNumber IS NULL")

    if args.since_dt:
        clauses.append("Updated >= ?")
        params.append(args.since_dt.strftime("%Y-%m-%d %H:%M:%S"))

    where_sql = " AND ".join(clauses) if clauses else "1"
    return where_sql, params

def compute_insert_ranks(cur, count: int, position: str, after_title: str | None):
    if position == "beginning":
        min_rank = cur.execute(
            "SELECT COALESCE(MIN(Rank), 0) FROM RecordQueueItems WHERE Status IN (0,1)"
        ).fetchone()[0]
        return [min_rank - i - 1 for i in range(count)]

    if position == "end":
        max_rank = cur.execute(
            "SELECT COALESCE(MAX(Rank), 0) FROM RecordQueueItems WHERE Status IN (0,1)"
        ).fetchone()[0]
        return [max_rank + i + 1 for i in range(count)]

    row = cur.execute(
        "SELECT Rank FROM RecordQueueItems WHERE (SeriesTitle = ? OR Name = ?) AND Status IN (0,1)"
        " ORDER BY Rank DESC LIMIT 1",
        (after_title, after_title)
    ).fetchone()
    if not row:
        raise ValueError(f'Could not find queued/recording item titled "{after_title}"')
    base = row[0]
    return [base + (i + 1) * 0.001 for i in range(count)]

def requeue_items(args):
    if not args.all and not (args.title or args.since or args.movies_only or args.include_partial):
        print("Can not requeue all items unless --all is specified. Please select a filtering option or specify --all.")
        return

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    where_sql, params = build_where(args)
    query = f"SELECT ID, Rank, Name, SeriesTitle FROM RecordQueueItems WHERE {where_sql} ORDER BY Updated DESC"
    to_promote = cur.execute(query, params).fetchall()

    if not to_promote:
        print("No matching failed/partial rows found.")
        return

    print(f"Found {len(to_promote)} item(s) to re-queue.")
    ranks = compute_insert_ranks(cur, len(to_promote), args.position, args.after_title)

    if args.dry_run:
        print("\nDRY RUN - the following items would be requeued:\n")

        print("Current Queue:")
        existing = cur.execute(
            "SELECT ID, Rank, Name, SeriesTitle FROM RecordQueueItems WHERE Status IN (0,1) ORDER BY Rank"
        ).fetchall()
        for rec_id, rank, name, series in existing:
            print(f"  ID {rec_id:>6}  Rank={rank:<8}  {series or name}")

        print("\nProposed additions:")
        for (rec_id, _, name, series), r in zip(to_promote, ranks):
            print(f"  ID {rec_id:>6}  Rank->{r:<8}  {series or name}")
        return

    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("BEGIN TRANSACTION;")
    for (rec_id, _, _, _), new_rank in zip(to_promote, ranks):
        cur.execute(
            """
            UPDATE RecordQueueItems
            SET Status  = 0,
                Rank    = ?,
                Error   = NULL,
                Queued  = ?,
                Updated = ?
            WHERE ID    = ?
            """,
            (new_rank, utc_now, utc_now, rec_id)
        )
    con.commit()
    print(f"Promoted {len(to_promote)} item(s). PlayOn must be restarted to reload the queue.")

def parse_args():
    if "--help" in sys.argv:
        print(__doc__)
        sys.exit(0)

    p = argparse.ArgumentParser(description="Re-queue failed/partial PlayOn recordings.", add_help=False)
    p.add_argument("--db", default=DB_PATH_DEFAULT, help="Path to recording.db")
    p.add_argument("--title", action="append", help="Title or SeriesTitle to match (repeatable)")
    p.add_argument("--since", dest="since", help="Date filter: today|yesterday|this-week|this-month|MM-DD-YY")
    p.add_argument("--movies-only", action="store_true")
    p.add_argument("--include-partial", action="store_true", help="Include Status=3 rows as well as 4")
    p.add_argument("--position", default="end", choices=["beginning", "end", "after"], help="Where to insert new items")
    p.add_argument("--after-title", help="Title to insert after (required if --position after)")
    p.add_argument("--kill", action="store_true", help="Kill running PlayOn processes automatically")
    p.add_argument("--dry-run", action="store_true", help="Do not modify DB; just list actions")
    p.add_argument("--all", action="store_true", help="Allow requeueing of all matching items without filters")
    return p.parse_args()

def main():
    args = parse_args()
    args.since_dt = parse_since(args.since) if args.since else None

    running = find_running_pids()
    if running:
        print(f"Detected running PlayOn processes: {running}")
        if not args.kill:
            print("Rerun with --kill or close PlayOn manually.")
            sys.exit(1)
        print("Killing processes...")
        for pid in running:
            subprocess.call(["taskkill", "/PID", str(pid), "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    requeue_items(args)

if __name__ == "__main__":
    main()