"""
playon_requeue.py  -  General PlayOn Home re-queue utility
=========================================================

**READ THIS FIRST -- HIGH RISK OPERATION!**
-------------------------------------------------
Running this script will modify *recording.db* used by PlayOn Home.  A corrupt or
inconsistent database can prevent PlayOn from launching or recording correctly.

* Make sure PlayOn Home is **COMPLETELY CLOSED** (tray icon exited) **or** use
   the `--kill` flag so the script force-kills any MediaMall processes.
* The script **creates a timestamped backup** copy of the original database
   (`recording.db.bak-YYYYMMDD-HHMMSS`) in the same directory before any write.
* Use `--dry-run` first.  It prints what *would* change and makes **NO** edits.
* You are solely responsible for any data loss or disruption.  Running this
   script *probably voids any warranty*.

Requirements
------------
* Windows 10/11 with Python 3.10+ (includes the built-in `sqlite3` module).
* The script checks that it can import `sqlite3`; if that fails, it exits.

Purpose
-------
PlayOn Home is great but when it fails, it REALLY fails. This script handles
it when your network suddenly goes down and you end up with hundreds of failed
recordings. No more individually clicking multiple times to re-add each movie or
show back into your queue!

This script will re-queue failed recordings (`Status = 4`) and optionally
partially-recorded shows (`Status = 3`).  You can filter by title, date,
movie-only, etc., and insert the re-queued items at the beginning, end, or
immediately after the last occurrence of a given title in the current queue.

Command-line Flags (stackable)
--------------------
- `--title "TITLE"`       Match SeriesTitle *or* Name (repeatable).
- `--since KEYWORD`       `today`, `yesterday`, `this-week`, `this-month`, or
                          an explicit `MM-DD-YY` date.
- `--movies-only`         Only rows with **no** Season/Episode number.
- `--include-partial`     Include `Status = 3` (partial) items in search.
- `--position {beginning|end|after}`  Where to insert.
- `--after-title "TITLE"` Title anchor required if `--position after`.
- `--dry-run`             Show current queue *and* proposed queue; no edits.
- `--dry-run-output FILE` Export dry-run proposed additions to a CSV file.
- `--kill`                Kill running PlayOn / MediaMall processes first.
- `--all`                 Allow mass re-queue when no other filters are given.
- `--limit N`             Limit the number of items to re-queue.
- `--verbose`             Print verbose output, like SQL statements.
- `--no-backup`           Skip creating a database backup (NOT RECOMMENDED).


Examples
--------
```
python playon_requeue.py --title "The Day of the Jackal" --movies-only --include-partial --since this-month --position end
python playon_requeue.py --title "Columbo" --since 06-01-24 --position beginning
python playon_requeue.py --title "Babylon 5" --position after --after-title "Babylon 5"
python playon_requeue.py --movies-only --since yesterday --include-partial --dry-run --dry-run-output "proposed.csv"
python playon_requeue.py --title "Mythbusters" --since this-week --kill --position end --limit 5
```
"""

# ---------------------------------------------------------------------------
#  Standard Library Imports
# ---------------------------------------------------------------------------
import argparse
import os
import shutil
import sqlite3
import subprocess
import sys
import csv
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

# ---------------------------------------------------------------------------
#  Global Constants
# ---------------------------------------------------------------------------
DB_PATH_DEFAULT = r"C:\ProgramData\MediaMall\Recording\recording.db"
BACKUP_TEMPLATE = "recording.db.bak-{stamp}"
PROCESS_NAMES   = (
    "PlayOn", "MediaMallServer", "MediaMall", "SettingsManager", "POC-Downloader"
)

# ---------------------------------------------------------------------------
#  Utility: Pretty Timestamp
# ---------------------------------------------------------------------------
STAMP = datetime.now().strftime("%Y%m%d-%H%M%S")

# ---------------------------------------------------------------------------
#  Utility: Parse --since into a UTC datetime
# ---------------------------------------------------------------------------

# Converts user input like "yesterday" or "this-month" to a timezone-aware UTC datetime.
def parse_since(token: str) -> datetime:
    if token is None:
        return None
    token = token.lower()
    now   = datetime.now(timezone.utc)

    if token == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if token == "yesterday":
        return (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if token in ("this-week", "week", "w"):
        start = now - timedelta(days=now.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0)
    if token in ("this-month", "month", "m"):
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    try:
        return datetime.strptime(token, "%m-%d-%y").replace(tzinfo=timezone.utc)
    except ValueError as err:
        raise argparse.ArgumentTypeError(f"Invalid --since value: {token}") from err

# ---------------------------------------------------------------------------
#  Utility: Detect running PlayOn / MediaMall processes (returns PID list)
# ---------------------------------------------------------------------------

# Returns a list of matching PIDs for known PlayOn/MediaMall-related processes.
def find_running_pids() -> List[int]:
    pids: List[int] = []
    try:
        output = subprocess.check_output("tasklist /FO CSV", text=True, encoding="utf-8", errors="ignore")
        for line in output.splitlines()[1:]:
            cols = [c.strip('"') for c in line.split(',')]
            if cols and cols[0].split('.')[0] in PROCESS_NAMES:
                pids.append(int(cols[1]))
    except subprocess.CalledProcessError:
        pass
    return pids

# ---------------------------------------------------------------------------
#  Utility: Create a safety backup of the database file
# ---------------------------------------------------------------------------

# Makes a timestamped backup copy of the recording.db before editing.
def backup_database(db_path: str) -> str:
    backup_name = BACKUP_TEMPLATE.format(stamp=STAMP)
    backup_path = os.path.join(os.path.dirname(db_path), backup_name)
    shutil.copy2(db_path, backup_path)
    print(f"Database backed up to: {backup_path}")
    return backup_path

# ---------------------------------------------------------------------------
#  Build dynamic SQL WHERE clause from CLI filters
# ---------------------------------------------------------------------------

# Constructs the WHERE clause used to fetch only items matching CLI filters.
def build_where(args) -> Tuple[str, List]:
    clauses: List[str] = []
    params:  List      = []

    status_codes = [4] + ([3] if args.include_partial else [])
    clauses.append(f"Status IN ({','.join(['?']*len(status_codes))})")
    params.extend(status_codes)

    if args.title:
        t_clauses = []
        for t in args.title:
            t_clauses.append("lower(SeriesTitle) = ? OR lower(Name) = ?")
            params.extend([t.lower(), t.lower()])
        clauses.append(f"({' OR '.join(t_clauses)})")

    if args.movies_only:
        clauses.append("Season IS NULL AND EpisodeNumber IS NULL")

    if args.since_dt:
        clauses.append("Updated >= ?")
        params.append(args.since_dt.strftime("%Y-%m-%d %H:%M:%S"))

    return " AND ".join(clauses) if clauses else "1", params

# ---------------------------------------------------------------------------
#  Compute target ranks for inserting new items
# ---------------------------------------------------------------------------

# Given the insert mode, computes the new queue rank(s) for added items.
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

    # For 'after' position, find the last item with the anchor title.
    row = cur.execute(
        "SELECT Rank FROM RecordQueueItems"
        " WHERE (SeriesTitle = ? OR Name = ?) AND Status IN (0,1)"
        " ORDER BY Rank DESC LIMIT 1",
        (after_title, after_title)
    ).fetchone()
    if row is None:
        raise ValueError(f'Anchor title "{after_title}" not found in current queue.')
    base = row[0]
    # To insert items *after* the base rank without re-numbering the entire queue,
    # we add a small fractional value. The Rank column is a REAL (float), so this
    # places the new items neatly between two existing integer ranks.
    return [base + (i + 1) * 0.001 for i in range(count)]

def requeue_items(args):
    try:
        con = sqlite3.connect(args.db)
        cur = con.cursor()
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

    where_sql, params = build_where(args)
    query = f"SELECT ID, Rank, Name, SeriesTitle, Season, EpisodeNumber FROM RecordQueueItems WHERE {where_sql} ORDER BY Updated DESC"
    
    if args.verbose:
        print(f"Verbose: Executing SQL query: {query}")
        print(f"Verbose: With parameters: {params}")

    try:
        to_promote = cur.execute(query, params).fetchall()
    except sqlite3.Error as e:
        print(f"Error querying database: {e}")
        con.close()
        return
        
    if args.verbose:
        print(f"Verbose: Found {len(to_promote)} raw data row(s).")
        for row in to_promote:
            print(f"  - {row}")

    # Apply the limit if provided
    if args.limit and len(to_promote) > args.limit:
        print(f"Limiting selection from {len(to_promote)} to {args.limit} item(s).")
        to_promote = to_promote[:args.limit]

    if not to_promote:
        print("No matching failed/partial rows found.")
        con.close()
        return

    print(f"Found {len(to_promote)} item(s) to re-queue.")
    
    try:
        ranks = compute_insert_ranks(cur, len(to_promote), args.position, args.after_title)
    except ValueError as e:
        print(f"Error: {e}")
        con.close()
        return
    except sqlite3.Error as e:
        print(f"Database error while calculating ranks: {e}")
        con.close()
        return

    if args.dry_run:
        print("\nDRY RUN - the following items would be requeued:\n")
        print("Current Queue:")
        try:
            existing = cur.execute(
                "SELECT ID, Rank, Name, SeriesTitle, Season, EpisodeNumber FROM RecordQueueItems WHERE Status IN (0,1) ORDER BY Rank"
            ).fetchall()
            for rec_id, rank, name, series, season, episode in existing:
                # For display, use the series title if available, otherwise fall back to the item name (for movies).
                info = series or name
                # If it's a TV show (has season/episode), format and append that info.
                if season is not None and episode is not None:
                    info += f" S{int(season):02}E{int(episode):02}"
                print(f"  ID {rec_id:>6}  Rank={rank:<8.3f}  {info}")
        except sqlite3.Error as e:
            print(f"Error fetching current queue for dry run: {e}")

        print("\nProposed additions:")
        proposed_additions = list(zip(to_promote, ranks))
        for (rec_id, _, name, series, season, episode), r in proposed_additions:
            info = series or name
            if season is not None and episode is not None:
                info += f" S{int(season):02}E{int(episode):02}"
            print(f"  ID {rec_id:>6}  Rank->{r:<8.3f}  {info}")
        
        # Export to CSV if requested
        if args.dry_run_output:
            try:
                with open(args.dry_run_output, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Original_ID', 'Proposed_Rank', 'Title', 'Season', 'Episode'])
                    for (rec_id, _, name, series, season, episode), r in proposed_additions:
                        writer.writerow([rec_id, f"{r:.3f}", series or name, season, episode])
                print(f"\nDry run output saved to {args.dry_run_output}")
            except IOError as e:
                print(f"\nError writing to file {args.dry_run_output}: {e}")

        con.close()
        return

    # --- FINAL CONFIRMATION ---
    print("\n" + "="*60)
    print("!! WARNING: HIGH RISK OPERATION !!")
    print("="*60)
    print("You are about to make permanent changes to the PlayOn database.")
    print(f"This will re-queue {len(to_promote)} item(s).")
    
    if not args.no_backup:
        print("\nA backup will be created, but it is highly recommended to use --dry-run first.")
    else:
        print("\nWARNING: You have specified --no-backup. No backup will be created.")

    try:
        confirm = input("Are you sure you want to proceed? (yes/no): ")
    except EOFError:
        confirm = 'no'

    if confirm.lower() != 'yes':
        print("\nOperation cancelled by user.")
        con.close()
        return
    
    print("\nUser confirmed. Proceeding with changes...")
    
    # Actual update logic
    if not args.no_backup:
        backup_database(args.db)

    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        cur.execute("BEGIN TRANSACTION;")
        for (rec_id, _, _, _, _, _), new_rank in zip(to_promote, ranks):
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
    except sqlite3.Error as e:
        print(f"An error occurred during the database transaction: {e}")
        con.rollback()
    finally:
        con.close()


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, 
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--db", default=DB_PATH_DEFAULT, help="Path to recording.db. Default: %(default)s")
    p.add_argument("--title", action="append", help="Title or SeriesTitle to match (case-insensitive, repeatable).")
    p.add_argument("--since", dest="since", help="Date filter: today|yesterday|this-week|this-month|MM-DD-YY.")
    p.add_argument("--movies-only", action="store_true", help="Only re-queue items that appear to be movies (no season/episode).")
    p.add_argument("--include-partial", action="store_true", help="Include Status=3 (partial) rows in addition to Status=4 (failed).")
    p.add_argument("--position", default="end", choices=["beginning", "end", "after"], help="Where to insert new items in the queue. Default: %(default)s")
    p.add_argument("--after-title", help="Title to insert after (required if --position is 'after').")
    p.add_argument("--kill", action="store_true", help="Kill running PlayOn processes automatically before running.")
    p.add_argument("--dry-run", action="store_true", help="Do not modify DB; just list the actions that would be taken.")
    p.add_argument("--dry-run-output", metavar="FILE", help="Export the proposed additions to a CSV file during a dry run.")
    p.add_argument("--all", action="store_true", help="Allow re-queueing without any filters.")
    p.add_argument("--limit", type=int, help="Limit the number of items to re-queue (applied after filtering).")
    p.add_argument("--verbose", action="store_true", help="Print verbose output, including SQL statements and raw data rows.")
    p.add_argument("--no-backup", action="store_true", help="Skip creating a database backup (NOT RECOMMENDED).")

    return p.parse_args()

def main():
    # Check for sqlite3 module
    try:
        import sqlite3
    except ImportError:
        print("Error: The 'sqlite3' module is required but could not be imported.")
        print("This module is usually included with Python. Please check your installation.")
        sys.exit(1)

    args = parse_args()
    
    # Validate --after-title requirement
    if args.position == 'after' and not args.after_title:
        print("Error: --after-title is required when using --position 'after'")
        sys.exit(1)
        
    # Validate that at least one filter is used, unless --all is specified
    if not any([args.title, args.since, args.movies_only, args.include_partial, args.all]):
         print("No filters specified. You must use at least one filter (--title, --since, etc.)")
         print("or specify --all to re-queue everything. Use --help for more info.")
         sys.exit(1)

    try:
        args.since_dt = parse_since(args.since) if args.since else None
    except argparse.ArgumentTypeError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Check for DB file existence
    if not os.path.exists(args.db):
        print(f"Error: Database file not found at '{args.db}'")
        print("Please ensure PlayOn Home is installed and the path is correct.")
        sys.exit(1)

    running = find_running_pids()
    if running:
        print(f"Detected running PlayOn processes (PIDs: {', '.join(map(str, running))}).")
        if not args.kill:
            print("The database may be locked. Rerun with the --kill flag or close PlayOn completely (including the tray icon) and try again.")
            sys.exit(1)
        
        print("Attempting to kill processes...")
        for pid in running:
            try:
                # Using taskkill for Windows-specific forceful termination
                subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=True, capture_output=True, text=True)
                print(f"  Successfully terminated PID {pid}.")
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                print(f"  Failed to kill PID {pid}: {e}")
                print("Please close PlayOn manually.")
                sys.exit(1)
        print("All detected processes terminated.")

    requeue_items(args)

if __name__ == "__main__":
    main()
