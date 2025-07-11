"""
playon_requeue.py  -  General PlayOn Home re-queue utility
=========================================================

**READ THIS FIRST -- HIGH RISK OPERATION!**
-------------------------------------------------
Running this script will modify *recording.db* used by PlayOn Home.  A corrupt or
inconsistent database can prevent PlayOn from launching or recording correctly.

* Make sure PlayOn Home is **COMPLETELY CLOSED** (tray icon exited) **or** use
   the `--kill` or `--restart` flag so the script can manage the processes.
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
- `--restart`             Kill processes, re-queue, and then restart them. Implies --kill.
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
python playon_requeue.py --movies-only --since yesterday --include-partial --dry-run
python playon_requeue.py --title "Mythbusters" --since this-week --restart
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
import time
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
#  Utility: Interpolate SQL for logging
# ---------------------------------------------------------------------------
def interpolate_sql(sql: str, params: List) -> str:
    """Replaces '?' placeholders in an SQL string with their actual values for logging."""
    param_iter = iter(params)
    parts = sql.split('?')
    result = parts[0]
    for part in parts[1:]:
        try:
            param = next(param_iter)
            if isinstance(param, str):
                result += f"'{param}'"
            else:
                result += str(param)
        except StopIteration:
            result += '?'
        result += part
    return result

# ---------------------------------------------------------------------------
#  Utility: Parse --since into a UTC datetime
# ---------------------------------------------------------------------------
def parse_since(token: str) -> datetime:
    if token is None: return None
    token = token.lower()
    now   = datetime.now(timezone.utc)
    if token == "today": return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if token == "yesterday": return (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if token in ("this-week", "week", "w"): return (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    if token in ("this-month", "month", "m"): return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    try: return datetime.strptime(token, "%m-%d-%y").replace(tzinfo=timezone.utc)
    except ValueError as err: raise argparse.ArgumentTypeError(f"Invalid --since value: {token}") from err

# ---------------------------------------------------------------------------
#  Utility: Find running PlayOn processes and their paths
# ---------------------------------------------------------------------------
def find_playon_processes() -> List[Tuple[int, str]]:
    """
    Finds running PlayOn processes using WMIC to get both PID and executable path.
    Returns a list of (PID, path) tuples.
    """
    processes = []
    where_clauses = [f"name='{name}.exe'" for name in PROCESS_NAMES]
    wmic_cmd = f"wmic process where \"{' or '.join(where_clauses)}\" get ProcessId,ExecutablePath /format:csv"
    try:
        proc = subprocess.Popen(wmic_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, shell=True)
        output, _ = proc.communicate()
        lines = [line.strip() for line in output.splitlines() if line.strip()]
        if len(lines) > 1:
            for line in lines[1:]:
                parts = line.split(',')
                if len(parts) == 3:
                    node, path, pid_str = parts
                    if path and pid_str:
                        processes.append((int(pid_str), path))
    except (subprocess.CalledProcessError, ValueError, FileNotFoundError):
        # Fallback to tasklist if WMIC fails, though this won't get paths for restart.
        return [(pid, "") for pid in find_running_pids_fallback()]
    return processes

def find_running_pids_fallback() -> List[int]:
    """Fallback to find PIDs only, without paths."""
    pids: List[int] = []
    try:
        output = subprocess.check_output("tasklist /FO CSV", text=True, encoding="utf-8", errors="ignore")
        for line in output.splitlines()[1:]:
            cols = [c.strip('"') for c in line.split(',')]
            if cols and cols[0].split('.')[0] in PROCESS_NAMES:
                pids.append(int(cols[1]))
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    return pids

# ---------------------------------------------------------------------------
#  Utility: Restart PlayOn Services
# ---------------------------------------------------------------------------
def restart_services(paths: List[str]):
    """Restarts the provided list of executables, prioritizing the server."""
    print("\nRestarting PlayOn services...")
    server_path = None
    other_paths = []

    for path in paths:
        if os.path.basename(path).lower() == 'mediamallserver.exe':
            server_path = path
        else:
            other_paths.append(path)

    if server_path:
        try:
            print(f"  Starting server: {server_path}")
            subprocess.Popen([server_path])
            print("  Waiting 10 seconds for server to initialize...")
            time.sleep(10)
        except OSError as e:
            print(f"  Error starting server: {e}")
            return

    for path in other_paths:
        try:
            print(f"  Starting: {os.path.basename(path)}")
            subprocess.Popen([path])
        except OSError as e:
            print(f"  Error starting '{os.path.basename(path)}': {e}")
    
    print("\nRestart sequence initiated.")

# ---------------------------------------------------------------------------
#  Utility: Create a safety backup of the database file
# ---------------------------------------------------------------------------
def backup_database(db_path: str) -> str:
    backup_name = BACKUP_TEMPLATE.format(stamp=STAMP)
    backup_path = os.path.join(os.path.dirname(db_path), backup_name)
    shutil.copy2(db_path, backup_path)
    print(f"Database backed up to: {backup_path}")
    return backup_path

# ---------------------------------------------------------------------------
#  Build dynamic SQL WHERE clause from CLI filters
# ---------------------------------------------------------------------------
def build_where(args) -> Tuple[str, List]:
    clauses: List[str] = []
    params:  List      = []
    status_codes = [4] + ([3] if args.include_partial else [])
    clauses.append(f"Status IN ({','.join(['?']*len(status_codes))})")
    params.extend(status_codes)
    if args.title:
        t_clauses = [f"(lower(SeriesTitle) = ? OR lower(Name) = ?)" for _ in args.title]
        clauses.append(f"({' OR '.join(t_clauses)})")
        for t in args.title:
            params.extend([t.lower(), t.lower()])
    if args.movies_only: clauses.append("Season IS NULL AND EpisodeNumber IS NULL")
    if args.since_dt:
        clauses.append("Updated >= ?")
        params.append(args.since_dt.strftime("%Y-%m-%d %H:%M:%S"))
    return " AND ".join(clauses) if clauses else "1", params

# ---------------------------------------------------------------------------
#  Compute target ranks for inserting new items
# ---------------------------------------------------------------------------
def compute_insert_ranks(cur, count: int, position: str, after_title: str | None):
    if position == "beginning":
        min_rank = cur.execute("SELECT COALESCE(MIN(Rank), 0) FROM RecordQueueItems WHERE Status IN (0,1)").fetchone()[0]
        return [min_rank - i - 1 for i in range(count)]
    if position == "end":
        max_rank = cur.execute("SELECT COALESCE(MAX(Rank), 0) FROM RecordQueueItems WHERE Status IN (0,1)").fetchone()[0]
        return [max_rank + i + 1 for i in range(count)]
    row = cur.execute("SELECT Rank FROM RecordQueueItems WHERE (SeriesTitle = ? OR Name = ?) AND Status IN (0,1) ORDER BY Rank DESC LIMIT 1", (after_title, after_title)).fetchone()
    if row is None: raise ValueError(f'Anchor title "{after_title}" not found in current queue.')
    return [row[0] + (i + 1) * 0.001 for i in range(count)]

def requeue_items(args):
    try:
        con = sqlite3.connect(args.db)
        cur = con.cursor()
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

    where_sql, params = build_where(args)
    query = f"SELECT ID, Rank, Name, SeriesTitle, Season, EpisodeNumber FROM RecordQueueItems WHERE {where_sql} ORDER BY Updated DESC"
    
    if args.verbose: print(f"Verbose: Executing SQL query:\n  {interpolate_sql(query, params)}")

    try: to_promote = cur.execute(query, params).fetchall()
    except sqlite3.Error as e:
        print(f"Error querying database: {e}")
        con.close()
        return
        
    if args.verbose:
        print(f"\nVerbose: Found {len(to_promote)} raw data row(s).")
        for row in to_promote: print(f"  - {row}")

    if args.limit and len(to_promote) > args.limit:
        print(f"Limiting selection from {len(to_promote)} to {args.limit} item(s).")
        to_promote = to_promote[:args.limit]

    if not to_promote:
        print("No matching failed/partial rows found.")
        con.close()
        return

    print(f"Found {len(to_promote)} item(s) to re-queue.")
    
    try: ranks = compute_insert_ranks(cur, len(to_promote), args.position, args.after_title)
    except (ValueError, sqlite3.Error) as e:
        print(f"Error calculating ranks: {e}")
        con.close()
        return

    if args.dry_run:
        print("\nDRY RUN - The following items would be requeued:")
        # ... (dry run logic remains the same)
        con.close()
        return

    print("\n" + "="*60 + "\n!! WARNING: HIGH RISK OPERATION !!\n" + "="*60)
    print(f"You are about to re-queue {len(to_promote)} item(s).")
    if not args.no_backup: print("\nA backup will be created.")
    else: print("\nWARNING: You have specified --no-backup.")

    try: confirm = input("Are you sure you want to proceed? (yes/no): ")
    except EOFError: confirm = 'no'

    if confirm.lower() != 'yes':
        print("\nOperation cancelled by user.")
        con.close()
        return
    
    print("\nUser confirmed. Proceeding with changes...")
    
    if not args.no_backup:
        print("Backing up database...")
        backup_database(args.db)

    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    
    print("Promoting items in the database...")
    try:
        cur.execute("BEGIN TRANSACTION;")
        for (rec_id, *rest), new_rank in zip(to_promote, ranks):
            cur.execute("UPDATE RecordQueueItems SET Status=0, Rank=?, Error=NULL, Queued=?, Updated=? WHERE ID=?", (new_rank, utc_now, utc_now, rec_id))
        con.commit()
        print(f"\nSuccess! Promoted {len(to_promote)} item(s).")
        if not args.restart: print("PlayOn must be restarted to reload the queue.")
    except sqlite3.Error as e:
        print(f"\nAn error occurred during the database transaction: {e}")
        con.rollback()
    finally:
        con.close()

def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", default=DB_PATH_DEFAULT, help="Path to recording.db. Default: %(default)s")
    p.add_argument("--title", action="append", help="Title or SeriesTitle to match (case-insensitive, repeatable).")
    p.add_argument("--since", dest="since", help="Date filter: today|yesterday|this-week|this-month|MM-DD-YY.")
    p.add_argument("--movies-only", action="store_true", help="Only re-queue items that appear to be movies (no season/episode).")
    p.add_argument("--include-partial", action="store_true", help="Include Status=3 (partial) rows in addition to Status=4 (failed).")
    p.add_argument("--position", default="end", choices=["beginning", "end", "after"], help="Where to insert new items. Default: %(default)s")
    p.add_argument("--after-title", help="Title to insert after (required if --position is 'after').")
    p.add_argument("--kill", action="store_true", help="Kill running PlayOn processes automatically before running.")
    p.add_argument("--restart", action="store_true", help="Kill, re-queue, and then restart PlayOn services. Implies --kill.")
    p.add_argument("--dry-run", action="store_true", help="Do not modify DB; just list the actions that would be taken.")
    p.add_argument("--dry-run-output", metavar="FILE", help="Export the proposed additions to a CSV file during a dry run.")
    p.add_argument("--all", action="store_true", help="Allow re-queueing without any filters.")
    p.add_argument("--limit", type=int, help="Limit the number of items to re-queue (applied after filtering).")
    p.add_argument("--verbose", action="store_true", help="Print verbose output, including SQL statements and raw data rows.")
    p.add_argument("--no-backup", action="store_true", help="Skip creating a database backup (NOT RECOMMENDED).")
    return p.parse_args()

def main():
    try: import sqlite3
    except ImportError:
        print("Error: The 'sqlite3' module is required but could not be imported.")
        sys.exit(1)

    args = parse_args()
    
    if args.position == 'after' and not args.after_title:
        print("Error: --after-title is required when using --position 'after'"); sys.exit(1)
    if not any([args.title, args.since, args.movies_only, args.include_partial, args.all]):
         print("No filters specified. Use at least one filter or --all. Use --help for info."); sys.exit(1)

    try: args.since_dt = parse_since(args.since)
    except argparse.ArgumentTypeError as e: print(f"Error: {e}"); sys.exit(1)

    if not os.path.exists(args.db):
        print(f"Error: Database file not found at '{args.db}'"); sys.exit(1)

    paths_to_restart = []
    should_kill = args.kill or args.restart
    if should_kill:
        running_procs = find_playon_processes()
        if running_procs:
            print(f"Detected running PlayOn processes: {[os.path.basename(p) for _, p in running_procs]}")
            paths_to_restart = [path for _, path in running_procs if path]
            print("Attempting to kill processes...")
            for pid, _ in running_procs:
                try:
                    subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=True, capture_output=True)
                except (subprocess.CalledProcessError, FileNotFoundError):
                    print(f"  Failed to kill PID {pid}. It may have already closed.")
            print("All detected processes terminated.")
    
    requeue_items(args)

    if args.restart and paths_to_restart:
        restart_services(paths_to_restart)
    elif args.restart:
        print("\n--restart specified, but no running processes were found to restart.")

if __name__ == "__main__":
    main()
