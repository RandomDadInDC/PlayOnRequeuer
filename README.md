**READ THIS FIRST -- HIGH RISK OPERATION!**
-------------------------------------------------
Running this script will modify *recording.db* used by PlayOn Home.  A corrupt or
inconsistent database can prevent PlayOn from launching or recording correctly.

* Make sure PlayOn Home is **COMPLETELY CLOSED** (tray icon exited) **or** use
   the `--kill` or `--restart` flag so the script can manage the processes.
   (Note: --restart may not work right yet.)
* The script **creates a timestamped backup** copy of the original database
   (`recording.db.bak-YYYYMMDD-HHMMSS`) in the same directory before any write.
* Use `--dry-run` first.  It prints what *would* change and makes **NO** edits.
* You are solely responsible for any data loss or disruption.  Running this
   script *probably voids any warranty*.

Requirements
------------
* Windows 10/11 with Python 3.10+ (includes the built-in `sqlite3` module).
* The script checks that it can import `sqlite3`; if that fails, it exits.
* Sorry, no idea if this will work with the Mac version.

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

options:
  -h, --help            show this help message and exit
  --db DB               Path to recording.db. Default: C:\ProgramData\MediaMall\Recording\recording.db
  --title TITLE         Title or SeriesTitle to match (case-insensitive, repeatable).
  --since SINCE         Date filter: today|yesterday|this-week|this-month|MM-DD-YY.
  --movies-only         Only re-queue items that appear to be movies (no season/episode).
  --include-partial     Include Status=3 (partial) rows in addition to Status=4 (failed).
  --position {beginning,end,after}
                        Where to insert new items. Default: end
  --after-title AFTER_TITLE
                        Title to insert after (required if --position is 'after').
  --kill                Kill running PlayOn processes automatically before running.
  --restart             Kill, re-queue, and then restart PlayOn services. Implies --kill.
  --dry-run             Do not modify DB; just list the actions that would be taken.
  --dry-run-output FILE
                        Export the proposed additions to a CSV file during a dry run.
  --all                 Allow re-queueing without any filters.
  --limit LIMIT         Limit the number of items to re-queue (applied after filtering).
  --verbose             Print verbose output, including SQL statements and raw data rows.
  --no-backup           Skip creating a database backup (NOT RECOMMENDED).
