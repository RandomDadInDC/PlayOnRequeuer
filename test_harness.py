"""
test_harness.py - Unit testing harness for playon_requeue.py
============================================================

Purpose:
--------
This script provides a suite of automated tests for the `playon_requeue.py`
utility. It is designed to run without any risk to a user's live PlayOn
database or system processes.

How it Works:
-------------
- It uses Python's built-in `unittest` framework.
- It dynamically imports the script specified on the command line, even if the
  filename contains characters like dots (e.g., 'v6').
- For each test, it creates a temporary, in-memory SQLite database.
- This database is populated with a standard set of test data representing
  various scenarios (failed, partial, queued items, movies, TV shows).
- It uses `unittest.mock` to "patch" (intercept and replace) functions that
  interact with the outside world. This includes:
    - `sqlite3.connect`: Redirected to return a mock connection object that
      prevents the in-memory database from being closed prematurely.
    - `os.path.exists`, `shutil.copy2`: To simulate file operations without
      touching the disk.
    - `subprocess.run`, `find_running_pids`: To simulate finding and killing
      processes without affecting the system.
    - `print`, `input`: To capture script output and provide automated
      responses to prompts.
- Each test method checks a specific piece of functionality and asserts that
  the outcome is exactly what is expected.

How to Run:
-----------
1. Save this script as `test_harness.py` in the same directory as the
   script you want to test (e.g., `playon_requeue.v6.py`).
2. Open a command prompt or terminal in that directory.
3. Run the command, providing the name of the script to test:
   `python -m unittest test_harness.py playon_requeue.v6.py`

The tests will run, and you will see a summary of the results. "OK" means all
tests passed successfully.
"""

import unittest
from unittest.mock import patch, call, MagicMock
import sqlite3
import os
import sys
import importlib.util
import argparse
from datetime import datetime, timezone, timedelta

class TestPlayOnRequeue(unittest.TestCase):
    # This class variable will be populated by the main execution block
    # with the dynamically imported script module.
    script_module = None

    def setUp(self):
        """
        This method is called before each test function is executed.
        It sets up a clean, in-memory database and mocks external dependencies.
        """
        if not self.script_module:
            self.fail("Script module not loaded. Run the test harness with the script name as an argument.")

        # Create the real connection for the test setup and data creation.
        self.db_connection = sqlite3.connect(":memory:")
        self.addCleanup(self.db_connection.close)
        self.create_test_data()

        # --- Set up all mocks ---
        # Create a mock connection object that will be returned by the patched 'connect'.
        # This mock will delegate most calls to the real connection but will
        # intercept the 'close' call to prevent the in-memory DB from being destroyed.
        mock_conn_wrapper = MagicMock(spec=sqlite3.Connection)
        mock_conn_wrapper.cursor.side_effect = self.db_connection.cursor
        mock_conn_wrapper.commit.side_effect = self.db_connection.commit
        mock_conn_wrapper.rollback.side_effect = self.db_connection.rollback
        mock_conn_wrapper.execute.side_effect = self.db_connection.execute
        mock_conn_wrapper.executemany.side_effect = self.db_connection.executemany
        mock_conn_wrapper.close = MagicMock(return_value=None) # The intercepted call.

        # Patch the 'connect' function in the script's namespace to return our wrapper.
        self.mock_connect = patch(f'{self.script_module.__name__}.sqlite3.connect', return_value=mock_conn_wrapper)
        self.mock_connect.start()
        self.addCleanup(self.mock_connect.stop)

        # Patch other external dependencies
        self.mock_print = patch('builtins.print').start()
        self.addCleanup(self.mock_print.stop)
        
        self.mock_backup = patch(f'{self.script_module.__name__}.backup_database').start()
        self.addCleanup(self.mock_backup.stop)
        
        self.mock_find_pids = patch(f'{self.script_module.__name__}.find_running_pids').start()
        self.addCleanup(self.mock_find_pids.stop)

        self.mock_subprocess_run = patch('subprocess.run').start()
        self.addCleanup(self.mock_subprocess_run.stop)


    def create_test_data(self):
        """Populates the in-memory database with a variety of records."""
        cur = self.db_connection.cursor()
        cur.execute("""
        CREATE TABLE RecordQueueItems (
            ID INTEGER PRIMARY KEY,
            Name TEXT,
            SeriesTitle TEXT,
            Season REAL,
            EpisodeNumber REAL,
            Status INTEGER,
            Rank REAL,
            Updated TEXT,
            Error TEXT,
            Queued TEXT
        )
        """)
        now = datetime.now(timezone.utc)
        yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        last_week = (now - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        today = now.strftime("%Y-%m-%d %H:%M:%S")
        test_records = [
            (1, 'Episode 1', 'The Test Show', 1, 1, 0, 1.0, today, None, today),
            (2, 'Test Movie One', None, None, None, 0, 2.0, today, None, today),
            (3, 'Episode 2', 'The Test Show', 1, 2, 4, -1.0, today, 'Failed', today),
            (4, 'Test Movie Two', None, None, None, 4, -1.0, today, 'Failed', today),
            (5, 'Episode 3', 'The Test Show', 1, 3, 3, -1.0, today, 'Partial', today),
            (6, 'Episode 1', 'Old Show', 2, 1, 4, -1.0, last_week, 'Failed', last_week),
            (7, 'Episode 4', 'The Test Show', 1, 4, 4, -1.0, yesterday, 'Failed', yesterday),
        ]
        cur.executemany("INSERT INTO RecordQueueItems VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", test_records)
        self.db_connection.commit()

    def _get_args(self, arg_list):
        """Helper function to properly parse arguments by mocking sys.argv."""
        full_arg_list = ['test_harness.py'] + arg_list
        with patch.object(sys, 'argv', full_arg_list):
            args = self.script_module.parse_args()
        args.since_dt = self.script_module.parse_since(args.since) if args.since else None
        return args

    def test_requeue_failed_movie_by_title(self):
        """Tests re-queuing a single failed movie by its title."""
        args = self._get_args(['--title', 'Test Movie Two', '--movies-only'])
        with patch('builtins.input', return_value='yes'):
            self.script_module.requeue_items(args)
        cur = self.db_connection.cursor()
        cur.execute("SELECT Status, Rank FROM RecordQueueItems WHERE ID = 4")
        item = cur.fetchone()
        self.assertEqual(item[0], 0)
        self.assertGreater(item[1], 2.0)

    def test_requeue_with_limit(self):
        """Tests that the --limit flag correctly restricts the number of re-queued items."""
        args = self._get_args(['--title', 'The Test Show', '--limit', '1'])
        with patch('builtins.input', return_value='yes'):
            self.script_module.requeue_items(args)
        cur = self.db_connection.cursor()
        cur.execute("SELECT Status FROM RecordQueueItems WHERE ID = 3")
        self.assertEqual(cur.fetchone()[0], 0)
        cur.execute("SELECT Status FROM RecordQueueItems WHERE ID = 7")
        self.assertEqual(cur.fetchone()[0], 4)

    def test_dry_run_makes_no_changes(self):
        """Ensures that --dry-run finds items but does not alter the database."""
        args = self._get_args(['--title', 'The Test Show', '--dry-run'])
        self.script_module.requeue_items(args)
        cur = self.db_connection.cursor()
        cur.execute("SELECT Status FROM RecordQueueItems WHERE ID IN (3, 7)")
        statuses = [row[0] for row in cur.fetchall()]
        self.assertEqual(statuses, [4, 4])
        self.mock_print.assert_any_call("\nDRY RUN - the following items would be requeued:\n")

    def test_requeue_position_after(self):
        """Tests inserting re-queued items after a specific title using fractional ranks."""
        args = self._get_args(['--title', 'Old Show', '--position', 'after', '--after-title', 'The Test Show'])
        with patch('builtins.input', return_value='yes'):
            self.script_module.requeue_items(args)
        cur = self.db_connection.cursor()
        cur.execute("SELECT Rank FROM RecordQueueItems WHERE ID = 6")
        new_rank = cur.fetchone()[0]
        self.assertAlmostEqual(new_rank, 1.001)

    def test_no_backup_flag(self):
        """Ensures the --no-backup flag prevents the backup function from being called."""
        args = self._get_args(['--title', 'Test Movie Two', '--no-backup'])
        with patch('builtins.input', return_value='yes'):
            self.script_module.requeue_items(args)
        self.mock_backup.assert_not_called()

    def test_backup_is_called_by_default(self):
        """Ensures the backup function is called by default (i.e., without --no-backup)."""
        args = self._get_args(['--title', 'Test Movie Two'])
        with patch('builtins.input', return_value='yes'):
            self.script_module.requeue_items(args)
        self.mock_backup.assert_called_once()

    def test_kill_processes(self):
        """Tests that the --kill flag correctly identifies and tries to kill processes."""
        self.mock_find_pids.return_value = [123, 456]
        # We need to patch requeue_items to stop the main function from continuing
        with patch(f'{self.script_module.__name__}.requeue_items'):
            test_args = ['test_harness.py', '--kill', '--title', 'foo']
            with patch.object(sys, 'argv', test_args):
                self.script_module.main()
        self.mock_find_pids.assert_called_once()
        calls = [
            call(["taskkill", "/PID", "123", "/F"], check=True, capture_output=True, text=True),
            call(["taskkill", "/PID", "456", "/F"], check=True, capture_output=True, text=True)
        ]
        self.mock_subprocess_run.assert_has_calls(calls, any_order=True)

if __name__ == '__main__':
    # --- Main execution block ---
    # This block parses our custom command-line argument to find out which
    # script to test, loads it, and then passes control to unittest.
    
    parser = argparse.ArgumentParser(description="Testing harness for PlayOn requeue script.")
    parser.add_argument('script_name', help="The Python script file to test (e.g., playon_requeue.v6.py)")
    
    args, remaining_argv = parser.parse_known_args()
    
    script_path = args.script_name
    # Create a valid module name from the filename by replacing invalid characters.
    module_name = os.path.splitext(script_path)[0].replace('.', '_').replace('-', '_')
    
    try:
        # Use importlib.util to load the module from a specific file path.
        # This correctly handles filenames that are not valid Python identifiers.
        spec = importlib.util.spec_from_file_location(module_name, script_path)
        if spec is None:
            raise ImportError(f"Could not create module spec for {script_path}")
        
        script_module = importlib.util.module_from_spec(spec)
        # Add the module to sys.modules so that patch() can find it.
        sys.modules[module_name] = script_module
        spec.loader.exec_module(script_module)
        
        # Assign the loaded module to the test class.
        TestPlayOnRequeue.script_module = script_module
    except (ImportError, FileNotFoundError):
        print(f"Error: Could not import the script '{script_path}'.")
        print("Make sure it is in the same directory as test_harness.py and does not contain syntax errors.")
        sys.exit(1)

    # Run unittest with its own arguments. We must add the program name back to argv.
    unittest.main(argv=[sys.argv[0]] + remaining_argv, verbosity=2)
