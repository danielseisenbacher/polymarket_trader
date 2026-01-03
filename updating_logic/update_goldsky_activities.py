import os
from pathlib import Path
import sys
import csv
from concurrent.futures import ThreadPoolExecutor
from math import floor
import shutil

import pandas as pd
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from flatten_json import flatten
from datetime import datetime, timezone
import time
import duckdb
import subprocess

# ------------------------------------------------------------
# CSV Functions
# ------------------------------------------------------------

COLUMNS_TO_SAVE = ['timestamp', 'redeemer', 'condition', 'indexSets', 'payout', 'id']
SCRIPT_STARTING_TIME = int(datetime.now().timestamp())


def create_worker_files(nr_workers: int) -> list:
    """Create worker CSV files with headers and return list of file paths"""

    # Use pathlib for cleaner path handling
    worker_dir = Path("workers_activity")
    worker_dir.mkdir(exist_ok=True)

    # Find the next available worker number
    existing = [int(f.stem.split('_')[1]) for f in worker_dir.glob("worker_*.csv")]
    start_num = max(existing, default=-1) + 1

    workers = []
    for i in range(nr_workers):
        path = worker_dir / f"worker_{start_num + i}.csv"

        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(COLUMNS_TO_SAVE)

        workers.append(str(path))

    return workers


def get_multi_scrape_timestamps(db_last_sync: tuple):

    timestamps = []
    workers_dir = "workers_activity"
    if os.path.exists(workers_dir):
        workers = [os.path.join(workers_dir, worker) for worker in os.listdir(workers_dir)]
        for worker in workers:
            worker_timestamp = get_first_and_last_timestamp(worker)
            timestamps.append(worker_timestamp)

    timestamps.append(db_last_sync)
    return timestamps


def get_first_and_last_timestamp(cache_file: str) -> tuple:
    """Get the first and last timestamp from cache file, or (0,0) if file doesn't exist or is empty"""

    if not os.path.isfile(cache_file):
        raise Exception(f"No existing file found: {cache_file}")

    try:
        # Get header to find timestamp column index
        header_result = subprocess.run(['head', '-n', '1', cache_file], capture_output=True, text=True, check=True)
        headers = header_result.stdout.strip().split(',')

        if 'timestamp' not in headers:
            print("Timestamp column not found in headers")
            sys.exit()
            return 0, 0

        timestamp_index = headers.index('timestamp')

        # Get the last line
        last_result = subprocess.run(['tail', '-n', '1', cache_file], capture_output=True, text=True, check=True)
        last_line = last_result.stdout.strip()

        # Check if file only contains headers (last line equals header line)
        if not last_line or last_line.split(",") == headers:
            return 0, 0

        # Parse last timestamp
        last_values = last_line.split(',')
        if len(last_values) <= timestamp_index:
            print("Invalid last line format")
            return 0, 0

        last_timestamp = int(last_values[timestamp_index])
        readable_last = datetime.fromtimestamp(last_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


        # Get the first data line (line 2, after header)
        first_result = subprocess.run(['sed', '-n', '2p', cache_file], capture_output=True, text=True, check=True)
        first_line = first_result.stdout.strip()

        if not first_line:
            print("No data rows found after header")
            return 0, 0

        first_values = first_line.split(',')
        if len(first_values) <= timestamp_index:
            print("Invalid first line format")
            return 0, 0

        first_timestamp = int(first_values[timestamp_index])
        readable_first = datetime.fromtimestamp(first_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

        return first_timestamp, last_timestamp

    except Exception as e:
        print(f"An Exception occurred in the function 'get_first_and_last_timestamp':\n{e}")
        raise


def redundancy_fixer(timestamps):
    timestamps_wo_overlapse = []

    for begin, end in sorted(timestamps):
        if timestamps_wo_overlapse and timestamps_wo_overlapse[-1][1] >= begin - 120:
            timestamps_wo_overlapse[-1][1] = max(timestamps_wo_overlapse[-1][1], end)
        else:
            timestamps_wo_overlapse.append([begin, end])

    return timestamps_wo_overlapse


def get_missing_intervals(timestamps_wo_overlap: list) -> list:
    timestamps_wo_overlap.sort()
    missing_intervals = []

    for i, (start, end) in enumerate(timestamps_wo_overlap):
        if i + 1 < len(timestamps_wo_overlap):
            next_start = timestamps_wo_overlap[i + 1][0]
            missing_intervals.append([end + 1, next_start - 1])
        else:
            if end+1 < SCRIPT_STARTING_TIME:
                missing_intervals.append([end + 1, SCRIPT_STARTING_TIME])

    return missing_inresulttervals


def balance_load(missing_intervals: list, nr_workers: int):
    """Balance load across workers and return list of work assignments"""
    all_assignments = []

    for interval in missing_intervals:
        interval_range = interval[1] - interval[0]
        worker_range = floor(interval_range / nr_workers)

        workers = create_worker_files(nr_workers)

        subset_assignments = []
        for count, worker in enumerate(workers):
            start = interval[0] + (count * worker_range)
            end = interval[0] + ((count + 1) * worker_range)

            # Last worker gets any remainder
            if count == nr_workers - 1:
                end = interval[1]

            subset_assignments.append((start, end, worker))

        all_assignments.append(subset_assignments)

        print(f"Load balanced to {nr_workers} workers for interval [{interval[0]}, {interval[1]}]")

    return all_assignments


def run_scrape_worker(start, end, worker_file):
    """Wrapper function for running scrape in a thread"""
    return scrape(start, end, worker_file)


def scrape(timestamp_start, timestamp_end, output_file, at_once=1000):
    QUERY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/activity-subgraph/0.0.4/gn"
    MAX_RETRIES = 6

    count = 0
    total_records = 0

    last_timestamp = int(timestamp_start)
    last_id = ""

    print(f"{output_file} - Starting scrape from {timestamp_start} to {timestamp_end}")

    while True:
        # Always use simple timestamp filter
        where_clause = f'''
          where: {{
            timestamp_gte: "{last_timestamp}"
            timestamp_lte: "{timestamp_end}"
          }}
        '''

        q_string = f"""
        query MyQuery {{
          redemptions(
            first: {at_once}
            orderBy: timestamp
            orderDirection: asc
            {where_clause}
          ) {{
            id
            timestamp
            redeemer
            condition
            indexSets
            payout
          }}
        }}
        """

        query = gql(q_string)
        transport = RequestsHTTPTransport(
            url=QUERY_URL,
            verify=True,
            retries=3,
            timeout=10
        )
        client = Client(transport=transport)

        res = None
        for attempt in range(MAX_RETRIES):
            try:
                res = client.execute(query)
                break
            except Exception as e:
                wait_time = 2 ** attempt
                if attempt < MAX_RETRIES - 1:
                    print(f"{output_file} - Query error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                    print(f"{output_file} - Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise

        events = res.get("redemptions", [])
        if not events:
            print(f"{output_file} - No more data")
            break

        # Process events to handle indexSets array
        processed_events = []
        for event in events:
            flat_event = flatten(event)
            # Convert indexSets list to JSON string if it exists
            if 'indexSets' in event:
                flat_event['indexSets'] = "-".join(str(index) for index in event['indexSets'])
            processed_events.append(flat_event)

        df = pd.DataFrame(processed_events)

        # Ensure deterministic ordering
        df = df.sort_values(["timestamp", "id"], ascending=[True, True]).reset_index(drop=True)

        # Filter out records we've already seen (when paginating within same timestamp)
        if last_id != "":
            # Keep only records that are strictly after our cursor
            df = df[
                (df["timestamp"].astype(int) > last_timestamp) |
                ((df["timestamp"].astype(int) == last_timestamp) & (df["id"] > last_id))
                ].reset_index(drop=True)

        if len(df) == 0:
            # All records in this batch were duplicates, advance timestamp
            last_timestamp += 1
            last_id = ""
            continue

        # Update cursor to the last record
        last_timestamp = int(df.iloc[-1]["timestamp"])
        last_id = df.iloc[-1]["id"]

        count += 1
        total_records += len(df)

        print(
            f"{output_file:<30} "
            f"Batch {count:<5} "
            f"Records: {len(df):<5} "
            f"To Go: {timestamp_end - last_timestamp}"
        )

        df_to_save = df[COLUMNS_TO_SAVE].copy()

        if os.path.isfile(output_file):
            df_to_save.to_csv(output_file, index=False, mode="a", header=False)
        else:
            df_to_save.to_csv(output_file, index=False)

        # Stop when we got fewer records than we filtered to
        # (meaning the API returned fewer than requested)
        if len(events) < at_once:
            break

    print(f"{output_file} - Finished! Total new records: {total_records}")
    return total_records

def sanity_check(db_last_sync, last_time_missing_intervals):

    timestamps = get_multi_scrape_timestamps(db_last_sync)

    timestamps_wo_overlapse = redundancy_fixer(timestamps)

    # invert the covered times - to get the missing times
    missing_intervals = get_missing_intervals(timestamps_wo_overlapse)

    if missing_intervals == last_time_missing_intervals:
        print("-" * 50)
        print("RERUNNING DID NOT HELP")
        print("skipping..")
        print("-" * 50)
        pass

    elif not len(missing_intervals) == 0:
        print("-"*50)
        print("SANITY CHECK NOT SUCCESSFUL")
        print(f"Multiple Intervals Found: {len(missing_intervals)}")
        print("-" * 50)
        return "multiple_missing_intervals"

    print("-" * 50)
    print("SANITY CHECK SUCCESSFUL")
    print("-" * 50)
    return True


# ------------------------------------------------------------
# DB Functions
# ------------------------------------------------------------

def create_table(duck_db_path):
    """Create the redemptions table if it does not already exist."""
    con = duckdb.connect(duck_db_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS redemptions (
            "timestamp" BIGINT,
            redeemer VARCHAR,
            "condition" VARCHAR,
            indexSets VARCHAR,
            payout BIGINT,
            id VARCHAR
        );
    """)

    con.close()
    print("DuckDB table 'redemptions' ready.")


def get_latest_timestamp_from_duckdb(duck_db_path):
    """Return the most recently synced timestamp."""
    con = duckdb.connect(duck_db_path)

    result = con.execute("""
            SELECT timestamp FROM redemptions ORDER BY timestamp DESC LIMIT 1;
        """).fetchone()

    con.close()

    if result is None:
        print("No Sync of redemptions has been conducted...")
        return 0, 1748728800  # No data yet - starting from a reasonable timestamp
    else:
        last_sync = result[0]
        print("Last Sync was conducted:", datetime.fromtimestamp(last_sync))

    return 0, last_sync  # Extract timestamp


def write_workers_to_duckdb(duck_db_path="polymarket.duckdb"):
    print("+"*50)
    print("Start inserting data from the worker files")
    print("+" * 50)

    con = duckdb.connect(duck_db_path)
    chunk_size = 10000000

    workers_dir = Path("workers_activity").absolute()

    if not workers_dir.exists():
        print("No workers directory found!")
        return

    for count, worker in enumerate(os.listdir(workers_dir)):
        print("+" * 50)
        print(f"{count+1}/{len(os.listdir(workers_dir))} Processing {worker}...")
        print("+" * 50)

        worker_path = workers_dir / worker

        # Get total number of rows (excluding header)
        result = subprocess.run(['wc', '-l', str(worker_path)], capture_output=True, text=True)
        total_rows = int(result.stdout.split()[0]) - 1

        print(f"Total rows to load: {total_rows:,}")

        offset = 0
        while offset < total_rows:

            rows_inserted = con.execute(f"""
                INSERT INTO redemptions 
                SELECT * FROM read_csv('{worker_path}', header=true, all_varchar=true, delim=',', max_line_size=10000000, escape='', quote='', encoding = 'utf-8', comment='', parallel=false, strict_mode=false) 
                LIMIT {chunk_size} OFFSET {offset}
            """).fetchall()[0][0]

            offset += chunk_size
            progress = min(100, (offset / total_rows) * 100)
            print(f"Loaded {min(offset, total_rows):,}/{total_rows:,} rows ({progress:.1f}%)")

            # Break when no more rows
            if rows_inserted == 0:
                break

        print(f"✓ Completed {worker}\n")


def cleanup_worker_files():
    """Delete all worker files after loading is complete"""
    if os.path.exists("workers_activity"):
        file_count = len(os.listdir("workers_activity"))
        shutil.rmtree("workers_activity")
        print(f"\n{'=' * 50}")
        print(f"✓ Cleaned up {file_count} worker files")
        print(f"{'=' * 50}\n")

# ------------------------------------------------------------
# Main logic
# ------------------------------------------------------------

def update_goldsky_activities_to_duckdb(duck_db_path="polymarket.duckdb", last_time_missing_intervals=None):

    # Create workers directory if it doesn't exist
    workers_dir = Path("workers_activity").absolute()
    workers_dir.mkdir(exist_ok=True)

    NR_WORKERS = 120

    create_table(duck_db_path)

    # get the latest timestamp from duckdb
    last_sync_timestamp = get_latest_timestamp_from_duckdb(duck_db_path)

    # define the times that are already covered by the workers - should be nothing in the beginning
    timestamps = get_multi_scrape_timestamps(db_last_sync=last_sync_timestamp)
    print(f"Timestamps:\n{timestamps}\n")

    # fix redundancies that come from the csv
    timestamps_wo_overlaps = redundancy_fixer(timestamps)
    print(f"Timestamps without Overlaps:\n{timestamps_wo_overlaps}\n")

    # invert the covered times - to get the missing times
    missing_intervals = get_missing_intervals(timestamps_wo_overlaps)
    print(f"Missing intervals:\n{missing_intervals}\n")

    # split the workload
    assignments = balance_load(missing_intervals, NR_WORKERS)
    print(f"assignments:\n{assignments}\n")

    print(f"\n{'=' * 60}")
    print(f"Starting parallel scrape with {len(assignments)} work assignments across {NR_WORKERS} workers")
    print(f"{'=' * 60}\n")

    for count, assignment in enumerate(assignments):
        # Run workers in parallel
        with ThreadPoolExecutor(max_workers=NR_WORKERS) as executor:
            futures = []
            for start, end, worker_file in assignment:
                future = executor.submit(run_scrape_worker, start, end, worker_file)
                futures.append((future, worker_file))

            # Wait for all to complete
            total_records = 0
            for future, worker_file in futures:
                try:
                    records = future.result()
                    total_records += records
                    print(f"\n✓ Worker {worker_file} completed successfully")
                except Exception as e:
                    print(f"\n✗ Worker {worker_file} failed with error: {e}")

        print(f"\n{'=' * 60}")
        print(f"ASSIGNMENT {count+1} OUT OF {len(assignments)} DONE:\nAll workers finished! Total records scraped: {total_records}")
        print(f"{'=' * 60}\n")

    print(f"\n{'=' * 60}")
    print(f"ALL ASSIGNMENTS DONE: checking sanity...")
    print(f"{'=' * 60}\n")

    sanity_result = sanity_check(last_sync_timestamp, last_time_missing_intervals)

    if sanity_result == "multiple_missing_intervals":
        print("Rerun missing intervals...")
        update_goldsky_activities_to_duckdb(duck_db_path=duck_db_path, last_time_missing_intervals=missing_intervals)

    else:
        try:
            write_workers_to_duckdb(duck_db_path)
            cleanup_worker_files()

        except Exception as e:
            print("! "*50)
            print("ERROR OCCURRED:")
            print(e)
            print("! " * 50)


if __name__ == "__main__":
    update_goldsky_activities_to_duckdb()
    #write_workers_to_duckdb()  # Uncomment if you just want to write existing worker files