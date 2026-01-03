import os
import csv
import ast

import pandas as pd
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from concurrent.futures import ThreadPoolExecutor
import time
import duckdb
import subprocess

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

COLUMNS_TO_SAVE = ['id', 'user', 'asset_id', 'condition_id', 'balance', 'payouts']


# ------------------------------------------------------------
# Goldsky Scraping
# ------------------------------------------------------------

def balance_load(small=True):
    """Balance load across workers and return list of work assignments"""

    combinations = []
    hexadecimal = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"]

    for i in hexadecimal:
        if small:
            combinations.append(f"0x{i}")
            continue

        for j in hexadecimal:
            combinations.append(f"0x{i}{j}")

    return combinations


def scrape(task_string, at_once=1000):
    """Scrape ALL user position_workers"""
    QUERY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/positions-subgraph/0.0.7/gn"
    MAX_RETRIES = 10
    output_file = f"position_workers/{task_string}.csv"

    count = 0
    total_records = 0
    last_id = ""

    print(f"Output file: {output_file}")
    print("=" * 60)

    # Create/clear the output file with headers
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS_TO_SAVE)

    while True:
        where_clause = (f'where: {{ id_starts_with: "{task_string}"'
                        f'          id_gt: "{last_id}" }}') if last_id else ""

        q_string = f"""
        query MyQuery {{
          userBalances(
            first: {at_once}
            orderBy: id
            orderDirection: asc
            {where_clause}
          ) {{
            id
            user
            asset {{
              id
              condition {{
                id
                payouts
              }}
            }}
            balance
          }}
        }}
        """

        query = gql(q_string)
        transport = RequestsHTTPTransport(
            url=QUERY_URL,
            verify=True,
            retries=5,
            timeout=30
        )
        client = Client(transport=transport)

        res = None
        for attempt in range(MAX_RETRIES):
            try:
                res = client.execute(query)
                break
            except Exception as e:
                wait_time = min(60, 2 ** attempt)
                if attempt < MAX_RETRIES - 1:
                    error_msg = str(e)
                    print(f"Query error (attempt {attempt + 1}/{MAX_RETRIES}): {error_msg[:100]}")
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise

        balances = res.get("userBalances", [])
        if not balances:
            print(f"{task_string} - ✓ scraping complete!")
            break

        # Process balances into flat structure
        processed_balances = []
        for balance in balances:
            processed_balances.append({
                'id': balance['id'],
                'user': balance['user'],
                'asset_id': balance['asset']['id'],
                'condition_id': balance['asset']['condition']['id'],
                'balance': balance['balance'],
                'payouts': str(balance['asset']['condition']['payouts']) if balance['asset']['condition'][
                    'payouts'] else None
            })

        df = pd.DataFrame(processed_balances)

        last_id = balances[-1]['id']
        count += 1
        total_records += len(df)

        print(
            f"Worker: {task_string} | "
            f"Batch {count:<5} | "
            f"Records: {len(df):<5} | "
            f"Total: {total_records:>10,} | "
            f"Last ID: {last_id}"
        )

        # Append to CSV
        df[COLUMNS_TO_SAVE].to_csv(output_file, index=False, mode="a", header=False, quoting=csv.QUOTE_NONNUMERIC)

        if len(balances) < at_once:
            break

    print("=" * 60)
    print(f"{task_string}✓ Scraping complete! Total records: {total_records:,}")
    print("=" * 60)
    return total_records


def run_scrape_worker(task_string):
    """Wrapper function for running scrape in a thread"""
    return scrape(task_string)


# ------------------------------------------------------------
# Helper Function
# ------------------------------------------------------------

def fix_payout_column(csv_path: str, output_path: str = None):
    df = pd.read_csv(csv_path)

    def normalize_payout(val):
        if pd.isna(val):
            return val
        if isinstance(val, str) and val.startswith('[') and val.endswith(']'):
            try:
                parsed = ast.literal_eval(val)
                if isinstance(parsed, list):
                    return "-".join(str(x) for x in parsed)
            except Exception as e:
                print(e)
                pass
        return val

    df["payouts"] = df["payouts"].apply(normalize_payout)

    if output_path is None:
        df.to_csv(csv_path, index=False)
    else:
        df.to_csv(output_path, index=False)


# ------------------------------------------------------------
# DuckDB logic
# ------------------------------------------------------------
def load_to_duckdb(duck_db_path="polymarket.duckdb"):
    """Load CSV into DuckDB"""
    print("\n" + "=" * 60)
    print("Loading data into DuckDB...")
    print("=" * 60)


    for csv_file in os.listdir("../workers/position_workers"):

        con = duckdb.connect(duck_db_path)
        file_path = os.path.join("../workers/position_workers", csv_file)

        print(f"{csv_file} | Fixing payout column...")
        fix_payout_column(file_path)

        # Get row count
        result = subprocess.run(['wc', '-l', file_path], capture_output=True, text=True)
        total_rows = int(result.stdout.split()[0]) - 1  # Exclude header
        print(f"{csv_file} | Total rows to load: {total_rows:,}")

        # Create table
        con.execute("""
                    CREATE TABLE IF NOT EXISTS positions
                    (
                        id
                        VARCHAR
                        PRIMARY
                        KEY,
                        user
                        VARCHAR,
                        asset_id
                        VARCHAR,
                        condition_id
                        VARCHAR,
                        balance
                        VARCHAR,
                        payouts
                        VARCHAR
                    );
                    """)

        # Load data
        print("Inserting new data...")
        chunk_size = 10000000
        offset = 0

        while offset < total_rows:
            rows_inserted = con.execute(f"""
                INSERT OR IGNORE INTO positions 
                SELECT * FROM read_csv(
                    '{file_path}', 
                    header=true, 
                    all_varchar=true, 
                    delim=',',
                    quote='"',
                    escape='"',
                    max_line_size=10000000, 
                    encoding='utf-8', 
                    parallel=false
                )
                LIMIT {chunk_size} OFFSET {offset}
            """).fetchall()[0][0]

            offset += chunk_size
            progress = min(100, (offset / total_rows) * 100)
            print(f"Progress: {min(offset, total_rows):>10,} / {total_rows:,} rows ({progress:.1f}%)\n")

            if rows_inserted == 0:
                break

            con.close()

    con = duckdb.connect(duck_db_path)
    # Show statistics
    stats = con.execute("""
                        SELECT COUNT(*)                                                              as total_positions,
                               COUNT(DISTINCT user)                                                  as unique_users,
                               COUNT(DISTINCT condition_id)                                          as unique_markets,
                               COUNT(CASE WHEN payouts IS NOT NULL AND payouts != 'None' THEN 1 END) as resolved_positions
                        FROM positions
                        """).fetchone()

    print("\n" + "=" * 60)
    print("Database Statistics:")
    print("=" * 60)
    print(f"Total Positions:      {stats[0]:>15,}")
    print(f"Unique Users:         {stats[1]:>15,}")
    print(f"Unique Markets:       {stats[2]:>15,}")
    print(f"Resolved Positions:   {stats[3]:>15,}")
    print("=" * 60)
    print(f"✓ Data loaded into {duck_db_path}")
    con.close()


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def update_goldsky_positions(scrape=True):
    """Main function to scrape and load all position_workers"""

    if scrape:
        workers = balance_load()

        with ThreadPoolExecutor(max_workers=len(workers)) as executor:
            for task_string in workers:
                executor.submit(run_scrape_worker, task_string)

    # Load into DuckDB
    load_to_duckdb()

    print("\n✓ All done!")


if __name__ == "__main__":
    update_goldsky_positions(scrape=False)