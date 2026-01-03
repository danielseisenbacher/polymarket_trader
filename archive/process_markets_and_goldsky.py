import duckdb
from datetime import datetime, timedelta


def get_min_max_timestamp_from_transactions(con):
    """Get the date range from transactions table"""
    date_info = con.execute("""
                            SELECT MIN(timestamp) as min_date,
                                   MAX(timestamp) as max_date
                            FROM transactions
                            """).fetchone()

    min_date, max_date = date_info
    print(f"Transactions data ranges from {min_date} to {max_date}")

    # Convert to datetime objects
    if isinstance(min_date, str):
        min_date = datetime.fromisoformat(min_date.replace('Z', '+00:00'))
    if isinstance(max_date, str):
        max_date = datetime.fromisoformat(max_date.replace('Z', '+00:00'))

    return min_date, max_date


def get_last_processed_timestamp(con):
    """Get the last timestamp from processed_trades if it exists"""
    try:
        # Check if table exists
        table_exists = con.execute("""
                                   SELECT COUNT(*)
                                   FROM information_schema.tables
                                   WHERE table_name = 'processed_trades'
                                   """).fetchone()[0]

        if table_exists == 0:
            print("Table processed_trades does not exist yet")
            return None

        # Get last timestamp
        last_ts = con.execute("""
                              SELECT MAX(timestamp)
                              FROM processed_trades
                              """).fetchone()[0]

        if last_ts:
            print(f"Last processed timestamp: {last_ts}")
            if isinstance(last_ts, str):
                last_ts = datetime.fromisoformat(last_ts.replace('Z', '+00:00'))
            return last_ts
        else:
            print("Table exists but is empty")
            return None

    except Exception as e:
        print(f"Could not get last timestamp: {e}")
        return None


def build_batches(min_date, max_date, batch_delta_seconds=1):
    """Generate batches of the specified week size"""
    batches = []
    current_date = min_date

    while current_date < max_date:
        next_date = current_date + batch_delta_seconds
        if next_date > max_date:
            next_date = max_date
        batches.append((
            current_date,
            next_date
        ))
        current_date = next_date

    print(f"Generated {len(batches)} batches of {batch_delta_seconds} week(s) each")
    return batches


def get_query_template():
    """Return the SQL query template for processing trades"""
    return """
           -- Case 1: makerAssetId is non-USDC, join on token1
            SELECT 
                t.timestamp,
                m.id as market_id,
                m.question,
                m.answer1,
                m.answer2,
                t.maker,
                t.taker,
                'token1' as nonusdc_side,
                'SELL' as maker_direction,
                'BUY' as taker_direction,
                (t.makerAmountFilled / t.takerAmountFilled)::DOUBLE as price,
                t.takerAmountFilled / 1000000.0 as usd_amount,
                t.makerAmountFilled / 1000000.0 as token_amount,
                t.transactionHash,
                m.resolved_outcome,
                CASE 
                    WHEN m.resolved_outcome = 0 THEN NULL
                    WHEN m.resolved_outcome = 1 THEN TRUE
                    ELSE FALSE
                END as maker_won,
                CASE 
                    WHEN m.resolved_outcome = 0 THEN NULL
                    WHEN m.resolved_outcome = 1 THEN FALSE
                    ELSE TRUE
                END as taker_won
            FROM transactions t
            JOIN markets m ON m.token1 = t.makerAssetId
            WHERE t.makerAssetId != '0' AND t.takerAssetId = '0'
            
            UNION ALL
            
            -- Case 2: makerAssetId is non-USDC, join on token2
            SELECT 
                t.timestamp,
                m.id as market_id,
                m.question,
                m.answer1,
                m.answer2,
                t.maker,
                t.taker,
                'token2' as nonusdc_side,
                'SELL' as maker_direction,
                'BUY' as taker_direction,
                (t.makerAmountFilled / t.takerAmountFilled)::DOUBLE as price,
                t.takerAmountFilled / 1000000.0 as usd_amount,
                t.makerAmountFilled / 1000000.0 as token_amount,
                t.transactionHash,
                m.resolved_outcome,
                CASE 
                    WHEN m.resolved_outcome = 0 THEN NULL
                    WHEN m.resolved_outcome = 2 THEN TRUE
                    ELSE FALSE
                END as maker_won,
                CASE 
                    WHEN m.resolved_outcome = 0 THEN NULL
                    WHEN m.resolved_outcome = 2 THEN FALSE
                    ELSE TRUE
                END as taker_won
            FROM transactions t
            JOIN markets m ON m.token2 = t.makerAssetId
            WHERE t.makerAssetId != '0' AND t.takerAssetId = '0'
            
            UNION ALL
            
            -- Case 3: takerAssetId is non-USDC, join on token1
            SELECT 
                t.timestamp,
                m.id as market_id,
                m.question,
                m.answer1,
                m.answer2,
                t.maker,
                t.taker,
                'token1' as nonusdc_side,
                'BUY' as maker_direction,
                'SELL' as taker_direction,
                (t.takerAmountFilled / t.makerAmountFilled)::DOUBLE as price,
                t.makerAmountFilled / 1000000.0 as usd_amount,
                t.takerAmountFilled / 1000000.0 as token_amount,
                t.transactionHash,
                m.resolved_outcome,
                CASE 
                    WHEN m.resolved_outcome = 0 THEN NULL
                    WHEN m.resolved_outcome = 1 THEN FALSE
                    ELSE TRUE
                END as maker_won,
                CASE 
                    WHEN m.resolved_outcome = 0 THEN NULL
                    WHEN m.resolved_outcome = 1 THEN TRUE
                    ELSE FALSE
                END as taker_won
            FROM transactions t
            JOIN markets m ON m.token1 = t.takerAssetId
            WHERE t.makerAssetId = '0' AND t.takerAssetId != '0'
            
            UNION ALL
            
            -- Case 4: takerAssetId is non-USDC, join on token2
            SELECT 
                t.timestamp,
                m.id as market_id,
                m.question,
                m.answer1,
                m.answer2,
                t.maker,
                t.taker,
                'token2' as nonusdc_side,
                'BUY' as maker_direction,
                'SELL' as taker_direction,
                (t.takerAmountFilled / t.makerAmountFilled)::DOUBLE as price,
                t.makerAmountFilled / 1000000.0 as usd_amount,
                t.takerAmountFilled / 1000000.0 as token_amount,
                t.transactionHash,
                m.resolved_outcome,
                CASE 
                    WHEN m.resolved_outcome = 0 THEN NULL
                    WHEN m.resolved_outcome = 2 THEN FALSE
                    ELSE TRUE
                END as maker_won,
                CASE 
                    WHEN m.resolved_outcome = 0 THEN NULL
                    WHEN m.resolved_outcome = 2 THEN TRUE
                    ELSE FALSE
                END as taker_won
            FROM transactions t
            JOIN markets m ON m.token2 = t.takerAssetId
            WHERE t.makerAssetId = '0' AND t.takerAssetId != '0'
                       """


def create_indexes(con):
    """Create indexes on the processed_trades table"""
    print("\nCreating indexes...")
    indexes = [
        ("idx_mat_market", "market_id"),
        ("idx_mat_timestamp", "timestamp"),
        ("idx_mat_maker", "maker"),
        ("idx_mat_taker", "taker"),
    ]

    for idx_name, column in indexes:
        try:
            con.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON processed_trades({column})")
            print(f"  ✓ Index on {column} created")
        except Exception as e:
            print(f"  ✗ Error creating index on {column}: {e}")


def process_batches(con, batches, query_template, is_new_table):
    """Process all batches, creating or appending to the table"""
    total_batches = len(batches)

    for i, (start, end) in enumerate(batches, start=1):
        print(f"[{i}/{total_batches}] Processing {start} to {end}...")

        try:
            if i == 1 and is_new_table:
                # Create table with first batch
                con.execute(f"""
                CREATE TABLE processed_trades AS
                SELECT * FROM (
                    {query_template}
                ) sub
                WHERE timestamp >= '{start}' AND timestamp < '{end}'
                """)
                print(f"    ✓ Table created with first batch")
            else:
                # Insert subsequent batches
                con.execute(f"""
                INSERT INTO processed_trades
                SELECT * FROM (
                    {query_template}
                ) sub
                WHERE timestamp >= '{start}' AND timestamp < '{end}'
                """)
                print(f"    ✓ Batch inserted")

            # Show progress
            current_count = con.execute("SELECT COUNT(*) FROM processed_trades").fetchone()[0]
            print(f"    Total rows so far: {current_count:,}")

        except Exception as e:
            print(f"    ✗ Error in batch {start} to {end}: {e}")


    # Final count
    print("\n" + "=" * 60)
    total = con.execute("SELECT COUNT(*) FROM processed_trades").fetchone()[0]
    print(f"✓ All batches complete! Total rows: {total:,}")
    print("=" * 60)


def make_processed_trades_table(con, batch_delta_seconds=500000):
    """Main function to create/update the processed_trades table"""

    # Get transaction date range
    trans_min_date, trans_max_date = get_min_max_timestamp_from_transactions(con)

    # Check if table exists and get last processed timestamp
    last_processed = get_last_processed_timestamp(con)

    # Determine starting point
    if last_processed is None:
        # Table doesn't exist or is empty, start from beginning
        start_date = trans_min_date
        is_new_table = True
        print(f"\nStarting fresh from {start_date}")
    else:
        # Table exists, resume from last timestamp
        start_date = last_processed
        is_new_table = False
        print(f"\nResuming from {start_date}")

        # Check if we're already up to date
        if start_date >= trans_max_date:
            print("✓ Already up to date! No new data to process.")
            return

    # Build batches from start_date to end
    batches = build_batches(start_date, trans_max_date, batch_delta_seconds)

    if len(batches) == 0:
        print("✓ No batches to process!")
        return

    # Get query template
    query_template = get_query_template()

    # Process all batches
    process_batches(con, batches, query_template, is_new_table)

    # Create indexes if this is a new table
    if is_new_table:
        create_indexes(con)

    print("\n✓ Done! You can now query: SELECT * FROM processed_trades")


def combine_markets_and_goldsky(updated_market_ids=None, db_path='polymarket.duckdb', batch_delta_seconds=500000):
    """
    Main entry point to comupdated_market_idsbine markets and transactions data.
    This function is resumable - it will pick up where it left off if interrupted.

    Args:
        db_path: Path to the DuckDB database file
        batch_delta_seconds: Size of each batch in weeks (default: 1)
    """
    print("=" * 60)
    print("Processing Polymarket Trades")
    print("=" * 60)

    # Connect to database
    con = duckdb.connect(db_path)
    con.execute("SET memory_limit='3GB'")
    con.execute("SET threads=2")
    con.execute("SET preserve_insertion_order=false")

    try:
        make_processed_trades_table(con, batch_delta_seconds)
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
        print("Progress has been saved. Run again to resume from where you left off.")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("Progress has been saved. Run again to resume from where you left off.")
        raise
    finally:
        con.close()
        print("\nDatabase connection closed.")


if __name__ == "__main__":
    # Run the processing
    combine_markets_and_goldsky(
        db_path='../polymarket.duckdb',
        batch_delta_seconds=500000  # Process 1 week at a time
    )