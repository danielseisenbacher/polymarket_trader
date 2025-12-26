import warnings

warnings.filterwarnings('ignore')

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import polars as pl
from poly_utils.utils import get_markets
import duckdb


def get_processed_df(df):
    """Same processing logic as before"""
    markets_df = get_markets()
    markets_df = markets_df.rename({'id': 'market_id'})

    print("Processing trades...")

    # 1) Make markets long: (market_id, side, asset_id) where side ∈ {"token1", "token2"}
    markets_long = (
        markets_df
        .select(["market_id", "token1", "token2"])
        .melt(id_vars="market_id", value_vars=["token1", "token2"],
              variable_name="side", value_name="asset_id")
    )

    # 2) Identify the non-USDC asset for each trade
    df = df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    # 3) Join to recover the market + side
    df = df.join(
        markets_long,
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )

    # 4) Label columns
    df = df.with_columns([
        pl.when(pl.col("makerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("makerAsset"),
        pl.when(pl.col("takerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("takerAsset"),
        pl.col("market_id"),
    ])

    df = df[['timestamp', 'market_id', 'maker', 'makerAsset', 'makerAmountFilled', 'taker', 'takerAsset',
             'takerAmountFilled', 'transactionHash']]

    # 5) Scale amounts
    df = df.with_columns([
        (pl.col("makerAmountFilled") / 10 ** 6).alias("makerAmountFilled"),
        (pl.col("takerAmountFilled") / 10 ** 6).alias("takerAmountFilled"),
    ])

    # 6-7) Add directions
    df = df.with_columns([
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("BUY"))
        .otherwise(pl.lit("SELL"))
        .alias("taker_direction"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("SELL"))
        .otherwise(pl.lit("BUY"))
        .alias("maker_direction"),
    ])

    # 8) Calculate price and amounts
    df = df.with_columns([
        pl.when(pl.col("makerAsset") != "USDC")
        .then(pl.col("makerAsset"))
        .otherwise(pl.col("takerAsset"))
        .alias("nonusdc_side"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("usd_amount"),

        pl.when(pl.col("takerAsset") != "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("token_amount"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled") / pl.col("makerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled") / pl.col("takerAmountFilled"))
        .cast(pl.Float64)
        .alias("price")
    ])

    # 9) Final columns
    df = df[['timestamp', 'market_id', 'maker', 'taker', 'nonusdc_side', 'maker_direction', 'taker_direction', 'price',
             'usd_amount', 'token_amount', 'transactionHash']]
    return df


def initialize_database(db_path='polymarket.duckdb'):
    """Create database and tables if they don't exist"""
    con = duckdb.connect(db_path)

    # Create processed_trades table
    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_trades (
            timestamp TIMESTAMP,
            market_id VARCHAR,
            maker VARCHAR,
            taker VARCHAR,
            nonusdc_side VARCHAR,
            maker_direction VARCHAR,
            taker_direction VARCHAR,
            price DOUBLE,
            usd_amount DOUBLE,
            token_amount DOUBLE,
            transactionHash VARCHAR,
            PRIMARY KEY (transactionHash, maker, taker)
        )
    """)

    # Create indexes for fast queries
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_timestamp 
        ON processed_trades(timestamp)
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_market_id 
        ON processed_trades(market_id)
    """)

    print("✓ Database initialized")
    return con



def get_last_processed_timestamp(con):
    """Get the timestamp of the last processed trade"""
    result = con.execute("""
        SELECT MAX(timestamp) FROM transactions
    """).fetchone()[0]

    if result:
        print(f"📍 Resuming from: {result}")
    else:
        print("⚠ No existing data - processing from beginning")

    return result


def process_live():
    db_path = 'polymarket.duckdb'

    print("=" * 60)
    print("🔄 Processing Live Trades with DuckDB")
    print("=" * 60)

    # Initialize database
    con = initialize_database(db_path)

    # Get last processed timestamp
    last_timestamp = get_last_processed_timestamp(con)

    # Find CSV path
    path = "goldsky/orderFilled.csv"

    print(f"\n📂 Reading: {path}")

    schema_overrides = {
        "takerAssetId": pl.Utf8,
        "makerAssetId": pl.Utf8,
    }

    # Process in chunks
    chunk_size = 5_000_000  # Process 1M rows at a time
    total_processed = 0

    try:
        # Create the batched reader
        reader = pl.read_csv_batched(
            path,
            batch_size=chunk_size,
            schema_overrides=schema_overrides
        )

        batch_num = 0
        # Iterate using next_batches()
        while True:
            batches = reader.next_batches(1)
            if not batches:
                break

            chunk_df = batches[0]
            batch_num += 1

            # Convert timestamp
            chunk_df = chunk_df.with_columns(
                pl.from_epoch(pl.col('timestamp'), time_unit='s').alias('timestamp')
            )

            # Filter to only new rows
            if last_timestamp:
                chunk_df = chunk_df.filter(pl.col('timestamp') > last_timestamp)

            if len(chunk_df) == 0:
                print(f"⏭️  Batch {batch_num}: Skipped (no new data)")
                continue

            print(f"\n⚙️  Batch {batch_num}: Processing {len(chunk_df):,} rows...")

            # Process chunk
            processed_chunk = get_processed_df(chunk_df)

            # Get count before insert
            count_before = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]

            # Insert into database (DuckDB handles duplicates with PRIMARY KEY)
            con.execute("""
                INSERT INTO transactions 
                SELECT * FROM processed_chunk
                ON CONFLICT DO NOTHING
            """)

            # Get count after insert to calculate rows inserted
            count_after = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            rows_inserted = count_after - count_before
            total_processed += rows_inserted

            print(f"✓ Batch {batch_num}: Inserted {rows_inserted:,} new rows (total: {count_after:,})")

    except Exception as e:
        print(f"❌ Error during processing: {e}")
        con.close()
        raise

    # Final statistics
    total_rows = con.execute("SELECT COUNT(*) FROM processed_trades").fetchone()[0]

    print("\n" + "=" * 60)
    print("✅ Processing complete!")
    print(f"📊 Total rows in database: {total_rows:,}")
    print(f"📈 New rows added: {total_processed:,}")
    print("=" * 60)

    con.close()

def example_queries():
    """Example queries you can now run efficiently"""
    con = duckdb.connect('polymarket.duckdb')

    print("\n" + "=" * 60)
    print("📊 Example Queries")
    print("=" * 60)

    # 1. Recent trades
    print("\n1. Last 5 trades:")
    result = con.execute("""
        SELECT timestamp, market_id, taker_direction, price, usd_amount
        FROM processed_trades
        ORDER BY timestamp DESC
        LIMIT 5
    """).pl()
    print(result)

    # 2. Volume by market (last 24 hours)
    print("\n2. Top markets by volume (last 24 hours):")
    result = con.execute("""
        SELECT 
            market_id,
            COUNT(*) as trade_count,
            SUM(usd_amount) as total_volume,
            AVG(price) as avg_price
        FROM processed_trades
        WHERE timestamp > NOW() - INTERVAL '24 hours'
        GROUP BY market_id
        ORDER BY total_volume DESC
        LIMIT 10
    """).pl()
    print(result)

    # 3. Specific trader's activity
    print("\n3. Find a specific trader's recent trades:")
    result = con.execute("""
        SELECT timestamp, market_id, taker_direction, usd_amount
        FROM processed_trades
        WHERE taker = '0x...' -- replace with actual address
        ORDER BY timestamp DESC
        LIMIT 5
    """).pl()
    print(result)

    con.close()


if __name__ == "__main__":
    process_live()

    # Uncomment to see example queries:
    example_queries()