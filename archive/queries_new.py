import sys
import time
import os
import warnings
import pickle
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
from datetime import datetime

# Global database connection for performance
DB_CONNECTION = None


def get_db_connection(duck_db_path: str = "polymarket.duckdb"):
    global DB_CONNECTION
    if DB_CONNECTION is None:
        DB_CONNECTION = duckdb.connect(duck_db_path)
    return DB_CONNECTION


def create_indexes(duck_db_path: str = "polymarket.duckdb"):
    """Create indexes for faster queries - SAFER VERSION"""
    print("Creating database indexes for optimization...")

    indexes = [
        ("idx_transactions_maker", "transactions", "maker"),
        ("idx_redemptions_redeemer", "redemptions", "redeemer"),
        ("idx_markets_condition", "markets", "condition_id"),
        ("idx_redemptions_condition", "redemptions", "condition"),
        ("idx_transactions_taker", "transactions", "taker"),
        ("idx_transactions_maker_asset", "transactions", "makerAssetId"),
        ("idx_transactions_taker_asset", "transactions", "takerAssetId"),
        ("idx_markets_token1", "markets", "token1"),
        ("idx_markets_token2", "markets", "token2"),
    ]

    con = duckdb.connect(duck_db_path)

    # Set memory limit and thread count
    con.execute("SET memory_limit='4GB'")  # Adjust based on your system
    con.execute("SET threads=2")  # Limit parallelism

    for idx_name, table, column in indexes:
        try:
            print(f"Creating index {idx_name} on {table}({column})...", flush=True)
            con.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column})")
            print(f"  ✓ {idx_name} created", flush=True)
        except Exception as e:
            print(f"  ✗ Failed to create {idx_name}: {e}")

    con.close()
    print("Index creation complete!")


def create_materialized_tables_low_memory(duck_db_path: str = "polymarket.duckdb"):
    """
    Creates materialized table optimized for LOW MEMORY (8GB RAM).
    Processes markets in small batches to avoid memory issues.
    Expected time: 1-3 hours for 177M transactions.
    """
    print("=" * 70)
    print("Creating materialized tables (LOW MEMORY MODE - 8GB RAM)")
    print("=" * 70)

    con = duckdb.connect(duck_db_path)

    # CRITICAL: Conservative settings for 8GB RAM
    print("\n⚙️  Configuring DuckDB for low memory...")
    con.execute("SET memory_limit='8GB'")  # Use only 50% of RAM
    con.execute("SET threads=8")  # Reduce parallelism
    con.execute("SET temp_directory='/tmp/duckdb_temp'")
    con.execute("SET max_memory='8GB'")
    print("  ✓ Conservative settings applied (8GB limit)")

    # Get statistics
    print("\n📊 Analyzing source tables...")
    t_count = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    m_count = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    print(f"  Transactions: {t_count:,}")
    print(f"  Markets: {m_count:,}")
    print(f"  ⚠️  With 8GB RAM, expect 1-3 hours for completion")

    # Create indexes on source tables
    print("\n🔍 Ensuring source indexes exist...")
    indexes = [
        ("transactions.makerAssetId",
         "CREATE INDEX IF NOT EXISTS idx_transactions_maker_asset ON transactions(makerAssetId)"),
        ("transactions.takerAssetId",
         "CREATE INDEX IF NOT EXISTS idx_transactions_taker_asset ON transactions(takerAssetId)"),
        ("markets.token1", "CREATE INDEX IF NOT EXISTS idx_markets_token1 ON markets(token1)"),
        ("markets.token2", "CREATE INDEX IF NOT EXISTS idx_markets_token2 ON markets(token2)"),
    ]
    for desc, query in indexes:
        idx_start = time.time()
        con.execute(query)
        print(f"  ✓ {desc} ({time.time() - idx_start:.1f}s)")

    # Drop old table
    print("\n🗑️  Dropping old table...")
    con.execute("DROP TABLE IF EXISTS transactions_enriched")

    # Get market IDs to process in small batches
    print("\n📋 Getting markets for batch processing...")
    markets = con.execute("""
                          SELECT condition_id, token1, token2
                          FROM markets
                          WHERE token1 IS NOT NULL
                            AND token2 IS NOT NULL
                          ORDER BY condition_id
                          """).fetchall()
    print(f"  Found {len(markets):,} markets to process")

    # Process in SMALL batches (critical for low memory)
    batch_size = 1000  # Small batches to avoid memory issues
    total_batches = (len(markets) - 1) // batch_size + 1
    total_rows = 0
    overall_start = time.time()

    print(f"\n🚀 Processing {total_batches} batches of {batch_size} markets each...")
    print(f"   Progress updates every batch (~30-60s per batch)")
    print("=" * 70)

    for batch_num in range(total_batches):
        batch_start = time.time()
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(markets))
        batch_markets = markets[start_idx:end_idx]

        # Extract tokens from this batch
        batch_tokens = set()
        for _, token1, token2 in batch_markets:
            if token1 and token1 != '0':
                batch_tokens.add(token1)
            if token2 and token2 != '0':
                batch_tokens.add(token2)

        batch_tokens = list(batch_tokens)

        if not batch_tokens:
            continue

        # Create placeholders
        placeholders = ','.join(['?' for _ in batch_tokens])

        try:
            if batch_num == 0:
                # First batch: CREATE TABLE
                con.execute(f"""
                    CREATE TABLE transactions_enriched AS
                    SELECT 
                        t.*,
                        COALESCE(
                            CASE WHEN t.makerAssetId = m.token1 THEN m.token1 END,
                            CASE WHEN t.makerAssetId = m.token2 THEN m.token2 END,
                            CASE WHEN t.takerAssetId = m.token1 THEN m.token1 END,
                            CASE WHEN t.takerAssetId = m.token2 THEN m.token2 END
                        ) as matched_token,
                        COALESCE(
                            CASE WHEN t.makerAssetId = m.token1 THEN m.answer1 END,
                            CASE WHEN t.makerAssetId = m.token2 THEN m.answer2 END,
                            CASE WHEN t.takerAssetId = m.token1 THEN m.answer1 END,
                            CASE WHEN t.takerAssetId = m.token2 THEN m.answer2 END
                        ) as traded_answer,
                        m.question,
                        m.closedTime,
                        m.condition_id,
                        m.resolved_outcome,
                        m.answer1,
                        m.answer2,
                        m.token1,
                        m.token2
                    FROM transactions t
                    INNER JOIN markets m 
                        ON (t.makerAssetId IN ({placeholders}) OR t.takerAssetId IN ({placeholders}))
                        AND (t.makerAssetId IN (m.token1, m.token2) OR t.takerAssetId IN (m.token1, m.token2))
                    WHERE (t.makerAssetId != '0' OR t.takerAssetId != '0')
                """, batch_tokens + batch_tokens)
            else:
                # Subsequent batches: INSERT
                con.execute(f"""
                    INSERT INTO transactions_enriched
                    SELECT 
                        t.*,
                        COALESCE(
                            CASE WHEN t.makerAssetId = m.token1 THEN m.token1 END,
                            CASE WHEN t.makerAssetId = m.token2 THEN m.token2 END,
                            CASE WHEN t.takerAssetId = m.token1 THEN m.token1 END,
                            CASE WHEN t.takerAssetId = m.token2 THEN m.token2 END
                        ) as matched_token,
                        COALESCE(
                            CASE WHEN t.makerAssetId = m.token1 THEN m.answer1 END,
                            CASE WHEN t.makerAssetId = m.token2 THEN m.answer2 END,
                            CASE WHEN t.takerAssetId = m.token1 THEN m.answer1 END,
                            CASE WHEN t.takerAssetId = m.token2 THEN m.answer2 END
                        ) as traded_answer,
                        m.question,
                        m.closedTime,
                        m.condition_id,
                        m.resolved_outcome,
                        m.answer1,
                        m.answer2,
                        m.token1,
                        m.token2
                    FROM transactions t
                    INNER JOIN markets m 
                        ON (t.makerAssetId IN ({placeholders}) OR t.takerAssetId IN ({placeholders}))
                        AND (t.makerAssetId IN (m.token1, m.token2) OR t.takerAssetId IN (m.token1, m.token2))
                    WHERE (t.makerAssetId != '0' OR t.takerAssetId != '0')
                """, batch_tokens + batch_tokens)

            # Get progress
            current_count = con.execute("SELECT COUNT(*) FROM transactions_enriched").fetchone()[0]
            batch_rows = current_count - total_rows
            total_rows = current_count

            batch_time = time.time() - batch_start
            elapsed = time.time() - overall_start

            # Calculate ETA
            progress_pct = ((batch_num + 1) / total_batches) * 100
            avg_time = elapsed / (batch_num + 1)
            eta_seconds = avg_time * (total_batches - batch_num - 1)

            # Progress bar
            bar_width = 30
            filled = int(bar_width * (batch_num + 1) / total_batches)
            bar = '█' * filled + '░' * (bar_width - filled)

            print(f"[{bar}] {progress_pct:5.1f}% | "
                  f"Batch {batch_num + 1}/{total_batches} | "
                  f"+{batch_rows:,} rows ({batch_time:.1f}s) | "
                  f"Total: {total_rows:,} | "
                  f"ETA: {eta_seconds / 60:.0f}m")

        except Exception as e:
            print(f"  ⚠️  Error in batch {batch_num + 1}: {e}")
            continue

    creation_time = time.time() - overall_start

    print("=" * 70)
    print(f"\n✅ Table creation complete!")
    print(f"  ⏱️  Total time: {creation_time / 60:.1f} minutes ({creation_time / 3600:.1f} hours)")
    print(f"  📦 Total rows: {total_rows:,}")
    print(f"  📈 Average speed: {total_rows / creation_time:,.0f} rows/second")

    # Create indexes
    print("\n🔨 Creating indexes (this will take additional time)...")
    indexes = [
        ("idx_te_maker", "maker"),
        ("idx_te_taker", "taker"),
        ("idx_te_condition", "condition_id"),
        ("idx_te_matched_token", "matched_token"),
    ]

    for idx_name, column in indexes:
        idx_start = time.time()
        print(f"  Creating {idx_name}...", end=" ", flush=True)
        con.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON transactions_enriched({column})")
        print(f"✓ ({time.time() - idx_start:.1f}s)")

    # Final stats
    try:
        size_gb = con.execute("""
                              SELECT SUM(total_blocks * block_size) / 1024.0 / 1024.0 / 1024.0
                              FROM pragma_database_size()
                              """).fetchone()[0]
        print(f"\n💾 Database size: {size_gb:.2f} GB")
    except:
        pass

    con.close()

    total_time = time.time() - overall_start
    print("\n" + "=" * 70)
    print(f"✅ COMPLETE in {total_time / 60:.1f} minutes!")
    print("   transactions_enriched is ready to use")
    print("=" * 70)


if __name__ == "__main__":
    create_materialized_tables_low_memory()

if __name__ == "__main__":
    # Run the function
    create_materialized_tables_fast()


def run_duckdb_query(query: str, duck_db_path: str = "polymarket.duckdb") -> pd.DataFrame:
    con = get_db_connection(duck_db_path)
    result_df = con.execute(query).fetchdf()
    return result_df


def get_all_user_transactions_optimized(user: str) -> pd.DataFrame:
    """OPTIMIZED: Use pre-joined materialized table"""
    query = f'''
    SELECT
        strftime(to_timestamp(t.timestamp), '%Y-%m-%d %H:%M:%S') AS formatted, 
        '{user}' AS user,
        CASE
          WHEN t.maker = '{user}' AND t.makerAssetId = '0' THEN 'BUY'
          WHEN t.taker = '{user}' AND t.takerAssetId = '0' THEN 'BUY'
          WHEN t.maker = '{user}' AND t.takerAssetId = '0' THEN 'SELL'
          WHEN t.taker = '{user}' AND t.makerAssetId = '0' THEN 'SELL'
          ELSE 'INTERNAL'
        END AS status,
        CASE
           WHEN t.makerAssetId != '0' THEN t.makerAmountFilled / 1000000
           WHEN t.takerAssetId != '0' THEN t.takerAmountFilled / 1000000
        END AS shares,
        CASE
           WHEN t.makerAssetId = '0' THEN t.makerAmountFilled / 1000000
           WHEN t.takerAssetId = '0' THEN t.takerAmountFilled / 1000000
        END AS dollars,
        CASE
            WHEN t.makerAssetId = '0' THEN (t.makerAmountFilled / 1000000) / (t.takerAmountFilled / 1000000)
            WHEN t.takerAssetId = '0' THEN (t.takerAmountFilled / 1000000) / (t.makerAmountFilled / 1000000)
        END AS price_per_share,
        t.traded_answer,
        CASE
          WHEN t.resolved_outcome = 0 THEN 'UNRESOLVED'
          WHEN t.resolved_outcome = 1 THEN t.answer1
          WHEN t.resolved_outcome = 2 THEN t.answer2
        END AS outcome,
        t.question,
        t.closedTime,
        t.condition_id,
        t.transactionHash
    FROM transactions_enriched t
    WHERE t.maker = '{user}' OR t.taker = '{user}'
    ORDER BY t.condition_id, t.timestamp ASC;
    '''
    return run_duckdb_query(query)


def get_batch_user_data(user_list: list, duck_db_path: str = "polymarket.duckdb") -> dict:
    """Process multiple users in a single query - MASSIVE speedup"""
    if not user_list:
        return {}

    print(f"Batch processing {len(user_list)} users...")

    # Create a comma-separated list of quoted user addresses
    user_list_str = "'" + "','".join(user_list) + "'"

    con = get_db_connection(duck_db_path)

    # Get all transactions for all users in ONE query
    transactions_query = f'''
    SELECT
        CASE 
            WHEN t.maker IN ({user_list_str}) THEN t.maker
            ELSE t.taker 
        END AS user,
        strftime(to_timestamp(t.timestamp), '%Y-%m-%d %H:%M:%S') AS formatted,
        CASE
          WHEN t.maker IN ({user_list_str}) AND t.makerAssetId = '0' THEN 'BUY'
          WHEN t.taker IN ({user_list_str}) AND t.takerAssetId = '0' THEN 'BUY'
          WHEN t.maker IN ({user_list_str}) AND t.takerAssetId = '0' THEN 'SELL'
          WHEN t.taker IN ({user_list_str}) AND t.makerAssetId = '0' THEN 'SELL'
          ELSE 'INTERNAL'
        END AS status,
        CASE
           WHEN t.makerAssetId != '0' THEN t.makerAmountFilled / 1000000.0
           WHEN t.takerAssetId != '0' THEN t.takerAmountFilled / 1000000.0
        END AS shares,
        CASE
           WHEN t.makerAssetId = '0' THEN t.makerAmountFilled / 1000000.0
           WHEN t.takerAssetId = '0' THEN t.takerAmountFilled / 1000000.0
        END AS dollars,
        CASE
            WHEN t.makerAssetId = '0' THEN (t.makerAmountFilled / 1000000.0) / (t.takerAmountFilled / 1000000.0)
            WHEN t.takerAssetId = '0' THEN (t.takerAmountFilled / 1000000.0) / (t.makerAmountFilled / 1000000.0)
        END AS price_per_share,
        t.traded_answer,
        CASE
          WHEN t.resolved_outcome = 0 THEN 'UNRESOLVED'
          WHEN t.resolved_outcome = 1 THEN t.answer1
          WHEN t.resolved_outcome = 2 THEN t.answer2
        END AS outcome,
        t.question,
        t.closedTime,
        t.condition_id,
        t.transactionHash
    FROM transactions_enriched t
    WHERE t.maker IN ({user_list_str}) OR t.taker IN ({user_list_str})
    ORDER BY user, t.condition_id, t.timestamp ASC
    '''

    all_transactions = con.execute(transactions_query).fetchdf()

    # Get all redemptions for all users in ONE query
    redemptions_query = f'''
        SELECT * FROM redemptions r
        WHERE r.redeemer IN ({user_list_str})
    '''
    all_redemptions = con.execute(redemptions_query).fetchdf()

    # Group by user
    result = {}
    for user in user_list:
        user_transactions = all_transactions[all_transactions['user'] == user]
        user_redemptions = all_redemptions[all_redemptions['redeemer'] == user]
        result[user] = {
            'transactions': user_transactions,
            'redemptions': user_redemptions
        }

    return result


def get_all_user_redemptions(user: str) -> pd.DataFrame:
    """Get ALL redemptions for a user in one query"""
    query = f'''
        SELECT * FROM redemptions r
        WHERE r.redeemer = '{user}'
    '''
    return run_duckdb_query(query)


def print_result(result_df: pd.DataFrame) -> None:
    print()
    print(tabulate(result_df, headers='keys', tablefmt='github', showindex=False))
    print()


def analyze_single_investment(result_df, redemption_df, verbose=False, user_id=None, question=None,
                              market_condition_id=None):
    try:
        cash_flow = 0
        share_flow = 0
        max_shares_owned = 0
        max_money_invested = 0

        for _, row in result_df.iterrows():
            if row["status"] == "BUY":
                cash_flow -= row["dollars"]
                share_flow += row["shares"]
                if share_flow > max_shares_owned:
                    max_shares_owned = share_flow
                if cash_flow < max_money_invested:
                    max_money_invested = cash_flow
            elif row["status"] == "SELL":
                cash_flow += row["dollars"]
                share_flow -= row["shares"]

        if result_df.empty:
            return None

        # WON OR LOST OR UNRESOLVED STATUS
        if result_df.iloc[-1]["traded_answer"] == result_df.iloc[-1]["outcome"]:
            won_lost_unresolved = "WON"
        elif result_df.iloc[-1]["outcome"] == "UNRESOLVED":
            won_lost_unresolved = "UNRESOLVED"
        else:
            won_lost_unresolved = "LOST"

        # CASH REDEEMED STATUS
        cash_redeemed_status = 'False' if redemption_df.empty else 'True'

        # DOLLARS REDEEMED
        cash_redeemed_amount = redemption_df.iloc[0]["payout"] / 1_000_000 if not redemption_df.empty else 0

        # CASH REDEEMABLE STATUS
        cash_redeemable_status = 'True' if datetime.now() > pd.to_datetime(result_df.iloc[-1]["formatted"]) else 'False'

        # DOLLARS REDEEMABLE
        future_cash_redeemable = share_flow if redemption_df.empty and result_df.iloc[-1]["traded_answer"] == \
                                               result_df.iloc[-1]["outcome"] else 0

        # when lost in order to not overestimate winnings
        if won_lost_unresolved == 'LOST':
            cash_redeemed_amount_temp = 0
        else:
            cash_redeemed_amount_temp = cash_redeemed_amount

        cash_after_closing = (cash_flow + cash_redeemed_amount_temp)
        absolute_pl = float(f"{'+' if cash_after_closing > 0 else '-'}{abs(cash_after_closing):>.4f}")

        try:
            relative_pl = float(
                f"{'+' if cash_after_closing > 0 else '-'}{(100 / abs(cash_flow) * abs(cash_after_closing)):>.4f}")
        except ZeroDivisionError:
            relative_pl = 0

        if won_lost_unresolved == 'LOST':
            try:
                relative_pl = float(
                    f"{'+' if cash_after_closing > 0 else '-'}{(100 / abs(max_money_invested) * abs(cash_after_closing)):>.4f}")
            except ZeroDivisionError:
                relative_pl = 0
        elif won_lost_unresolved == 'WON' and cash_redeemed_status == "False" and cash_redeemed_amount < 1:
            try:
                relative_pl = float(
                    f"{'+' if cash_after_closing > 0 else '-'}{(100 / abs(max_money_invested) * abs(cash_after_closing)):>.4f}")
            except ZeroDivisionError:
                relative_pl = 0

        if datetime.now() > pd.to_datetime(result_df.iloc[-1]["formatted"]) and result_df.iloc[-1]["traded_answer"] == \
                result_df.iloc[-1]["outcome"]:
            pot_absolute_pl = float(
                f"{'' if cash_after_closing + share_flow > 0 else '-'}{abs(cash_after_closing + share_flow):>.4f}")
            try:
                pot_relative_pl = float(
                    f"{'' if cash_after_closing + share_flow > 0 else '-'}{(100 / abs(cash_flow) * abs(cash_after_closing + share_flow)): >.4f}")
            except ZeroDivisionError:
                pot_relative_pl = 0
        else:
            pot_absolute_pl = 'None'
            pot_relative_pl = 'None'

        market_dataframe = pd.DataFrame(
            {
                'UserId': [user_id],
                'MarketConditionId': [market_condition_id],
                'MarketQuestion': [question],
                'UserResult': [won_lost_unresolved],
                'MaxSharesOwnded': [max_shares_owned],
                'CashInvested': [cash_flow],
                'CashRedeemedStatus': [cash_redeemed_status],
                'CashRedeemedAmount': [cash_redeemed_amount],
                'CashRedeemableStatus': [cash_redeemable_status],
                'CashRedeemableAmount': [future_cash_redeemable],
                'AbsolutePL': [absolute_pl],
                'RelativePL': [relative_pl],
                'PotAbsolutePL': [pot_absolute_pl],
                'PotRelativePL': [pot_relative_pl],
            }
        )

    except Exception as e:
        print_result(result_df)
        raise e

    return market_dataframe


def analyze_user_from_batch(user_id: str, transactions_df: pd.DataFrame, redemptions_df: pd.DataFrame,
                            verbose: bool = False) -> pd.DataFrame:
    """Analyze a single user using pre-fetched batch data"""

    if transactions_df.empty:
        return pd.DataFrame()

    num_markets = transactions_df['condition_id'].nunique()
    user_data = []

    # Group by condition_id and process in memory
    for condition_id, condition_transactions in transactions_df.groupby('condition_id'):
        condition_redemptions = redemptions_df[
            redemptions_df['condition'] == condition_id
            ] if not redemptions_df.empty else pd.DataFrame()

        question = condition_transactions.iloc[0]['question']

        # Check if multi-answer market
        unique_answers = condition_transactions['traded_answer'].nunique()

        if unique_answers > 1:
            # Multi-answer market
            for traded_answer in condition_transactions['traded_answer'].unique():
                answer_transactions = condition_transactions[
                    condition_transactions['traded_answer'] == traded_answer
                    ]

                market_df = analyze_single_investment(
                    answer_transactions,
                    condition_redemptions,
                    verbose=verbose,
                    user_id=user_id,
                    question=question,
                    market_condition_id=condition_id
                )

                if market_df is not None:
                    user_data.append(market_df)
        else:
            # Single answer market
            market_df = analyze_single_investment(
                condition_transactions,
                condition_redemptions,
                verbose=verbose,
                user_id=user_id,
                question=question,
                market_condition_id=condition_id
            )

            if market_df is not None:
                user_data.append(market_df)

    if not user_data:
        return pd.DataFrame()

    return pd.concat(user_data, ignore_index=True)


def get_active_traders(duck_db_path="polymarket.duckdb", min_markets=20, max_markets=None):
    print(f"Evaluating active Traders with min_markets = {min_markets} and max_markets = {max_markets}...")
    con = duckdb.connect(duck_db_path)

    max_filter = f'AND COUNT(DISTINCT condition_id) < {max_markets}' if max_markets else ''
    result = con.execute(f"""
        SELECT user, COUNT(DISTINCT condition_id) as market_count
        FROM positions
        GROUP BY user
        HAVING COUNT(DISTINCT condition_id) > {min_markets} {max_filter}
        ORDER BY market_count DESC
    """).fetchdf()

    con.close()
    print(f"Found {len(result.index)} active Traders...")
    return result


def improved_user_data_output(user_data):
    readable_df = pd.DataFrame(
        columns=[
            "UserID",
            "MarketConditionID",
            "MarketQuestion",
            "AbsPL",
            "RelPL",
        ]
    )

    lost_rows = user_data[user_data["UserResult"] == "LOST"]
    won_and_cash_redeemed_rows = user_data[
        (user_data["UserResult"] == "WON") &
        (user_data["CashRedeemedStatus"].isin([True, "True", "true"]))
        ]
    won_but_nothing_redeemable = user_data[
        (user_data["UserResult"] == "WON") &
        (user_data["CashRedeemedStatus"].isin([False, "False", "false"])) &
        (user_data["CashRedeemableAmount"] < 1)
        ]
    won_and_redeemable = user_data[
        (user_data["UserResult"] == "WON") &
        (user_data["CashRedeemedStatus"].isin([False, "False", "false"])) &
        (user_data["CashRedeemableAmount"] >= 1)
        ]

    dfs = []

    if not lost_rows.empty:
        dfs.append(
            lost_rows[[
                "UserId",
                "MarketConditionId",
                "MarketQuestion",
                "AbsolutePL",
                "RelativePL",
            ]].rename(
                columns={
                    "AbsolutePL": "AbsPL",
                    "RelativePL": "RelPL",
                }
            )
        )

    if not won_and_cash_redeemed_rows.empty:
        dfs.append(
            won_and_cash_redeemed_rows[[
                "UserId",
                "MarketConditionId",
                "MarketQuestion",
                "AbsolutePL",
                "RelativePL",
            ]].rename(
                columns={
                    "AbsolutePL": "AbsPL",
                    "RelativePL": "RelPL",
                }
            )
        )

    if not won_but_nothing_redeemable.empty:
        dfs.append(
            won_but_nothing_redeemable[[
                "UserId",
                "MarketConditionId",
                "MarketQuestion",
                "AbsolutePL",
                "RelativePL",
            ]].rename(
                columns={
                    "AbsolutePL": "AbsPL",
                    "RelativePL": "RelPL",
                }
            )
        )

    if not won_and_redeemable.empty:
        dfs.append(
            won_and_redeemable[[
                "UserId",
                "MarketConditionId",
                "MarketQuestion",
                "PotAbsolutePL",
                "PotRelativePL",
            ]].rename(
                columns={
                    "PotAbsolutePL": "AbsPL",
                    "PotRelativePL": "RelPL",
                }
            )
        )

    if dfs:
        readable_df = pd.concat(dfs, ignore_index=True)

    return readable_df


def save_user_data(user_name, readable_user_data):
    os.makedirs("../artefacts/user_market_summary", exist_ok=True)
    with open(f"artefacts/user_market_summary/{user_name}.pickle", "wb") as f:
        pickle.dump(readable_user_data, f)


def create_user_statistics(readable_user_data: pd.DataFrame, min_trades: int = 15) -> pd.DataFrame | None:
    """Aggregate user trading data into comprehensive performance metrics"""
    df = readable_user_data.copy()

    if len(df) < min_trades:
        return None

    # Basic counts
    total_markets = len(df)

    # Win/Loss classification
    df['is_win'] = df['AbsPL'] > 0
    df['is_loss'] = df['AbsPL'] < 0
    df['is_breakeven'] = df['AbsPL'] == 0

    wins = df[df['is_win']]
    losses = df[df['is_loss']]

    # Calculate metrics
    metrics = {
        'user_id': df['UserId'].iloc[0] if len(df) > 0 else None,
        'total_markets_traded': total_markets,
        'num_wins': len(wins),
        'num_losses': len(losses),
        'num_breakeven': len(df[df['is_breakeven']]),
        'win_rate_pct': (len(wins) / total_markets * 100) if total_markets > 0 else 0,
        'loss_rate_pct': (len(losses) / total_markets * 100) if total_markets > 0 else 0,
        'total_pnl': df['AbsPL'].sum(),
        'total_gains': wins['AbsPL'].sum() if len(wins) > 0 else 0,
        'total_losses': losses['AbsPL'].sum() if len(losses) > 0 else 0,
        'net_pnl': df['AbsPL'].sum(),
        'avg_pnl_per_trade': df['AbsPL'].mean(),
        'median_pnl_per_trade': df['AbsPL'].median(),
        'max_win': wins['AbsPL'].max() if len(wins) > 0 else 0,
        'max_loss': losses['AbsPL'].min() if len(losses) > 0 else 0,
        'avg_win_amount': wins['AbsPL'].mean() if len(wins) > 0 else 0,
        'avg_loss_amount': losses['AbsPL'].mean() if len(losses) > 0 else 0,
        'avg_return_pct': df['RelPL'].mean(),
        'median_return_pct': df['RelPL'].median(),
        'max_return_pct': df['RelPL'].max(),
        'min_return_pct': df['RelPL'].min(),
        'avg_win_return_pct': wins['RelPL'].mean() if len(wins) > 0 else 0,
        'avg_loss_return_pct': losses['RelPL'].mean() if len(losses) > 0 else 0,
        'profit_factor': abs(wins['AbsPL'].sum() / losses['AbsPL'].sum()) if len(losses) > 0 and losses[
            'AbsPL'].sum() != 0 else float('inf'),
        'sharpe_ratio': df['AbsPL'].mean() / df['AbsPL'].std() if df['AbsPL'].std() != 0 else 0,
        'max_drawdown': df['AbsPL'].cumsum().min() if len(df) > 0 else 0,
        'pnl_std_dev': df['AbsPL'].std(),
        'pnl_coefficient_of_variation': (df['AbsPL'].std() / abs(df['AbsPL'].mean())) if df['AbsPL'].mean() != 0 else 0,
        'win_loss_ratio': (len(wins) / len(losses)) if len(losses) > 0 else float('inf'),
        'avg_win_loss_ratio': abs(wins['AbsPL'].mean() / losses['AbsPL'].mean()) if len(losses) > 0 and losses[
            'AbsPL'].mean() != 0 else float('inf'),
        'unique_conditions': df['MarketConditionId'].nunique(),
        'num_perfect_predictions': len(df[df['RelPL'] >= 100]),
        'num_total_losses': len(df[df['RelPL'] <= -100]),
        'num_trades_over_1k': len(df[df['AbsPL'].abs() > 1000]),
        'num_trades_over_10k': len(df[df['AbsPL'].abs() > 10000]),
        'num_trades_over_50k': len(df[df['AbsPL'].abs() > 50000]),
        'large_bet_win_rate': (len(wins[wins['AbsPL'] > 1000]) / len(df[df['AbsPL'].abs() > 1000]) * 100) if len(
            df[df['AbsPL'].abs() > 1000]) > 0 else 0,
        'best_market': df.loc[df['AbsPL'].idxmax(), 'MarketQuestion'] if len(df) > 0 else None,
        'worst_market': df.loc[df['AbsPL'].idxmin(), 'MarketQuestion'] if len(df) > 0 else None,
        'best_return': df['AbsPL'].max(),
        'worst_return': df['AbsPL'].min(),
        'estimated_total_capital_deployed': abs(losses['AbsPL'].sum()) if len(losses) > 0 else 0,
        'roi_on_capital': (df['AbsPL'].sum() / abs(losses['AbsPL'].sum()) * 100) if len(losses) > 0 and losses[
            'AbsPL'].sum() != 0 else 0,
    }

    return pd.DataFrame([metrics])


def save_single_line_user_stats(user_name: str, user_stats: pd.DataFrame) -> None:
    os.makedirs("../artefacts/user_markets", exist_ok=True)
    with open(f"artefacts/user_markets/{user_name}.pickle", "wb") as f:
        pickle.dump(user_stats, f)


if __name__ == "__main__":
    # ONE-TIME SETUP - Run this first time only!
    # Uncomment these lines on first run:
    create_indexes()
    create_materialized_tables_low_memory()
    print("Setup complete! Now run the script again.")
    sys.exit(0)

    # Load active traders
    if os.path.exists('../artefacts/active_traders/active_traders.pickle'):
        with open('../artefacts/active_traders/active_traders.pickle', 'rb') as f:
            active_traders = pickle.load(f)
    else:
        active_traders = get_active_traders(min_markets=20, max_markets=1000)
        with open('../artefacts/active_traders/active_traders.pickle', 'wb') as f:
            pickle.dump(active_traders, f)

    print(f"\nStarting BATCH analysis of {len(active_traders)} traders...")
    print("=" * 100)

    overall_start = datetime.now()

    # Process in batches for maximum speed
    BATCH_SIZE = 50  # Process 50 users at a time
    user_list = active_traders['user'].tolist()

    for batch_start in range(0, len(user_list), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(user_list))
        batch_users = user_list[batch_start:batch_end]

        print(f"\n{'=' * 100}")
        print(f"Processing batch {batch_start // BATCH_SIZE + 1} (users {batch_start + 1}-{batch_end})")
        print(f"{'=' * 100}")

        batch_start_time = datetime.now()

        # Fetch data for entire batch in ONE query
        batch_data = get_batch_user_data(batch_users)

        fetch_time = (datetime.now() - batch_start_time).total_seconds()
        print(f"Batch data fetched in {fetch_time:.2f} seconds")

        # Now process each user in the batch
        for idx, user in enumerate(batch_users, start=1):
            user_start = datetime.now()

            global_idx = batch_start + idx
            print(f"\nProcessing user {global_idx}/{len(user_list)}: {user}")

            # Get pre-fetched data for this user
            user_transactions = batch_data[user]['transactions']
            user_redemptions = batch_data[user]['redemptions']

            if user_transactions.empty:
                print(f"No data for user {user}, skipping...")
                continue

            # Analyze user with pre-fetched data
            user_data = analyze_user_from_batch(
                user,
                user_transactions,
                user_redemptions,
                verbose=False
            )

            if user_data.empty:
                print(f"No analyzable data for user {user}, skipping...")
                continue

            # Process and save results
            readable_user_data = improved_user_data_output(user_data)
            save_user_data(user, readable_user_data)

            # Create statistics
            user_stats = create_user_statistics(readable_user_data)
            if isinstance(user_stats, pd.DataFrame):
                save_single_line_user_stats(user, user_stats)
                print(f"✓ Saved stats for {user}")

            user_end = datetime.now()
            user_duration = (user_end - user_start).total_seconds()

            if idx % 10 == 0:
                print(f"  Progress: {idx}/{len(batch_users)} users in batch")

        batch_duration = (datetime.now() - batch_start_time).total_seconds()
        print(f"\nBatch completed in {batch_duration:.2f} seconds")
        print(f"Average time per user: {batch_duration / len(batch_users):.2f} seconds")

        # Estimate remaining time
        elapsed = (datetime.now() - overall_start).total_seconds()
        processed = batch_end
        remaining = len(user_list) - processed

        if processed > 0:
            avg_time_per_user = elapsed / processed
            estimated_remaining = avg_time_per_user * remaining
            print(f"\nOverall progress: {processed}/{len(user_list)} users")
            print(f"Estimated time remaining: {estimated_remaining / 3600:.2f} hours")

    overall_end = datetime.now()
    total_duration = (overall_end - overall_start).total_seconds()

    print("\n" + "=" * 100)
    print(f"COMPLETE! Processed {len(active_traders)} users in {total_duration / 3600:.2f} hours")
    print(f"Average time per user: {total_duration / len(active_traders):.2f} seconds")
    print("=" * 100)