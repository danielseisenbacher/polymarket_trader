import time
import os
import pickle
import requests
import duckdb
import pandas as pd
from tabulate import tabulate
from datetime import datetime


def get_batch_query_user_markets(user: str, condition_ids: list) -> str:
    """Get transactions for multiple markets at once"""
    quoted_ids = ','.join([f"'{cid}'" for cid in condition_ids])
    condition_filter = f"AND m.condition_id IN ({quoted_ids})"

    return f'''
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
        CASE
          WHEN COALESCE(NULLIF(t.makerAssetId, '0'), t.takerAssetId) = m.token1 THEN m.answer1
          WHEN COALESCE(NULLIF(t.makerAssetId, '0'), t.takerAssetId) = m.token2 THEN m.answer2
        END AS traded_answer,
        CASE
          WHEN m.resolved_outcome = 0 THEN 'UNRESOLVED'
          WHEN m.resolved_outcome = 1 THEN m.answer1
          WHEN m.resolved_outcome = 2 THEN m.answer2
        END AS outcome,
        m.question,
        m.closedTime,
        m.condition_id,
        t.transactionHash
    FROM transactions t
    JOIN markets m
      ON COALESCE(
           NULLIF(t.makerAssetId, '0'),
           t.takerAssetId
         ) IN (m.token1, m.token2)
    WHERE t.maker = '{user}' {condition_filter}
    ORDER BY m.condition_id, timestamp ASC;
    '''


def get_batch_payout_query(user: str, condition_ids: list) -> str:
    """Get redemptions for multiple markets at once"""
    quoted_ids = ','.join([f"'{cid}'" for cid in condition_ids])
    condition_filter = f"AND r.condition IN ({quoted_ids})"

    return f'''
        SELECT * FROM redemptions r
        WHERE r.redeemer = '{user}' {condition_filter}
    '''


def run_duckdb_query(query: str, duck_db_path: str = "polymarket.duckdb") -> pd.DataFrame:
    con = duckdb.connect(duck_db_path)
    result_df = con.execute(query).fetchdf()
    con.close()
    return result_df


def print_result(result_df: pd.DataFrame) -> None:
    time.sleep(2)
    print()
    print(tabulate(result_df, headers='keys', tablefmt='github', showindex=False))
    print()


def analyze_single_investment(result_df, redemption_df, user_id, question, market_condition_id):
    """Analyze a single market position and return final format directly"""
    if result_df.empty:
        return None

    try:
        # Calculate cash and share flows
        cash_spent = result_df[result_df["status"] == "BUY"]["dollars"].sum()
        cash_received = result_df[result_df["status"] == "SELL"]["dollars"].sum()
        shares_bought = result_df[result_df["status"] == "BUY"]["shares"].sum()
        shares_sold = result_df[result_df["status"] == "SELL"]["shares"].sum()

        net_cash = cash_received - cash_spent
        net_shares = shares_bought - shares_sold

        # Check if position won
        last_row = result_df.iloc[-1]
        won = last_row["traded_answer"] == last_row["outcome"]

        # Calculate realized P&L
        redemption_value = redemption_df.iloc[0]["payout"] / 1_000_000 if not redemption_df.empty else 0

        # Calculate absolute P&L
        if won and net_shares > 0:
            # Won with remaining shares: add potential redemption value
            abs_pl = net_cash + (redemption_value if redemption_value > 0 else net_shares)
        else:
            # Lost or fully closed position
            abs_pl = net_cash

        # Calculate relative P&L (return on investment)
        rel_pl = (abs_pl / cash_spent * 100) if cash_spent > 0 else 0

        return pd.DataFrame({
            'UserId': [user_id],
            'MarketConditionId': [market_condition_id],
            'MarketQuestion': [question],
            'AbsPL': [abs_pl],
            'RelPL': [rel_pl],
        })

    except Exception as e:
        print(f"Error analyzing market {market_condition_id}: {e}")
        return None


def analyze_batch_markets(user_id: str, condition_ids: list) -> list:
    """Process multiple markets in a single batch"""
    if not condition_ids:
        return []

    before = datetime.now()
    print(f"Fetching data for {len(condition_ids)} markets...")

    # Get all transactions for these markets in one query
    transactions_df = run_duckdb_query(get_batch_query_user_markets(user_id, condition_ids))

    # Get all redemptions for these markets in one query
    redemptions_df = run_duckdb_query(get_batch_payout_query(user_id, condition_ids))

    after = datetime.now()
    print(f"Batch query completed in {(after - before).total_seconds():.2f} seconds")

    market_dataframes = []

    # Process each market
    for condition_id in condition_ids:
        # Filter transactions for this specific market
        market_transactions = transactions_df[transactions_df['condition_id'] == condition_id]

        if market_transactions.empty:
            continue

        # Filter redemptions for this specific market
        market_redemptions = redemptions_df[redemptions_df['condition'] == condition_id] if not redemptions_df.empty else pd.DataFrame()

        # Get unique traded answers for this market
        traded_answers = market_transactions['traded_answer'].unique()

        # Process each traded answer separately (for multi-answer markets)
        for traded_answer in traded_answers:
            answer_transactions = market_transactions[market_transactions['traded_answer'] == traded_answer]

            market_df = analyze_single_investment(
                answer_transactions,
                market_redemptions,
                user_id=user_id,
                question=answer_transactions.iloc[0]['question'],
                market_condition_id=condition_id
            )

            if market_df is not None:
                market_dataframes.append(market_df)

    return market_dataframes


def get_active_traders(duck_db_path="polymarket.duckdb", min_markets=20, max_markets=None):
    print(f"Evaluating active Traders with min_markets = {min_markets} and max_markets = {max_markets}...")
    con = duckdb.connect(duck_db_path)

    result = con.execute(f"""
        SELECT user, COUNT(DISTINCT condition_id) as market_count
        FROM positions
        GROUP BY user
        HAVING COUNT(DISTINCT condition_id) > {min_markets}{f' AND COUNT(DISTINCT condition_id) < {max_markets}' if max_markets else ''}
        ORDER BY market_count DESC
    """).fetchdf()

    con.close()
    print(f"Found {len(result.index)} active Traders...")
    return result


def get_condition_ids_from_user(user_id, duck_db_path="polymarket.duckdb"):
    con = duckdb.connect(duck_db_path)

    result = con.execute(f"""
            SELECT DISTINCT condition_id
            FROM positions
            WHERE user = '{user_id}'
        """).fetchdf()

    con.close()
    print("+" * 100)
    print(f"User {user_id} traded in {len(result.index)} unique markets...")
    return result


def create_user_statistics(user_data: pd.DataFrame, min_trades: int = 15) -> pd.DataFrame | None:
    """Aggregate user trading data into comprehensive performance metrics"""
    df = user_data.copy()

    if len(df) < min_trades:
        print(f"Insufficient data: {len(df)} trades (minimum required: {min_trades})")
        return None

    df['is_win'] = df['AbsPL'] > 0
    df['is_loss'] = df['AbsPL'] < 0
    df['is_breakeven'] = df['AbsPL'] == 0

    wins = df[df['is_win']]
    losses = df[df['is_loss']]

    total_markets = len(df)

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
        'profit_factor': abs(wins['AbsPL'].sum() / losses['AbsPL'].sum()) if len(losses) > 0 and losses['AbsPL'].sum() != 0 else float('inf'),
        'sharpe_ratio': df['AbsPL'].mean() / df['AbsPL'].std() if df['AbsPL'].std() != 0 else 0,
    }

    return pd.DataFrame([metrics])


def save_user_data(user_name: str, user_data: pd.DataFrame) -> None:
    os.makedirs("artefacts/user_markets", exist_ok=True)
    with open(f"artefacts/user_markets/{user_name}.pickle", "wb") as f:
        pickle.dump(user_data, f)


def save_user_stats(user_name: str, user_stats: pd.DataFrame) -> None:
    os.makedirs("artefacts/user_market_summary", exist_ok=True)
    with open(f"artefacts/user_market_summary/{user_name}.pickle", "wb") as f:
        pickle.dump(user_stats, f)

def analyze_all_traders():
    BATCH_SIZE = 50  # Process 50 markets at once

    if os.path.exists('artefacts/active_traders/active_traders.pickle'):
        with open('artefacts/active_traders/active_traders.pickle', 'rb') as f:
            active_traders = pickle.load(f)
    else:
        active_traders = get_active_traders(min_markets=20, max_markets=1000)
        os.makedirs('artefacts/active_traders', exist_ok=True)
        with open('artefacts/active_traders/active_traders.pickle', 'wb') as f:
            pickle.dump(active_traders, f)

    for index, (_, row) in enumerate(active_traders.iterrows(), start=1):

        user = row["user"]

        if user != "0xee50a31c3f5a7c77824b12a941a54388a2827ed6":
            continue

        print(f"\n{'=' * 100}")
        print(f"Processing user {index}/{len(active_traders)}")

        condition_ids_df = get_condition_ids_from_user(user)
        condition_ids_list = condition_ids_df['condition_id'].tolist()

        total_markets = len(condition_ids_list)
        print(f"Total unique markets: {total_markets}")

        user_data = []

        # Process markets in batches
        for i in range(0, total_markets, BATCH_SIZE):
            batch = condition_ids_list[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (total_markets + BATCH_SIZE - 1) // BATCH_SIZE

            print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch)} markets)")

            batch_results = analyze_batch_markets(user, batch)
            user_data.extend(batch_results)

        if not user_data:
            print("No valid market data found")
            continue

        # Combine all results
        user_data_df = pd.concat(user_data, ignore_index=True)

        # Save market-level data
        save_user_data(user, user_data_df)
        print_result(user_data_df)

        # Create and save aggregated statistics
        user_stats = create_user_statistics(user_data_df)
        if user_stats is not None:
            print_result(user_stats)
            save_user_stats(user, user_stats)

        print(f"\nCompleted processing for user {user}")


def get_top_traders():
    BASE_URL = "https://data-api.polymarket.com/v1/leaderboard"
    LIMIT = 50
    MAX_OFFSET = 1000

    offset = 0
    rows = []

    while True:
        params = {
            "category": "OVERALL",
            "timePeriod": "ALL",
            "orderBy": "PNL",
            "limit": LIMIT,
            "offset": offset,
        }

        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()

        # Stop when no more results
        if not data:
            break

        rows.extend(data)

        offset += LIMIT

        if offset >= MAX_OFFSET:
            break

    # Convert to DataFrame
    df = pd.DataFrame(rows)

    # Save as pickle
    output_path = "artefacts/top_traders/polymarket_leaderboard_top_1000.pkl"
    df.to_pickle(output_path)

    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":

    get_top_traders()
    analyze_all_traders()
