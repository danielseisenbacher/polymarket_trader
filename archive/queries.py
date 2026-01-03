import time
import os
import warnings
import pickle
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
from datetime import datetime

def get_query_user_market(user: str, market_condition_id: str = None, question: str = None) -> str:
    condition_filter = ""
    if market_condition_id:
        condition_filter = f" AND m.condition_id = '{market_condition_id}'"

    question_filter = ''
    if question:
        question_filter = f" AND m.question = '{question}'"

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
        -- 🔑 which answer was traded (numeric)
        --CASE
        --  WHEN COALESCE(NULLIF(t.makerAssetId, '0'), t.takerAssetId) = m.token1 THEN 1
        --  WHEN COALESCE(NULLIF(t.makerAssetId, '0'), t.takerAssetId) = m.token2 THEN 2
        --END AS traded_answer_id,
        -- 🔑 which answer was traded (label)
        CASE
          WHEN COALESCE(NULLIF(t.makerAssetId, '0'), t.takerAssetId) = m.token1 THEN m.answer1
          WHEN COALESCE(NULLIF(t.makerAssetId, '0'), t.takerAssetId) = m.token2 THEN m.answer2
        END AS traded_answer,
        -- 🔑 market result: 0 / 1 / 2
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
    WHERE t.maker = '{user}'{condition_filter}{question_filter}
    ORDER BY timestamp ASC;
    '''


def get_condition_id_from_question(question: str) -> str:
    query = f'''
            SELECT * FROM markets m 
            WHERE m.question = '{question}'
        '''
    result_df = run_duckdb_query(query)
    if result_df.empty:
        raise Exception(f"No ConditionId could be found with provided question: {result_df}")
    elif len(result_df.index) > 1:
        raise Exception(f"More than one market with the question {question} was returned: {result_df}")

    condition_id = result_df.iloc[0]["condition_id"]
    return condition_id

def get_question_from_condition_id(condition_id: str) -> str:
    query = f'''
                SELECT * FROM markets m 
                WHERE m.condition_id = '{condition_id}'
            '''
    result_df = run_duckdb_query(query)
    if result_df.empty:
        warnings.warn("No Question could be found with provided ConditionID")
        question = ""
    elif len(result_df.index) > 1:
        raise Exception(f"More than one question with the conditionID {condition_id} was returned: {result_df}")
    else:
        question = result_df.iloc[0]["question"]
    return question


def get_payout_query(user: str, market_condition_id: str=None, question: str=None) -> str:
    condition_filter = ""
    if market_condition_id:
        condition_filter = f" AND r.condition = '{market_condition_id}'"

    elif question:
        condition_id = get_condition_id_from_question(question)
        condition_filter = f" AND r.condition = '{condition_id}'"

    return f'''
                SELECT * FROM redemptions r
                WHERE r.redeemer = '{user}'{condition_filter}
            '''


def run_duckdb_query(query:str, duck_db_path: str = "polymarket.duckdb") -> pd.DataFrame:
    con = duckdb.connect(duck_db_path)
    result_df = con.execute(query).fetchdf()
    con.close()
    return result_df


def print_result(result_df: pd.DataFrame) -> None:
    time.sleep(2)
    print()
    print(tabulate(result_df, headers='keys', tablefmt='github', showindex=False))
    print()


def plot_cumulative_position(result_df: pd.DataFrame, redemption: pd.DataFrame = None):
    df = result_df.copy()
    df["formatted"] = pd.to_datetime(df["formatted"])

    if df.size == 0:
        print("Not able to plot: No Entries in Dataframe...")
        return

    if redemption.empty and df["formatted"].max() < df["closedTime"].iloc[0]:
        last = df.iloc[[-1]].copy()
        last[["dollars", "shares"]] = 0
        last["formatted"] = df["closedTime"].iloc[0]
        df = pd.concat([df, last], ignore_index=True)

    df["signed_shares"] = df.apply(
        lambda r: r["shares"] if r["status"] == "BUY" else -r["shares"],
        axis=1
    )
    df = df.sort_values("formatted")

    plt.figure(figsize=(12, 6))

    for answer, g in df.groupby("traded_answer"):
        g = g.copy()
        g["cum_position"] = g["signed_shares"].cumsum()

        # Plot segments with different colors based on direction
        for i in range(len(g) - 1):
            x = [g["formatted"].iloc[i], g["formatted"].iloc[i + 1]]
            y = [g["cum_position"].iloc[i], g["cum_position"].iloc[i + 1]]
            color = "green" if y[1] > y[0] else "red"
            plt.plot(x, y, color=color, marker="o", label=answer if i == 0 else "")

    if len(redemption.index) > 0:
        redemption["timestamp"] = pd.to_datetime(redemption["timestamp"], unit="s")
        plt.plot(redemption.iloc[0]["timestamp"], 0, marker="o")

    plt.vlines(df["closedTime"][0], plt.gca().get_ylim()[0], plt.gca().get_ylim()[1], color="r", linestyle="dashed",
               label="closed")
    plt.xlabel("Time")
    plt.ylabel("Shares")
    plt.title(df["question"].iloc[0])

    # Remove duplicate labels in legend
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys())

    plt.grid(True)
    plt.tight_layout()
    plt.show()


def analyze_single_investment(result_df, redemption_df, verbose=False, user_id=None, question=None, market_condition_id=None):
    try:
        cash_flow = 0
        share_flow=0
        max_shares_owned=0
        max_money_invested=0

        for _, row in result_df.iterrows():
            if row["status"] == "BUY":
                cash_flow -= row["dollars"]  # money spent
                share_flow += row["shares"]
                if share_flow > max_shares_owned:
                    max_shares_owned = share_flow
                if cash_flow < max_money_invested:
                    max_money_invested = cash_flow

            elif row["status"] == "SELL":
                cash_flow += row["dollars"]  # money received
                share_flow -= row["shares"]
            else: print("UNKNOWN STATUS:", row["status"])

        if result_df.empty:
            print("No transactions found - probably pre SYNC date.")
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
            relative_pl = float(f"{'+' if cash_after_closing > 0 else '-'}{(100 / abs(cash_flow) * abs(cash_after_closing)):>.4f}")
        except ZeroDivisionError:
            relative_pl = 0
        if won_lost_unresolved == 'LOST':
            try:
                relative_pl = float(f"{'+' if cash_after_closing > 0 else '-'}{(100 / abs(max_money_invested) * abs(cash_after_closing)):>.4f}")
            except ZeroDivisionError:
                relative_pl = 0
        elif won_lost_unresolved == 'WON' and cash_redeemed_status == "False" and cash_redeemed_amount < 1:
            try:
                relative_pl = float(f"{'+' if cash_after_closing > 0 else '-'}{(100 / abs(max_money_invested) * abs(cash_after_closing)):>.4f}")
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

        if verbose:
            print(f"{'USER RESULT':<30} {won_lost_unresolved:>15}")  # undecided status
            print(f"{'MAX SHARES INVESTED':<30} {max_shares_owned:>15,.4f}")
            print(f"{'CASH INVESTED':<30} {cash_flow:>15,.4f}")
            print(f"{'CASH REDEEMED STATUS':<30} {cash_redeemed_status:>15}")
            print(f"{'DOLLAR AMOUNT REDEEMED':<30} {cash_redeemed_amount:>15,.4f}")
            print(f"{'CASH REDEEMABLE STATUS':<30} {cash_redeemable_status:>15}")
            print(f"{'FUTURE REDEMPTION ($)':<30} {future_cash_redeemable:>15,.4f}")
            # CASH AFTER CLOSING
            print("-" * 100)
            print(f"{'ABSOLUTE P&L ($)':<30} {absolute_pl:>15,.4f}")
            print(f"{'RELATIVE P&L (%)':<30} {relative_pl:>15,.4f}")
            print("-" * 100)
            print(f"{'POT. ABSOLUTE P&L ($)':<30} {pot_absolute_pl:>15}")
            print(f"{'POT. RELATIVE P&L ($)':<30} {pot_relative_pl:>15}")
            print("=" * 100)

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


def analyze_multi_answer_market(user_id, market_condition_id, transaction_result, question, verbose):
    market_dataframe_list = []
    redemption_result = run_duckdb_query(
        get_payout_query(
            user=user_id,
            market_condition_id=market_condition_id
        )
    )

    for traded_answer in list(set(transaction_result["traded_answer"])):
        # DO ANALYSIS
        if verbose:
            print()
            print_result(transaction_result[transaction_result["traded_answer"] == traded_answer])
            
        market_dataframe = analyze_single_investment(
            transaction_result[transaction_result["traded_answer"] == traded_answer],
            redemption_result,
            verbose=verbose,
            user_id=user_id,
            question=question if question else None,
            market_condition_id=market_condition_id if market_condition_id else get_condition_id_from_question(question)
        )
        market_dataframe_list.append(market_dataframe)

    return market_dataframe_list

def analyze_one_user_market(user_id:str, question:str|None=None, market_condition_id:str|None=None, verbose:bool=False) -> pd.DataFrame|None|list:
    # HEADER
    question = question if question else get_question_from_condition_id(market_condition_id)
    if verbose:
        before = datetime.now()
        # DEFINE EVERYTHING
        print()
        print("=" * 100)
        print(f"{'MARKET QUESTION':<30}", question)
        print(f"{'USER ID':<30}", user_id)
        print("-"*100)

    print("user_market")
    before = datetime.now()

    # GET INFO
    transaction_result = run_duckdb_query(
        get_query_user_market(
            user=user_id,
            market_condition_id=market_condition_id
        )
    )
    after = datetime.now()
    delta = after - before
    print(f"user_market delta: {delta}")

    market_dataframe_list = []

    if len(list(set(transaction_result["traded_answer"]))) > 1:
        if verbose:
            print("\n\n")
            print(f"Entering Multi Anser Analysis: {question}\nMultiple Answers traded in one market: {list(set(transaction_result["traded_answer"]))}")

        market_dataframe_list = analyze_multi_answer_market(
            user_id,
            market_condition_id,
            transaction_result,
            question=question if question else None,
            verbose=verbose
        )
        return market_dataframe_list

    print("--"*10)
    before = datetime.now()
    redemption_result = run_duckdb_query(
        get_payout_query(
            user=user_id,
            market_condition_id=market_condition_id
        )
    )
    after = datetime.now()
    delta = after - before
    print("Time for this query: ", delta.total_seconds())

    # DO ANALYSIS
    #print_result(transaction_result)
    market_dataframe = analyze_single_investment(
        transaction_result,
        redemption_result,
        verbose=verbose,
        user_id=user_id,
        question=question if question else None,
        market_condition_id=market_condition_id if market_condition_id else get_condition_id_from_question(question)
    )

    if verbose:
        # print_result(transaction_result)
        # plot_cumulative_position(transaction_result, redemption_result)

        after = datetime.now()
        delta = after - before
        print("Time for this query: ", delta.total_seconds())
    return market_dataframe


def get_active_traders(duck_db_path="polymarket.duckdb", min_markets=20, max_markets=None):
    print(f"Evaluating active Traders with min_markets = {min_markets} and max_markets = {max_markets}...")
    con = duckdb.connect(duck_db_path)


    result = con.execute(f"""
        SELECT user, COUNT(DISTINCT condition_id) as market_count
        FROM positions
        GROUP BY user
        HAVING COUNT(DISTINCT condition_id) > {min_markets}{f'AND COUNT(DISTINCT condition_id) < {max_markets}' if max_markets else ''}
        ORDER BY market_count DESC
    """).fetchdf()

    con.close()
    print(f"Found {len(result.index)} active Traders...")
    return result


def get_condition_ids_from_user(user_id, duck_db_path="polymarket.duckdb"):
    con = duckdb.connect(duck_db_path)

    result = con.execute(f"""
            SELECT condition_id
            FROM positions
            WHERE user = '{user_id}'
        """).fetchdf()

    con.close()
    print("+"*100)
    print(f"User {user_id} traded in {len(result.index)} markets...")
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
    with open(f"user_market_summary/{user_name}.pickle", "wb") as f:
        pickle.dump(readable_user_data, f)


def create_user_statistics(readable_user_data: pd.DataFrame,  min_trades:int=15) -> pd.DataFrame|None:
    """
    Aggregate user trading data into comprehensive performance metrics
    """

    df = readable_user_data.copy()

    if len(df) < min_trades:
        print(f"Insufficient data: {len(df)} trades (minimum required: {min_trades})")
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

        # Trade counts
        'total_markets_traded': total_markets,
        'num_wins': len(wins),
        'num_losses': len(losses),
        'num_breakeven': len(df[df['is_breakeven']]),

        # Win rate
        'win_rate_pct': (len(wins) / total_markets * 100) if total_markets > 0 else 0,
        'loss_rate_pct': (len(losses) / total_markets * 100) if total_markets > 0 else 0,

        # P&L metrics (absolute dollars)
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

        # P&L metrics (relative %)
        'avg_return_pct': df['RelPL'].mean(),
        'median_return_pct': df['RelPL'].median(),
        'max_return_pct': df['RelPL'].max(),
        'min_return_pct': df['RelPL'].min(),

        'avg_win_return_pct': wins['RelPL'].mean() if len(wins) > 0 else 0,
        'avg_loss_return_pct': losses['RelPL'].mean() if len(losses) > 0 else 0,

        # Risk metrics
        'profit_factor': abs(wins['AbsPL'].sum() / losses['AbsPL'].sum()) if len(losses) > 0 and losses[
            'AbsPL'].sum() != 0 else float('inf'),
        'sharpe_ratio': df['AbsPL'].mean() / df['AbsPL'].std() if df['AbsPL'].std() != 0 else 0,
        'max_drawdown': df['AbsPL'].cumsum().min() if len(df) > 0 else 0,

        # Consistency metrics
        'pnl_std_dev': df['AbsPL'].std(),
        'pnl_coefficient_of_variation': (df['AbsPL'].std() / abs(df['AbsPL'].mean())) if df[
                                                                                             'AbsPL'].mean() != 0 else 0,
        'win_loss_ratio': (len(wins) / len(losses)) if len(losses) > 0 else float('inf'),
        'avg_win_loss_ratio': abs(wins['AbsPL'].mean() / losses['AbsPL'].mean()) if len(losses) > 0 and losses[
            'AbsPL'].mean() != 0 else float('inf'),

        # Market diversity
        'unique_conditions': df['MarketConditionId'].nunique(),
        'markets_list': df['MarketQuestion'].tolist(),
        'condition_ids_list': df['MarketConditionId'].tolist(),

        # Perfect predictions (100%+ returns)
        'num_perfect_predictions': len(df[df['RelPL'] >= 100]),
        'num_total_losses': len(df[df['RelPL'] <= -100]),

        # Large trades
        'num_trades_over_1k': len(df[df['AbsPL'].abs() > 1000]),
        'num_trades_over_10k': len(df[df['AbsPL'].abs() > 10000]),
        'num_trades_over_50k': len(df[df['AbsPL'].abs() > 50000]),

        # Hit rate on large bets
        'large_bet_win_rate': (len(wins[wins['AbsPL'] > 1000]) / len(df[df['AbsPL'].abs() > 1000]) * 100) if len(
            df[df['AbsPL'].abs() > 1000]) > 0 else 0,

        # Best and worst markets
        'best_market': df.loc[df['AbsPL'].idxmax(), 'MarketQuestion'] if len(df) > 0 else None,
        'worst_market': df.loc[df['AbsPL'].idxmin(), 'MarketQuestion'] if len(df) > 0 else None,
        'best_return': df['AbsPL'].max(),
        'worst_return': df['AbsPL'].min(),

        # Total capital involved (estimate based on losses, since that's money spent)
        'estimated_total_capital_deployed': abs(losses['AbsPL'].sum()) if len(losses) > 0 else 0,
        'roi_on_capital': (df['AbsPL'].sum() / abs(losses['AbsPL'].sum()) * 100) if len(losses) > 0 and losses[
            'AbsPL'].sum() != 0 else 0,
    }

    return pd.DataFrame([metrics])


def save_single_line_user_stats(user_name:str, user_stats:pd.DataFrame)->None:
    with open(f"aggregated_user_data/{user_name}.pickle", "wb") as f:
        pickle.dump(user_stats, f)

    return None

if __name__ == "__main__":
    #user_id = "0xee50a31c3f5a7c77824b12a941a54388a2827ed6"  # "0x5374d13b5f614f0571cbe94742c3df5fbadc7f49" ###
    #question = 'Gemini 3.0 Flash released by December 15?'  # 'Will Pope Leo XIV be the #1 searched person on Google this year?' #"Will inflation reach more than 10% in 2025?" ###'Will Kendrick Lamar be the #1 searched person on Google this year?'#
    #market_condition_id = None

    if os.path.exists('../artefacts/active_traders/active_traders.pickle'):
        with open('../artefacts/active_traders/active_traders.pickle', 'rb') as f:
            active_traders = pickle.load(f)
    else:
        active_traders = get_active_traders(
            min_markets=20,
            max_markets=1000
        )
        with open('../artefacts/active_traders/active_traders.pickle', 'wb') as f:
            pickle.dump(active_traders, f)

    for index, (_, row) in enumerate(active_traders.iterrows(), start=1):
        print(index,"/",len(active_traders))
        user = row["user"]
        if user != "0xee50a31c3f5a7c77824b12a941a54388a2827ed6":
            continue

        condition_ids_df = get_condition_ids_from_user(user)

        user_data = []

        # Get unique condition_ids and count duplicates
        unique_condition_ids = condition_ids_df['condition_id'].nunique()
        total_rows = len(condition_ids_df)
        num_duplicates = total_rows - unique_condition_ids

        print(f"Total condition_ids: {total_rows}")
        print(f"Unique condition_ids: {unique_condition_ids}")
        print(f"Duplicates: {num_duplicates}")
        print()

        # Remove duplicates and iterate through unique condition_ids only
        condition_ids_df_unique = condition_ids_df.drop_duplicates(subset=['condition_id'])

        for idx, (_, r) in enumerate(condition_ids_df_unique.iterrows(), start=1):
            print(f"Analyzing market: {idx}/{unique_condition_ids}")

            market_df = analyze_one_user_market(
                user_id=user,
                market_condition_id=r["condition_id"],
                question=None,
                verbose=False
            )
            if isinstance(market_df, pd.DataFrame):
                user_data.append(market_df)

            elif isinstance(market_df, list):
                user_data.extend(market_df)

            else:
                # None's will be skipped
                pass

        user_data = pd.concat(user_data, ignore_index=True)

        #user_stats = analyze_all_user_data(user_data)
        #print_result(user_data)

        readable_user_data = improved_user_data_output(user_data)

        # save readable user data
        save_user_data(user, readable_user_data)

        print_result(readable_user_data)

        user_stats = create_user_statistics(readable_user_data)
        print_result(user_stats)

        if isinstance(user_stats, pd.DataFrame):
            save_single_line_user_stats(user, user_stats)

