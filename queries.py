import time

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


def get_payout_query(user: str, market_condition_id: str=None, question: str=None) -> str:
    condition_filter = ""
    if market_condition_id:
        condition_filter = f" AND r.condition = '{market_condition_id}'"

    elif question:
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


def analyze_single_investment(result_df, redemption_df):
    cash_flow = 0
    share_flow=0
    max_shares_owned=0

    for _, row in result_df.iterrows():
        if row["status"] == "BUY":
            cash_flow -= row["dollars"]  # money spent
            share_flow += row["shares"]
            if share_flow > max_shares_owned:
                max_shares_owned = share_flow

        elif row["status"] == "SELL":
            cash_flow += row["dollars"]  # money received
            share_flow -= row["shares"]
        else: print("UNKNOWN STATUS:", row["status"])

    print(f"{'CASH REDEEMED STATUS':<30} {'False' if redemption_df.empty else 'True':>15}")
    if not redemption_df.empty:
        payout = redemption_df.iloc[0]["payout"] / 1_000_000
        print(f"{'CASH REDEEMED':<30} {payout:>15,.4f}")
    else:
        print(f"{'CASH REDEEMABLE':<30} {'True' if datetime.now() > pd.to_datetime(result_df.iloc[-1]["formatted"]) else 'False':>15}")
        print(f"{'USER RESULT':<30} {'Won' if result_df.iloc[-1]["traded_answer"] ==  result_df.iloc[-1]["outcome"] else 'Lost':>15}")
        print(f"{'FUTURE REDEMPTION ($)':<30} {share_flow:>15,.4f}")
        payout = 0
    print(f"{'MAX SHARES INVESTED':<30} {max_shares_owned:>15,.4f}")
    print(f"{'CASH INVESTED':<30} {cash_flow:>15,.4f}")

    print("-" * 100)
    cash_after_closing = (cash_flow + payout)
    print(f"{'ABSOLUTE P&L ($)':<30} {'+' if cash_after_closing > 0 else '-'}{abs(cash_after_closing):>15,.4f}")
    print(f"{'RELATIVE P&L (%)':<30} {'+' if cash_after_closing > 0 else '-'}{(100 / abs(cash_flow) * abs(cash_after_closing)):>15,.4f}")

    if datetime.now() > pd.to_datetime(result_df.iloc[-1]["formatted"]) and result_df.iloc[-1]["traded_answer"] ==  result_df.iloc[-1]["outcome"]:
        print("-" * 100)
        print(f"{'POT. ABSOLUTE P&L ($)':<30} {'+' if cash_after_closing+share_flow > 0 else '-'}{abs(cash_after_closing+share_flow):>15,.4f}")
        print(f"{'POT. RELATIVE P&L (%)':<30} {'+' if cash_after_closing+share_flow > 0 else '-'}{(100 / abs(cash_flow) * abs(cash_after_closing+share_flow)):>15,.4f}")

    print("=" * 100)
    return


if __name__ == "__main__":

    # DEFINE EVERYTHING
    user_id = "0xee50a31c3f5a7c77824b12a941a54388a2827ed6"
    question = 'Gemini 3.0 Flash released by December 15?' #'Will Pope Leo XIV be the #1 searched person on Google this year?' #

    # HEADER
    print()
    print("INVESTMENT ANALYSIS")
    print("=" * 100)
    print(f"{'MARKET QUESTION':<30}", question)
    print(f"{'USER ID':<30}", user_id)
    print("-"*100)

    # GET INFO
    transaction_result = run_duckdb_query(
        get_query_user_market(
            user=user_id,
            question=question
        )
    )
    if len(list(set(transaction_result["traded_answer"]))) > 1:
        raise Warning("Mehrere tokens wurden in einem Market getraded, z.B. YES und NO: ", transaction_result)

    redemption_result = run_duckdb_query(
        get_payout_query(
            user=user_id,
            question=question
        )
    )

    # DO ANALYSIS
    # print_result(transaction_result)
    analyze_single_investment(transaction_result, redemption_result)


    #print_result(transaction_result)
    plot_cumulative_position(transaction_result, redemption_result)
