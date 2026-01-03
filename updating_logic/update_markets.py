import requests
import duckdb
import json
from datetime import datetime, timezone


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def clean_timestamp(value):
    """Convert Polymarket ISO timestamp into DuckDB-compatible format."""
    if not value:
        return None
    return value.replace("Z", "+00:00")  # DuckDB parses this correctly


def resolve_answer(outcome_string, closedTime):
    if not outcome_string or not closedTime:
        return 0

    try:
        prices = [float(p) for p in json.loads(outcome_string)]
        answer = 1 if prices[0] > 0.99 else (2 if prices[1] > 0.99 else 0)
        return answer
    except (json.JSONDecodeError, ValueError, IndexError, TypeError):
        # If parsing fails for any reason, return 0 (unresolved)
        return 0

# ------------------------------------------------------------
# DB Functions
# ------------------------------------------------------------

def create_markets_table_if_not_exists(con):
    """Create the markets table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            id VARCHAR PRIMARY KEY, 
            createdAt TIMESTAMP, 
            updatedAt TIMESTAMP, 
            question VARCHAR, 
            answer1 VARCHAR, 
            answer2 VARCHAR, 
            neg_risk BOOLEAN, 
            market_slug VARCHAR, 
            token1 VARCHAR, 
            token2 VARCHAR, 
            condition_id VARCHAR, 
            volume DOUBLE, 
            ticker VARCHAR, 
            closedTime TIMESTAMP,
            outcomePrices VARCHAR,
            resolved_outcome INTEGER
        )
    """)
    print("✓ Markets table created or already exists")


def upsert_market_row(con, row):
    """Insert or update a market row."""
    con.execute("""
        INSERT INTO markets AS m (
            id, createdAt, updatedAt, question, answer1, answer2, neg_risk,
            market_slug, token1, token2, condition_id, volume, ticker, closedTime, outcomePrices, resolved_outcome
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            createdAt        = EXCLUDED.createdAt,
            updatedAt        = EXCLUDED.updatedAt,
            question         = EXCLUDED.question,
            answer1          = EXCLUDED.answer1,
            answer2          = EXCLUDED.answer2,
            neg_risk         = EXCLUDED.neg_risk,
            market_slug      = EXCLUDED.market_slug,
            token1           = EXCLUDED.token1,
            token2           = EXCLUDED.token2,
            condition_id     = EXCLUDED.condition_id,
            volume           = EXCLUDED.volume,
            ticker           = EXCLUDED.ticker,
            closedTime       = EXCLUDED.closedTime,
            outcomePrices    = EXCLUDED.outcomePrices,
            resolved_outcome = EXCLUDED.resolved_outcome;
    """, row)


def get_last_sync(duck_db_path):
    """Return the most recently synced updatedAt timestamp."""
    con = duckdb.connect(duck_db_path)

    result = con.execute("""
        SELECT updatedAt FROM markets ORDER BY updatedAt DESC LIMIT 1;
    """).fetchone()

    con.close()

    if result is None:
        print("No Sync of markets has been conducted...")
        return None  # No data yet
    else:
        last_sync = result[0].replace(tzinfo=timezone.utc)
        print("Last Sync was conducted:", last_sync)

    return last_sync # Extract timestamp

# ------------------------------------------------------------
# Main sync function
# ------------------------------------------------------------

def update_markets_to_duckdb(duck_db_path="polymarket.duckdb", batch_size=500):

    con = duckdb.connect(duck_db_path)
    create_markets_table_if_not_exists(con)

    last_sync = get_last_sync(duck_db_path)

    base_url = "https://gamma-api.polymarket.com/markets"
    offset = 0
    total_inserted = 0

    # to track which markets were updated in this round
    updated_market_ids = {}

    while True:
        params = {
            "order": "updatedAt",
            "ascending": "false",
            "limit": batch_size,
            "offset": offset
        }

        r = requests.get(base_url, params=params, timeout=30)

        if r.status_code != 200:
            print("API error:", r.status_code, r.text)
            break

        markets = r.json()

        if not markets:
            print("No more markets from API.")
            break

        batch_count = 0
        stop_sync = False

        first_timestamp = markets[0].get("updatedAt")
        last_timestamp = markets[-1].get("updatedAt")
        for m in markets:

            api_updated = clean_timestamp(m.get("updatedAt"))

            # Stop when reaching older data
            if last_sync and datetime.fromisoformat(api_updated) <= last_sync:
                print("Reached already-synced timestamp. Stopping.")
                stop_sync = True
                break

            # outcomes list
            outcomes = []
            try:
                outcomes = json.loads(m.get("outcomes", "[]") or "[]")
            except:
                pass

            # clobTokenIds
            clob = []
            try:
                clob = json.loads(m.get("clobTokenIds", "[]") or "[]")
            except:
                pass

            # ticker (from events)
            ticker = ""
            if m.get("events"):
                ticker = m["events"][0].get("ticker", "")

            market_id = m.get("id", "")
            if market_id != "":
                updated_market_ids[m.get("id", "")] = resolve_answer(m.get("outcomePrices"), clean_timestamp(m.get("closedTime")))

            row = [
                m.get("id", ""),
                clean_timestamp(m.get("createdAt")),
                api_updated,
                m.get("question", "") or m.get("title", ""),
                outcomes[0] if len(outcomes) > 0 else "",
                outcomes[1] if len(outcomes) > 1 else "",
                (
                    m.get("negRiskAugmented", False)
                    or m.get("negRiskOther", False)
                    or m.get("enableNegRisk", False)
                    or m.get("negRisk", False)
                ),
                m.get("slug", ""),
                clob[0] if len(clob) > 0 and clob[0] != "" else None,
                clob[1] if len(clob) > 1 and clob[1] != "" else None,
                m.get("conditionId", ""),
                m.get("volumeNum") or m.get("volume") or 0.0,
                ticker,
                clean_timestamp(m.get("closedTime")),
                m.get("outcomePrices"),
                resolve_answer(m.get("outcomePrices"), clean_timestamp(m.get("closedTime")))
            ]

            upsert_market_row(con, row)
            batch_count += 1

        total_inserted += batch_count
        first_timestamp = markets[0].get("updatedAt")
        last_timestamp = markets[-1].get("updatedAt")

        print(f"Inserted {batch_count} rows (total {total_inserted}) - {first_timestamp} - {last_timestamp}")

        if stop_sync:
            break

        # Move offset for next page
        offset += batch_count

        if batch_count == 0:
            break

    con.close()
    print("\nFinished updating DuckDB.")
    print(f"Total new rows inserted: {total_inserted}")

    return updated_market_ids


# ------------------------------------------------------------
# Run the sync
# ------------------------------------------------------------
if __name__ == "__main__":
    update_markets_to_duckdb(batch_size=500)
