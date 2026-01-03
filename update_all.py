from updating_logic.update_markets import update_markets_to_duckdb
from updating_logic.update_goldsky_orderbook import update_goldsky_orderbook_to_duckdb
from updating_logic.update_goldsky_activities import update_goldsky_activities_to_duckdb
from updating_logic.update_goldsky_positions import update_goldsky_positions


if __name__ == "__main__":
    print("\n\n\n")
    print("#"*150)
    print("STEP 1")
    print("UPDATING MARKETS")
    print("#" * 150)
    print("\n\n\n")
    updated_market_ids = update_markets_to_duckdb()

    print("\n\n\n")
    print("#" * 150)
    print("STEP 2")
    print("UPDATING ORDERBOOK GOLDSKY")
    print("#" * 150)
    print("\n\n\n")
    update_goldsky_orderbook_to_duckdb()

    print("\n\n\n")
    print("#" * 150)
    print("STEP 3")
    print("UPDATING REDEMPTIONS GOLDSKY")
    print("#" * 150)
    print("\n\n\n")
    update_goldsky_activities_to_duckdb()

    print("\n\n\n")
    print("#" * 150)
    print("STEP 4")
    print("UPDATING POSITIONS GOLDSKY")
    print("#" * 150)
    print("\n\n\n")
    update_goldsky_positions()

    print("\n\n\n")
    print("#" * 150)
    print("STEP 4")
    print("UPDATING POSITIONS GOLDSKY")
    print("#" * 150)
    print("\n\n\n")
    update_goldsky_positions()

    print("\n\n\n")
    print("#" * 150)
    print("DONE")
    print("#" * 150)
    print("\n\n\n")