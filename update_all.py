from update_markets import update_markets_to_duckdb
from update_goldsky_orderbook import update_goldsky_orderbook_to_duckdb
from update_goldsky_activities import update_goldsky_activities_to_duckdb
from process_markets_and_goldsky import combine_markets_and_goldsky
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
    print("POSTPROCESSING")
    print("#" * 150)
    print("\n\n\n")
    #combine_markets_and_goldsky()

    print("\n\n\n")
    print("#" * 150)
    print("DONE")
    print("#" * 150)
    print("\n\n\n")