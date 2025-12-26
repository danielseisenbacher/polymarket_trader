import os
import csv
import json
import requests
import time
from typing import List
import polars as pl

PLATFORM_WALLETS = ['0xc5d563a36ae78145c45a50134d48a1215220f80a', '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e']


def get_markets(main_file: str = "markets.csv", missing_file: str = "missing_markets.csv"):
    """
    Load and combine markets from both files, deduplicate, and sort by createdAt
    Returns combined Polars DataFrame sorted by creation date
    """
    import polars as pl
    
    # Schema overrides for long token IDs
    schema_overrides = {
        "token1": pl.Utf8,      # 76-digit ids → strings
        "token2": pl.Utf8,
    }
    
    dfs = []
    
    # Load main markets file
    if os.path.exists(main_file):
        main_df = pl.scan_csv(main_file, schema_overrides=schema_overrides).collect(streaming=True)
        dfs.append(main_df)
        print(f"Loaded {len(main_df)} markets from {main_file}")
    
    # Load missing markets file
    if os.path.exists(missing_file):
        missing_df = pl.scan_csv(missing_file, schema_overrides=schema_overrides).collect(streaming=True)
        dfs.append(missing_df)
        print(f"Loaded {len(missing_df)} markets from {missing_file}")
    
    if not dfs:
        print("No market files found!")
        return pl.DataFrame()
    
    # Combine, deduplicate, and sort
    combined_df = (
        pl.concat(dfs)
        .unique(subset=['id'], keep='first')
        .sort('createdAt')
    )
    
    print(f"Combined total: {len(combined_df)} unique markets (sorted by createdAt)")
    return combined_df

