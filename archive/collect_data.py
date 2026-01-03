import os
import pandas as pd
import tabulate

INPUT_DIR = "../artefacts/user_markets"
OUTPUT_FILE = "combined.xlsx"

dfs = []
reference_columns = None

for filename in os.listdir(INPUT_DIR):
    if not filename.endswith(".pickle"):
        continue

    path = os.path.join(INPUT_DIR, filename)
    #print(f"Loading {path}")

    df = pd.read_pickle(path)

    # Skip empty frames
    if df.empty:
        print(f"⚠️  Skipping empty file: {filename}")
        continue

    # Initialize schema from first valid file
    if reference_columns is None:
        reference_columns = list(df.columns)
        print(f"Using schema from {filename}: {reference_columns}")
        dfs.append(df)
        continue

    # Enforce schema equality
    if list(df.columns) != reference_columns:
        print(f"❌ Skipping {filename} — schema mismatch")
        print(f"    Expected: {reference_columns}")
        print(f"    Found:    {list(df.columns)}")
        continue

    dfs.append(df)

if not dfs:
    raise RuntimeError("No DataFrames matched the reference schema")

combined_df = pd.concat(dfs, ignore_index=True)
combined_df["url"] = "https://polymarket.com/" + combined_df["user_id"].astype(str)
cols = ["url"] + [c for c in combined_df.columns if c != "url"]
combined_df = combined_df[cols]
combined_df = combined_df.sort_values(by="median_roi_pct", ascending=False)


print(tabulate.tabulate(combined_df, headers="keys", tablefmt="psql"))


