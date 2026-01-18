import requests
import os
import pandas as pd
import time
from datetime import date, timedelta

base_url = "https://forecastex.com/api/download"
start_date = date(2024, 8, 1)
end_date = date(2026, 1, 12)
output_dir = "forecastex_prices"
master_csv = "Forecastex_Price_Analysis.csv"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# --- STEP 1: DOWNLOAD DATA ---
current_date = start_date
print(f"Starting download from {start_date} to {end_date}...")

while current_date <= end_date:
    date_str = current_date.strftime("%Y%m%d")
    filename = os.path.join(output_dir, f"prices_{date_str}.csv")
    
    if not os.path.exists(filename):
        url = f"{base_url}?type=prices&date={date_str}"
        try:
            response = requests.get(url, timeout=10)
            # Check if we got valid CSV text (not empty, not an HTML error)
            if response.status_code == 200 and len(response.text.strip()) > 50:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                print(f"Downloaded: {date_str}")
                time.sleep(0.1) # Be polite to the server
            else:
                print(f"No data for {date_str} (Status: {response.status_code})")
        except Exception as e:
            print(f"Error downloading {date_str}: {e}")
    
    current_date += timedelta(days=1)

# --- STEP 2: PROCESS & CLEAN DATA ---
print("\nProcessing downloaded files...")
all_files = sorted([f for f in os.listdir(output_dir) if f.endswith('.csv')])
all_dfs = []

for file_name in all_files:
    file_path = os.path.join(output_dir, file_name)
    date_label = file_name.replace("prices_", "").replace(".csv", "")
    
    try:
        # Read the raw CSV
        df = pd.read_csv(file_path)
        
        if df.empty:
            continue

        # 1. DROP THE "NO" SIDE (ODD ROWS)
        # Logic: Row 1 is Header. Row 2 is YES (Index 0). Row 3 is NO (Index 1).
        # Want to keep Index 0, 2, 4... (The Evens)
        # Do this BEFORE dropping blanks to preserve the pair structure.
        df_yes_only = df.iloc[::2].copy()
        
        # 2. DROP ROWS WITH BLANK SETTLEMENT PRICE
        # Find the column that contains "Settlement"
        price_col = next((col for col in df_yes_only.columns if "Settlement" in col), None)
        
        if price_col:
            # Drop rows where the price is blank
            df_cleaned = df_yes_only.dropna(subset=[price_col])
        else:
            # If we can't find the column, keep data but warn user
            print(f"Warning: Could not find 'Settlement Price' column in {date_label}")
            df_cleaned = df_yes_only

        # 3. ADD DATE COLUMN
        if not df_cleaned.empty:
            df_cleaned.insert(0, 'Data_Date', date_label)
            all_dfs.append(df_cleaned)
            
    except Exception as e:
        print(f"Skipping {file_name}: {e}")

# --- STEP 3: SAVE MASTER FILE ---
if all_dfs:
    print("\nMerging into Master CSV...")
    # Concatenate all days into one long table
    master_df = pd.concat(all_dfs, ignore_index=True)
    
    master_df.to_csv(master_csv, index=False)
    print(f"Success! Saved filtered data to: {master_csv}")
    print(f"Total rows: {len(master_df)}")
else:
    print("No valid data found.")
