#!/usr/bin/env python

import pandas as pd
import datetime
import os

data = {
    "id": [1,2,3,4],
    "firstname": ["Jakob", "Stefan", "Rami", "Therese"],
    "lastname": ["Agelin", "Verzel", "Moghrabi", "Olsson"],
    "created_at": [None for _ in range(4)],
    "updated_at": [None for _ in range(4)],
    "deleted_at": [None for _ in range(4)]
}

# Set the working directory to the script's directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# File to store the data
csv_file = "../seeds/raw_people.csv"

# Check if the file already exists
file_exists = os.path.isfile(csv_file)

# Load existing data from the CSV file
existing_data = pd.read_csv(csv_file, keep_default_na=True) if file_exists else pd.DataFrame()

df = pd.DataFrame(data)
now = str(datetime.datetime.now())

# Set created_at timestamp only if the file is new
if not file_exists:
    df['created_at'] = now
    df['updated_at'] = now
    df.to_csv(csv_file, index=False)
    
# Check if row exists in the source file and update updated_at
if not existing_data.empty and pd.Series(df['id']).isin(existing_data['id']).any():
    df = pd.DataFrame(existing_data)
    rows_to_update = (df["deleted_at"].isnull()).sum()
    df.loc[df["deleted_at"].isnull(), 'updated_at'] = now
    df.to_csv(csv_file, index=False)
    print(f" Updated {rows_to_update} rows.")
else:
    print(f" Added {len(df)} rows.")