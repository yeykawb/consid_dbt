#!/usr/bin/env python

import pandas as pd
import datetime
import os

raw_people = {
    "id": [1,2,3,4],
    "firstname": ["Jakob", "Stefan", "Rami", "Therese"],
    "lastname": ["Agelin", "Verzel", "Moghrabi", "Olsson"],
    "created_at": [None for _ in range(4)],
    "updated_at": [None for _ in range(4)]
}

raw_people_deleted = {
    "id": [1,2,3,4],
    "deleted": [False for _ in range(4)]
}

# Set the working directory to the script's directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# File to store the data
raw_people_file = "../seeds/raw_people.csv"
raw_people_deleted_file = "../seeds/raw_people_deleted.csv"

# Check if the file already exists
raw_people_file_exists = os.path.isfile(raw_people_file)
raw_people_deleted_file_exists = os.path.isfile(raw_people_deleted_file)

# Load existing data from the CSV file
existing_people_data = pd.read_csv(raw_people_file, keep_default_na=True) if raw_people_file_exists else pd.DataFrame()

raw_people_df = pd.DataFrame(raw_people)
now = str(datetime.datetime.now())

# Set created_at timestamp only if the file is new
if not raw_people_file_exists:
    raw_people_df['created_at'] = now
    raw_people_df['updated_at'] = now
    raw_people_df.to_csv(raw_people_file, index=False)
    
if not raw_people_deleted_file_exists:
    deleted_df = pd.DataFrame(raw_people_deleted)
    deleted_df.to_csv(raw_people_deleted_file, index=False)
    
existing_deleted_people_data = pd.read_csv(raw_people_deleted_file, keep_default_na=True)

# Check if row exists in the source file and update updated_at
if not existing_people_data.empty and pd.Series(raw_people_df['id']).isin(existing_people_data['id']).any():
    merged_df = pd.merge(existing_people_data, existing_deleted_people_data, on='id')
    rows_to_update = (merged_df["deleted"] == True).sum()
    merged_df.loc[merged_df["deleted"] == True, 'updated_at'] = now
    merged_df.drop("deleted", axis=1, inplace=True)
    merged_df.to_csv(raw_people_file, index=False)
    print(f" Updated {rows_to_update} rows.")
else:
    print(f" Added {len(raw_people_df)} rows.")