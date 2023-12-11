#!/usr/bin/env python

import pandas as pd
import hashlib
import datetime
import random
import os

# Function to generate an MD5 hash of the current timestamp
def generate_unique_id(start_date):
    global unique_id_counter
    timestamp = start_date
    encode_input = f"{timestamp}_{unique_id_counter}"
    encoded = encode_input.encode()
    unique_id = hashlib.md5(encoded).hexdigest()
    unique_id_counter += 1
    return unique_id

# Function to get the day of the week as an integer (1=Monday, 7=Sunday)
def get_day_of_week(date):
    return date.isoweekday()

def generate_random_userid():
    return random.randint(1, 4)

def generate_random_login_amount():
    return random.randint(1, 100)

def generate_login_data(iterated_date, num_rows):
    all_logins = []
    
    data = {
        "id": [generate_unique_id(iterated_date) for _ in range(num_rows)],
        "logintimestamp": [iterated_date for i in range(num_rows)],
        "dayofweek": [get_day_of_week(iterated_date) for i in range(num_rows)],
        "userid": [generate_random_userid() for _ in range(num_rows)]
    }
    all_logins.append(data)
    
    return pd.concat([pd.DataFrame(data) for data in all_logins], ignore_index=True)

###############################################################

# Makes sure that each ID will be unique.
unique_id_counter = 1

# Set the working directory to the script's directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# File to store the data
csv_file = "../seeds/raw_logins.csv"

# Get the start date from the last recorded date in the CSV file
if os.path.exists(csv_file):
    df_existing = pd.read_csv(csv_file)
    last_date = pd.to_datetime(df_existing['logintimestamp']).max()
    start_date = last_date + datetime.timedelta(days=1)
else:
    # If the file doesn't exist, start from the specified date
    start_year_input = int(input("Enter starting year: "))
    start_month_input = int(input("Enter starting month: "))
    start_day_input = int(input("Enter starting day: "))
    start_date = datetime.datetime(start_year_input, start_month_input, start_day_input)
    
# Generate login data for each day in selected range, defaults to 7 days.
range_input = input("Enter the number of days to generate logins for: ")
range_value = int(range_input) if range_input.isdigit() else 7
period = [i for i in range(range_value)]
week_df = pd.DataFrame()
for day in period:
    iterated_date = start_date + datetime.timedelta(days=day)
    num_rows = generate_random_login_amount()
    df = generate_login_data(iterated_date, num_rows)
    week_df = pd.concat([week_df, df])

# Append the data to the CSV file
week_df.to_csv(csv_file, mode='a', header=not os.path.exists(csv_file), index=False)
generated_rows = len(week_df)
print(f"Generated {generated_rows} logins for {range_value} days.")