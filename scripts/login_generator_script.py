#!/usr/bin/env python

import pandas as pd
import hashlib
import datetime
import random
import os

# Function to generate an MD5 hash of the current timestamp
# Makes sure that each ID will be unique.
unique_id_counter = 1

# Function to get the current timestamp
def get_current_timestamp():
    return str(datetime.datetime.now())

def generate_unique_id():
    global unique_id_counter
    timestamp = get_current_timestamp()
    encode_input = f"{timestamp}_{unique_id_counter}"
    encoded = encode_input.encode()
    unique_id = hashlib.md5(encoded).hexdigest()
    unique_id_counter += 1
    return unique_id

# Function to get the day of the week as an integer (1=Monday, 7=Sunday)
def get_day_of_week():
    return datetime.datetime.today().isoweekday()

def generate_random_userid():
    return random.randint(1,4)

num_rows = 100

# Dict to store data
data = {
    "id": [generate_unique_id() for _ in range(num_rows)],
    "logintimestamp": [get_current_timestamp() for _ in range(num_rows)],
    "dayofweek": [get_day_of_week() for _ in range(num_rows)],
    "userid": [generate_random_userid() for _ in range(num_rows)]
}

# Set the working directory to the script's directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# File to store the data
csv_file = "../seeds/raw_logins.csv"

# Check if the file already exists
file_exists = os.path.isfile(csv_file)

df = pd.DataFrame(data)

# Append the data to the CSV file
if file_exists:
    df.to_csv(csv_file, mode='a', header=False, index=False)
else:
    df.to_csv(csv_file, index=False)

print(df)