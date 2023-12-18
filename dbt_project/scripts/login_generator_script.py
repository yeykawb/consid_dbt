#!/usr/bin/env python

import datetime
import pandas as pd
import random
import os
import uuid

def generate_login_data(iterated_date, num_rows):
    all_logins = []
    
    data = {
        "id": [str(uuid.uuid4()) for _ in range(num_rows)],
        "logintimestamp": [iterated_date for i in range(num_rows)],
        "userid": [random.randint(1, 4) for _ in range(num_rows)]
    }
    all_logins.append(data)
    
    return pd.concat([pd.DataFrame(data) for data in all_logins], ignore_index=True)

def main():
    
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
    
    num_rows_input = input("Enter the number of logins to generate for each day: ")
    num_rows_value = int(num_rows_input) if num_rows_input.isdigit() else 10
    
    period = [i for i in range(range_value)]
    week_df = pd.DataFrame()
    for day in period:
        iterated_date = start_date + datetime.timedelta(days=day)
        num_rows = random.randint(1, num_rows_value)
        df = generate_login_data(iterated_date, num_rows)
        week_df = pd.concat([week_df, df])

    # Append the data to the CSV file
    week_df.to_csv(csv_file, mode='a', header=not os.path.exists(csv_file), index=False)
    generated_rows = len(week_df)
    print(f"Generated {generated_rows} logins for {range_value} days.")

if __name__ == "__main__":
    main()

