# Import required libraries
import pandas as pd
from datetime import datetime
import pytz
import os

month = ["202312", "202401", "202402", "202403", "202404", "202405", "202406", "202407", "202408", "202409", "202410", "202411"]

for i in month:
    # Assign month to variable for file naming
    month_str = i

    # Define file path
    file_path = r"C:/Users/VEEPL/Documents/divvy-tripdata/" + month_str + "-divvy-tripdata.csv"

    print(file_path)

    # Read the dataset
    data = pd.read_csv(file_path)

    # Check if 'ride_id' column exists
    if 'ride_id' not in data.columns:
        raise ValueError(f"Error: `ride_id` column not found in file for month {month_str}.")

    # Clean and preprocess the data
    data_cleaned = data[
        (data['ride_id'].notna()) & (data['ride_id'] != '') &
        (data['started_at'].notna()) &
        (data['ended_at'].notna())
        ].copy()

    # Convert started_at and ended_at to datetime format
    data_cleaned['started_at'] = pd.to_datetime(data_cleaned['started_at'], utc=True)
    data_cleaned['ended_at'] = pd.to_datetime(data_cleaned['ended_at'], utc=True)

    # Remove rows where both start_station_name and start_station_id are missing
    data_cleaned = data_cleaned.dropna(subset=['start_station_name', 'start_station_id'], how='all')

    # Remove rows where both end_station_name and end_station_id are missing
    data_cleaned = data_cleaned.dropna(subset=['end_station_name', 'end_station_id'], how='all')

    # Sort the dataset by started_at
    data_cleaned = data_cleaned.sort_values('started_at')

    # Calculate ride_length and day_of_week
    data_cleaned['ride_length'] = (data_cleaned['ended_at'] - data_cleaned['started_at']).dt.total_seconds()
    data_cleaned['day_of_week'] = data_cleaned['started_at'].dt.day_name()

    # Remove rows with negative ride_length
    data_cleaned = data_cleaned[data_cleaned['ride_length'] >= 0]

    # Format ride_length as HH:MM:SS
    data_cleaned['ride_length'] = data_cleaned['ride_length'].apply(
        lambda x: f"{int(x // 3600):02d}:{int((x % 3600) // 60):02d}:{int(x % 60):02d}"
    )

    # View the first few rows of the cleaned and processed data
    print(f"Processed data for month {month_str}:")
    print(data_cleaned.head())

    # Check if the directory exists
    if not os.path.exists(r"C:/Users/VEEPL/Documents/Pcleaned_data"):
        # If the directory does not exist, create it
        os.makedirs(r"C:/Users/VEEPL/Documents/Pcleaned_data")

    # Export the cleaned dataset to a CSV file
    # Adjust the filename to match the current month being processed
    output_file = r"C:/Users/VEEPL/Documents/Pcleaned_data/cleaned_2024" + month_str + ".csv"
    data_cleaned.to_csv(output_file, index=False)