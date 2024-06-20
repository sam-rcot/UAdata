import pandas as pd
import os
import re

# Directory containing the CSV files
input_dir = '../data/all_pages'
output_file = '../combined_analytics.csv'

# Regular expression to extract date ranges from filenames
date_range_regex = re.compile(r'(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})')

# List to hold individual DataFrames
dfs = []

# Loop through all files in the input directory
for filename in os.listdir(input_dir):
    if filename.endswith('.csv'):
        # Extract the date range from the filename
        match = date_range_regex.search(filename)
        if match:
            start_date, end_date = match.groups()

            # Read the CSV file into a DataFrame
            df = pd.read_csv(os.path.join(input_dir, filename))

            # Add the date range columns
            df['start_date'] = start_date
            df['end_date'] = end_date

            # Append the DataFrame to the list
            dfs.append(df)

# Combine all DataFrames into a single DataFrame
combined_df = pd.concat(dfs, ignore_index=True)

# Save the combined DataFrame to a new CSV file
combined_df.to_csv(output_file, index=False)

print(f"Combined CSV file saved to {output_file}")