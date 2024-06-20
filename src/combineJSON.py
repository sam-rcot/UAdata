import pandas as pd
import json

# Input CSV file
input_csv = '../combined_analytics.csv'
# Output JSON file
output_json = './combined_analytics.json'

# Read the CSV file into a DataFrame
df = pd.read_csv(input_csv)

# Convert the DataFrame to a dictionary
data = df.to_dict(orient='records')

# Save the dictionary to a JSON file
with open(output_json, 'w') as file:
    json.dump(data, file, indent=2)

print(f"Converted {input_csv} to {output_json}")