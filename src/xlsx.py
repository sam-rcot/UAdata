import pandas as pd

# Read the CSV file
df = pd.read_csv("../combined_analytics.csv")

# List of numeric columns to convert
numeric_columns = ['ga:pageviews', 'ga:uniquePageviews', 'ga:avgTimeOnPage', 'ga:entrances', 'ga:bounceRate', 'ga:exitRate']

# Convert columns to numeric, replacing commas and handling errors
for column in numeric_columns:
    # Remove commas (if present) and convert to numeric
    df[column] = pd.to_numeric(df[column].astype(str).str.replace(',', ''), errors='coerce')

# Save the DataFrame to an Excel file
df.to_excel("combined.xlsx", index=False)