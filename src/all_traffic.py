from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
import csv
import pandas as pd
import json
from datetime import datetime, timedelta
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = './client_secrets.json'
VIEW_ID = '150538750'
OUTPUT_DIR = '../data/all_traffic/'
JSON_DIR = '../data/all_traffic/json/'


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
    analytics = discovery.build('analyticsreporting', 'v4', credentials=credentials)
    return analytics


def get_report(analytics, date):
    return analytics.reports().batchGet(
        body={
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": date, "endDate": date}],
                    "metrics": [
                        {"expression": "ga:users"},
                        {"expression": "ga:newUsers"},
                        {"expression": "ga:sessions"},
                        {"expression": "ga:bounceRate"},
                        {"expression": "ga:pageviewsPerSession"},
                        {"expression": "ga:avgSessionDuration"}
                    ],
                    "dimensions": [
                        {"name": "ga:channelGrouping"}
                    ],
                    "pageSize": 10000  # Adjust this value as necessary
                }
            ]
        }
    ).execute()


def write_to_csv(response, file_name):
    with open(file_name, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for report in response.get('reports', []):
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = [entry.get('name') for entry in
                             columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]

            # Write headers only if the file is empty
            if file.tell() == 0:
                writer.writerow(dimensionHeaders + metricHeaders)

            for row in report.get('data', {}).get('rows', []):
                dimensions = row.get('dimensions', [])
                metrics = [value for value in row.get('metrics', [])[0].get('values', [])]
                writer.writerow(dimensions + metrics)


def write_to_json(response, file_name):
    with open(file_name, 'w', encoding='utf-8') as file:
        json.dump(response, file, ensure_ascii=False, indent=4)


def generate_date_ranges(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def process_csv_to_excel(file_name):
    logging.info(f"Processing CSV to Excel for file: {file_name}")
    # Read the CSV file
    df = pd.read_csv(file_name)

    # List of numeric columns to convert
    numeric_columns = ['ga:users', 'ga:newUsers', 'ga:sessions', 'ga:bounceRate', 'ga:pageviewsPerSession',
                       'ga:avgSessionDuration']

    # Convert columns to numeric, replacing commas and handling errors
    for column in numeric_columns:
        # Remove commas (if present) and convert to numeric
        df[column] = pd.to_numeric(df[column].astype(str).str.replace(',', ''), errors='coerce')

    # Sort the DataFrame by ga:sessions
    df = df.sort_values(by='ga:sessions', ascending=False)

    # Save the DataFrame to an Excel file
    excel_file_name = file_name.replace('.csv', '.xlsx')
    df.to_excel(excel_file_name, index=False)
    logging.info(f"Saved Excel file: {excel_file_name}")


def main():
    logging.info("Starting script...")
    analytics = initialize_analyticsreporting()
    start_date = datetime(2017, 5, 15)
    end_date = datetime(2023, 8, 7)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(JSON_DIR, exist_ok=True)

    for date in generate_date_ranges(start_date, end_date):
        date_str = date.strftime('%Y-%m-%d')
        file_name = f"{OUTPUT_DIR}UniversalAnalytics_AllTraffic_{date_str}.csv"
        json_file_name = f"{JSON_DIR}UniversalAnalytics_AllTraffic_{date_str}.json"

        logging.info(f"Fetching data for date: {date_str}")

        response = get_report(analytics, date_str)
        write_to_csv(response, file_name)
        write_to_json(response, json_file_name)

        logging.info(f"Finished fetching data for date: {date_str}")

        # Process the CSV to Excel after fetching all data
        process_csv_to_excel(file_name)

    logging.info("Script finished.")


if __name__ == '__main__':
    main()