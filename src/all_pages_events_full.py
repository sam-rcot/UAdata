from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
import csv
import pandas as pd
import json
from datetime import datetime
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = './client_secrets.json'
VIEW_ID = '150538750'
OUTPUT_DIR = '../data/all_pages/'
JSON_DIR = '../data/all_pages/json/'

def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
    analytics = discovery.build('analyticsreporting', 'v4', credentials=credentials)
    return analytics

def get_report(analytics, start_date, end_date, page_token=None):
    return analytics.reports().batchGet(
        body={
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": start_date, "endDate": end_date}],
                    "metrics": [
                        {"expression": "ga:pageviews"},
                        {"expression": "ga:uniquePageviews"},
                        {"expression": "ga:avgTimeOnPage"},
                        {"expression": "ga:entrances"},
                        {"expression": "ga:bounceRate"},
                        {"expression": "ga:exitRate"}
                    ],
                    "dimensions": [
                        {"name": "ga:pagePath"}
                    ],
                    "dimensionFilterClauses": [
                        {
                            "filters": [
                                {
                                    "dimensionName": "ga:pagePath",
                                    "operator": "REGEXP",
                                    "not": True,
                                    "expressions": ["\\?.*"]
                                }
                            ]
                        }
                    ],
                    "pageToken": page_token,
                    "pageSize": 10000  # Adjust this value as necessary
                },
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": start_date, "endDate": end_date}],
                    "metrics": [
                        {"expression": "ga:totalEvents"}
                    ],
                    "dimensions": [
                        {"name": "ga:pagePath"},
                        {"name": "ga:eventCategory"},
                        {"name": "ga:eventAction"},
                        {"name": "ga:eventLabel"}
                    ],
                    "pageToken": page_token,
                    "pageSize": 10000  # Adjust this value as necessary
                }
            ]
        }
    ).execute()

def write_to_csv(response, file_name, event_file_name):
    with open(file_name, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for report in response.get('reports', []):
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = [entry.get('name') for entry in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]

            # Write headers only if the file is empty
            if file.tell() == 0:
                writer.writerow(dimensionHeaders + metricHeaders)

            for row in report.get('data', {}).get('rows', []):
                dimensions = row.get('dimensions', [])
                metrics = [value for value in row.get('metrics', [])[0].get('values', [])]
                writer.writerow(dimensions + metrics)

    with open(event_file_name, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for report in response.get('reports', []):
            if report['columnHeader']['dimensions'] == ['ga:pagePath', 'ga:eventCategory', 'ga:eventAction', 'ga:eventLabel']:
                columnHeader = report.get('columnHeader', {})
                dimensionHeaders = columnHeader.get('dimensions', [])
                metricHeaders = [entry.get('name') for entry in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]

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

def process_csv_to_excel(file_name):
    logging.info(f"Processing CSV to Excel for file: {file_name}")
    # Read the CSV file
    df = pd.read_csv(file_name)

    # Define the numeric columns we expect
    numeric_columns = ['ga:pageviews', 'ga:uniquePageviews', 'ga:avgTimeOnPage', 'ga:entrances', 'ga:bounceRate', 'ga:exitRate', 'ga:totalEvents']

    # Convert columns to numeric, replacing commas and handling errors
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column].astype(str).str.replace(',', ''), errors='coerce')

    # If ga:pageviews exists, sort by it
    if 'ga:pageviews' in df.columns:
        df = df.sort_values(by='ga:pageviews', ascending=False)

    # Save the DataFrame to an Excel file
    excel_file_name = file_name.replace('.csv', '.xlsx')
    df.to_excel(excel_file_name, index=False)
    logging.info(f"Saved Excel file: {excel_file_name}")

def main():
    logging.info("Starting script...")
    analytics = initialize_analyticsreporting()
    start_date = '2017-05-15'
    end_date = '2023-08-07'
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(JSON_DIR, exist_ok=True)

    file_name = f"{OUTPUT_DIR}UniversalAnalytics_AllPages_{start_date}_to_{end_date}.csv"
    event_file_name = f"{OUTPUT_DIR}UniversalAnalytics_AllPages_Events_{start_date}_to_{end_date}.csv"
    json_file_name = f"{JSON_DIR}UniversalAnalytics_AllPages_{start_date}_to_{end_date}.json"

    logging.info(f"Fetching data for date range: {start_date} to {end_date}")

    page_token = None
    while True:
        response = get_report(analytics, start_date, end_date, page_token)
        write_to_csv(response, file_name, event_file_name)
        write_to_json(response, json_file_name)

        # Check if there is another page of data
        page_token = response.get('reports', [])[0].get('nextPageToken', None)
        if not page_token:
            break

    logging.info(f"Finished fetching data for date range: {start_date} to {end_date}")

    # Process the CSV to Excel after fetching all data
    process_csv_to_excel(file_name)
    process_csv_to_excel(event_file_name)

    logging.info("Script finished.")

if __name__ == '__main__':
    main()