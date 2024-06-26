from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = './client_secrets.json'
VIEW_ID = '150538750'


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
                        {"name": "ga:channelGrouping"},
                        {"name": "ga:deviceCategory"},
                        {"name": "ga:browser"},
                        {"name": "ga:operatingSystem"}
                    ],
                    "pageSize": 10000  # Adjust this value as necessary
                }
            ]
        }
    ).execute()


def print_response(response):
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = [entry.get('name') for entry in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]

        # Print headers
        headers = dimensionHeaders + metricHeaders
        print("\t".join(headers))

        # Print rows
        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            metrics = [value for value in row.get('metrics', [])[0].get('values', [])]
            print("\t".join(dimensions + metrics))


def generate_date_ranges(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def main():
    logging.info("Starting script...")
    analytics = initialize_analyticsreporting()
    start_date = datetime(2017, 5, 15)
    end_date = datetime(2023, 8, 7)

    for date in generate_date_ranges(start_date, end_date):
        date_str = date.strftime('%Y-%m-%d')
        logging.info(f"Fetching data for date: {date_str}")

        response = get_report(analytics, date_str)
        print_response(response)

        logging.info(f"Finished fetching data for date: {date_str}")

    logging.info("Script finished.")


if __name__ == '__main__':
    main()