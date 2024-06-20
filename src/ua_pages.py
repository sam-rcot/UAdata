from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
import csv

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = './client_secrets.json'
VIEW_ID = '150538750'


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
    analytics = discovery.build('analyticsreporting', 'v4', credentials=credentials)
    return analytics


def get_report(analytics, page_token=None):
    return analytics.reports().batchGet(
        body={
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": "2014-11-01", "endDate": "400daysAgo"}],
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


def main():
    analytics = initialize_analyticsreporting()
    page_token = None
    file_name = f"UniversalAnalytics_AllPages_{date}.csv"

    while True:
        response = get_report(analytics, page_token)
        write_to_csv(response, file_name)

        # Check if there is another page of data
        page_token = response.get('reports', [])[0].get('nextPageToken', None)
        if not page_token:
            break


if __name__ == '__main__':
    main()