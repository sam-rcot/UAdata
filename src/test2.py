"""Hello Analytics Reporting API V4."""

# from apiclient.discovery import build
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
import json

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = './client_secrets.json'
VIEW_ID = '150538750'

build = discovery.build


def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics


def get_report(analytics):
    """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
    return analytics.reports().batchGet(
        body={
            "reportRequests":
                [
                    {
                        "viewId": VIEW_ID,
                        "dateRanges": [
                            {"endDate": "400daysAgo", "startDate": "2014-11-01"}
                        ],
                        "metrics": [
                            {"expression": "ga:pageviews"},
                            {"expression": "ga:sessions"}
                        ],
                        "dimensions": [{"name": "ga:browser"}, {"name": "ga:country"}],
                        "dimensionFilterClauses": [
                            {
                                "filters": [
                                    {
                                        "dimensionName": "ga:browser",
                                        "operator": "EXACT",
                                        "expressions": ["Chrome"]
                                    }
                                ]
                            }
                        ]
                    }
                ]
        }
    ).execute()


def print_response(response):
    """Parses and prints the Analytics Reporting API V4 response.

  Args:
    response: An Analytics Reporting API V4 response.
  """
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            for header, dimension in zip(dimensionHeaders, dimensions):
                print(header + ': ', dimension)

            for i, values in enumerate(dateRangeValues):
                print('Date range:', str(i))
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    print(metricHeader.get('name') + ':', value)


def main():
    analytics = initialize_analyticsreporting()
    response = get_report(analytics)
    print_response(response)
    with open("report.json", "w") as f:
        json.dump(response, f, indent=2)
    # print("Hello world!")


if __name__ == '__main__':
    main()
