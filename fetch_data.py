import concurrent
import calendar
import logging
import os
import sys
import time

import requests
import pandas as pd

from config.env import *
from commons import INCIDENTS_LIST, output_dir, incident_file, log_file
from json_to_csv import json_to_dataframe
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from logger_config import setup_logging
from utils import send_email_error

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)

setup_logging(module_name="PagerDuty_list_logs")

logger = logging.getLogger(__name__)


def fetch_monthly_incidents():
    headers = {
        'Accept': 'application/vnd.pagerduty+json;version=2',
        'Content-Type': 'application/json',
        'Authorization': f'Token token={access_token}'
    }

    all_incidents = []

    # Define start and end dates
    start_date = datetime(2018, 1, 1)
    # Set end_date to the last day of the previous month
    end_date = datetime.now().replace(day=1) - timedelta(days=1)
    # Make the API call with the current month's date range
    params = {
        'limit': 100,
        'offset': 0
    }
    while start_date < end_date:
        # Find the last day of the current month
        last_day_of_month = calendar.monthrange(start_date.year, start_date.month)[1]

        # Set since and until parameters for the current month
        since = start_date.strftime('%Y-%m-%d')
        until = datetime(start_date.year, start_date.month, last_day_of_month).strftime('%Y-%m-%d')

        logger.info(f"Fetching incidents from {since} to {until}")
        params['since'] = since
        params['until'] = until

        response = requests.get(INCIDENTS_LIST, headers=headers, params=params)

        try:
            response.raise_for_status()  # Raise an exception for failed HTTP requests

            result = response.json()
            incident_data = result['incidents']
            all_incidents.extend(incident_data)
            more = False
            if not result.get("more"):
                params["offset"] = 0
                start_date = datetime(start_date.year, start_date.month, last_day_of_month) + timedelta(days=1)
            else:
                more = True
                params['offset'] += params.get('limit')
            # Move to the next month
            logger.info(f"Incidents fetched: {len(all_incidents)} with status code {response.status_code}")


        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching incidents: {e}")
            error_message = f"Error fetching incidents: {e}"
            send_email_error(error_message)
            break
        except ValueError as ve:
            logger.error(f"Error parsing JSON response: {ve}")
            error_message = f"Error parsing JSON response: {ve}"
            send_email_error(error_message)
            break
        except Exception as ex:
            logger.error(f"An unexpected error occurred: {ex}")
            error_message = f"An unexpected error occurred: {ex}"
            send_email_error(error_message)
            break

    # Convert to DataFrame and save to CSV
    flatten_json = json_to_dataframe(all_incidents)

    folder_path = output_dir
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    csv_file_path = os.path.join(output_dir, incident_file)
    flatten_json.to_csv(csv_file_path, index=False)

    logger.info(f"Total number of incidents fetched: {len(all_incidents)}")
    return flatten_json


def fetch_log_for_id(id, headers):
    incident_url = f'https://api.pagerduty.com/incidents/{id}/log_entries'
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(incident_url, headers=headers)
            response.raise_for_status()
            response_json = response.json()
            return response_json.get('log_entries', [])
        except Exception as ex:
            logger.error(f"An unexpected error occurred for incident ID {id}: {ex}")
            error_message = f"An unexpected error occurred {id}: {ex}"
            send_email_error(error_message)
            if response.status_code == 429:
                if attempt < retries - 1:
                    retry_after = int(response.headers.get('ratelimit-reset', 1))
                    time.sleep(retry_after)
                    continue
            raise ex
    return []


def fetch_log():
    csv_file_path = os.path.join(output_dir, incident_file)
    df = pd.read_csv(csv_file_path)
    incident_ids = df['id'].drop_duplicates().tolist()

    all_logs = []

    headers = {
        'Accept': 'application/vnd.pagerduty+json;version=2',
        'Content-Type': 'application/json',
        'Authorization': f'Token token={access_token}'
    }

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_id = {executor.submit(fetch_log_for_id, id, headers): id for id in incident_ids}
        for future in concurrent.futures.as_completed(future_to_id):
            id = future_to_id[future]
            try:
                log_entries = future.result()
                all_logs.extend(log_entries)
                logger.info(f"Log entries fetched for incident ID {id}")
            except Exception as e:
                logger.error(f"An error occurred for incident ID {id}: {e}")
                error_message = f"An error occurred for incident {id}: {e}"
                send_email_error(error_message)
                executor.shutdown(wait=False, cancel_futures=True)
                print("EXIT")
                sys.exit(0)

    flatten_json = json_to_dataframe(all_logs)

    folder_path = output_dir
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    csv_file_path = os.path.join(output_dir, log_file)

    flatten_json.to_csv(csv_file_path, index=False)

    logger.info(f"Total number of Logs fetched: {len(all_logs)}")
    return flatten_json
