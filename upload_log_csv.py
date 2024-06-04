import csv
import os
import requests
import logging
from utils import get_access_token, send_email_error
# from utils import get_local_access_token, send_email_error
from commons import log_csv_dir, log_csv
from config.env import BASE_URL, DATASET_LOG
from requests.exceptions import SSLError
from logger_config import setup_logging

# Setup logging
setup_logging(module_name="upload_log_csv")

logger = logging.getLogger(__name__)


def get_csv_file_headers(file_path):
    """Returns the headers of a CSV file as a list."""
    with open(file_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        header = next(csv_reader)
        return header


def generate_schema_columns(header):
    """Returns a list of schema columns based on the headers of a CSV file."""
    updated_schema_column = []

    for elem in header:
        if elem in ['Project', 'Status', 'Description']:
            column_type = "string"
        elif elem == 'Date':
            column_type = "datetime"
        else:
            column_type = "string"

        updated_schema_column.append({
            "type": column_type,
            "name": elem
        })

    return updated_schema_column

# Updates the schema of the dataset
def update_schema(name, updated_schema_column, token):
    payload = {

        "schema": {
            "columns": updated_schema_column
        }
    }
    header = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.put(f'{BASE_URL}/datasets/{DATASET_LOG}', headers=header, json=payload)

    if response.ok:
        logger.info(f"updated_schema '{name}' with ID {DATASET_LOG}")
    else:
        error_message = f"Failed to upload dataset '{name}' with status code {response.status_code}"
        send_email_error(error_message)
        logger.error(f"Failed to upload dataset '{name}' with status code {response.status_code}")
        response.raise_for_status()


def generate_update_schema():
    access_token = get_access_token()
    # access_token = get_local_access_token()
    csv_file_path = os.path.join(os.getcwd(), log_csv_dir, log_csv)

    headers = get_csv_file_headers(csv_file_path)

    updated_schema_column = generate_schema_columns(headers)

    dataset_name = "DataSet Summary"
    try:
        # UPDATE DATASET FOR DOMO
        update_schema(dataset_name, updated_schema_column, access_token)
    except requests.exceptions.RequestException as e:
        error_message = f"An error occurred while creating dataset '{dataset_name}': {e}"
        send_email_error(error_message)
        logger.error(f"An error occurred while creating dataset '{dataset_name}': {e}")


def upload_csv():
    # Get current working directory
    current_directory = os.getcwd()

    # Construct the path to the CSV file within the "jaccard_index_csv_data" directory
    csv_path = os.path.join(current_directory, log_csv_dir, log_csv)

    # Check if the CSV file exists
    if os.path.isfile(csv_path):
        with open(csv_path, encoding='utf-8') as f:
            csv_data = f.read().encode('utf-8')

        header = {
            "Authorization": f"Bearer {get_access_token()}",
            # "Authorization": f"Bearer {get_local_access_token()}",
            "Content-Type": "text/csv",
        }
        try:
            # Upload CSV to dataset
            response = requests.put(f'{BASE_URL}/datasets/{DATASET_LOG}/data?updateMethod=APPEND', headers=header, data=csv_data)

            # Check the API status
            if response.status_code == 204:
                logger.info("Data imported successfully")
            else:
                error_message = f'Error {response.status_code} for file {log_csv}: {response.text}'
                logger.error(f'Error {response.status_code} for file {log_csv}: {response.text}')
                # send_email_error(error_message)
        except SSLError as e:
            error_message = "An SSLError occurred:", e
            logger.error("An SSLError occurred:", e)
            send_email_error(error_message)

    else:
        error_message = f"CSV file {log_csv} not found in {log_csv_dir} folder."
        logger.info(f"CSV file {log_csv} not found in {log_csv_dir} folder.")
        send_email_error(error_message)
