import csv
import os
from datetime import datetime
import re
from commons import log_csv_dir, log_csv


def extract_log_components(log_line):
    date_pattern = r"\[(.*?)\]"
    status_pattern = r"\b(INFO|ERROR):"
    description_pattern = r":\s(.+)"

    # Extract the date
    date_match = re.search(date_pattern, log_line)
    date = date_match.group(1) if date_match else ""

    # Extract the status
    status_match = re.search(status_pattern, log_line)
    status = status_match.group(1) if status_match else ""

    # Extract the description
    description_match = re.search(description_pattern, log_line)
    description = description_match.group(1) if description_match else ""
    return date, status, description


def convert_to_csv():
    # Create the log_csv_dir if it doesn't exist
    if not os.path.exists(log_csv_dir):
        os.makedirs(log_csv_dir)

    current_dir = os.getcwd()

    # Extract the project directory name from the file path
    project_name = os.path.basename(current_dir)

    # Path to log folder and log_csv_dir
    logs_folder_path = os.path.join(current_dir, 'logs')
    log_csv_path = os.path.join(current_dir, log_csv_dir)

    # List each log file from log folder
    log_files = [f for f in os.listdir(logs_folder_path) if
                 os.path.isfile(os.path.join(logs_folder_path, f)) and f.endswith('.log') and not f.endswith(
                     '.error.log')]

    # Open the CSV file
    csv_file_path = os.path.join(log_csv_path, log_csv)
    with open(csv_file_path, 'w', newline="") as csv_file:
        writer = csv.writer(csv_file)

        # write the header row
        writer.writerow(['Project', 'Date', 'Status', 'Description'])

        # Initialize latest_date_overall
        latest_date_overall = None

        # Process each log file
        for log_file in log_files:
            log_file_path = os.path.join(logs_folder_path, log_file)

            # Open the log file
            with open(log_file_path, "r", encoding="utf-8") as file:
                # Read the content of the log file
                content = file.read()

                # Split the content into lines
                lines = content.splitlines()

                # Iterate through each line to find the latest date in the log file
                latest_date = None
                for line in lines:
                    date_time, _, _ = extract_log_components(line)
                    if date_time:
                        date = date_time.split()[0]  # Extract only the date portion

                        # Convert the dates to datetime objects for comparison
                        current_date = datetime.strptime(date, "%Y-%m-%d")

                        if latest_date is None or current_date > latest_date:
                            latest_date = current_date

                # Update latest_date_overall if a newer date is found
                if latest_date is not None:
                    if latest_date_overall is None or latest_date > latest_date_overall:
                        latest_date_overall = latest_date

                # Extract log components from each line and write to CSV
                for line in lines:
                    date, status, description = extract_log_components(line)
                    if date:
                        current_date = datetime.strptime(date.split()[0], "%Y-%m-%d")  # Extract only the date portion

                        # Check if the log entry date is the overall latest date
                        if current_date.date() == datetime.now().date():
                            writer.writerow([project_name, date, status, description])

        print("Log files converted to individual CSV files successfully.")

