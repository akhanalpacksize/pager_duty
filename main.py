from fetch_data import fetch_monthly_incidents, fetch_log
from upload_incident import generate_update_schema_list,upload_csv_list
from upload_log import generate_update_schema_log,upload_csv_log
from log_to_csv import convert_to_csv
from upload_log_csv import generate_update_schema,upload_csv


if __name__ == "__main__":
    try:
        fetch_monthly_incidents()
        generate_update_schema_list()
        upload_csv_list()
        fetch_log()
        generate_update_schema_log()
        upload_csv_log()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        convert_to_csv()
        generate_update_schema()
        upload_csv()


