from fetch_data import fetch_monthly_incidents, fetch_log
from upload_incident import generate_update_schema_list,upload_csv_list
from upload_log import generate_update_schema_log,upload_csv_log

if __name__ == "__main__":
    fetch_monthly_incidents()
    generate_update_schema_list()
    upload_csv_list()
    fetch_log()
    generate_update_schema_log()
    upload_csv_log()