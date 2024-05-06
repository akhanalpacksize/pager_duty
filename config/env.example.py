# GETS JSON DATA FROM API AND SAVES IT LOCALLY
def get_data_from_devops_api():
    try:
        for api_name, api_url_dict in API_URL.items():
            for api_method, url in api_url_dict.items():
                file_name = api_name + '_' + api_method + '.json'
                # EVERY AZURE DEVOPS API MAY SUPPORT DIFFERENT QUERY PARAMS.
                # SO RETRIEVING FUNCTION TO USE  FOR API FROM "API_TO_FUNCTION_MAP" DICT
                # E.G COMMIT API USES "get_azure_devops_commit_data" WHEREAS DEPLOYMENT API USES "get_azure_devops_data"
                if api_name in API_TO_FUNCTION_MAP.keys():
                    api_function = API_TO_FUNCTION_MAP.get(api_name)
                    result = api_function(url, file_name)
                else:
                    result = get_azure_devops_data(url, file_name)
                json_data = json.dumps(result)
                with open(os.path.join(API_RESPONSE_DIR, file_name), "w") as outfile:
                    outfile.write(json_data)
        logger.info("Successfully retrieved data from devops API")
    except Exception as e:
        logger.error(f'Error Occurred: {e} \n\r', exc_info=True)
        error_message = f'Error Occurred: {e} \n\r'
        send_email_error(error_message)


create_folder_if_does_not_exist(API_RESPONSE_DIR)
# PULLS DATA FROM AZURE
