import json
import logging
import os
import gc
from copy import deepcopy

import pandas

# from commons import output_dir,output_json
from utils import create_folder_if_does_not_exist, chunk_and_merge_dataframe,send_email_error
# SETUP LOGGING
logger = logging.getLogger(__name__)


# CROSS JOINS DATA TO FORM ROW
def cross_join(left, right):
    new_rows = [] if right else left
    for left_row in left:
        for index, right_row in enumerate(right):
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    # triggering garbage collection manually to release unused memory
    return new_rows


# FLATTENS LIST
def flatten_list(data):
    for index, elem in enumerate(data):
        elem['Rank'] = index + 1
        if isinstance(elem, list):
            yield from flatten_list(elem)
        else:
            yield elem


# CONVERTS THE JSON DATA IN PANDAS DATAFRAME
def json_to_dataframe(data_in):
    def flatten_json(data, prev_heading=''):
        if isinstance(data, dict):
            rows = [{}]
            for key, value in data.items():
                if value and "\r\n" in str(value):
                    value = value.replace("\r\n", '-')
                if prev_heading:
                    rows = cross_join(rows, flatten_json(value, prev_heading + '_' + key))
                else:
                    rows = cross_join(rows, flatten_json(value, key))

        elif isinstance(data, list):
            rows = []
            for item in data:
                [rows.append(elem) for elem in flatten_list(flatten_json(item, prev_heading))]
        else:
            rows = [{prev_heading: data}]
        return rows

    data = flatten_json(data_in)
    # triggering garbage collect manually
    gc.collect()
    df = chunk_and_merge_dataframe(data)
    return df

