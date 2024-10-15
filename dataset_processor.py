import csv
from datetime import datetime

from iotdb.utils.IoTDBConstants import TSDataType


def extract_table_name(file_name):
    return file_name.split('_')[0]

def extract_meta_data(header, split_char):
    column_names_list = []
    column_names = header.split(split_char)
    index = 0
    if column_names[index] == 'Time':
        index += 1
        column_names_list.append('Time')
    table_name = '_'.join(column_names[index].split('.')[:-1])
    for i in range(index, len(column_names)):
        column_names_list.append(column_names[i].split('.')[-1])
    return table_name, column_names_list

def read_csv_files(filepath, database:str, split_char:str):
    with open(filepath, mode='r') as csv_file:
        reader = csv.reader(csv_file, delimiter=split_char)

        headers = next(reader)  
        if len(headers) != 1:
            column_names = headers
            table_name = '_'.join(column_names[1].split('.')[:-1])
        else:
            table_name, column_names = extract_meta_data(headers[0], split_char)

        data_types = [None] * len(column_names)
        data_types[0] = 'INTEGER'
        temp_data = []

        for row in reader:
            if len(row) != 1:
                temp_row = "".join(row)
            else:
                temp_row = row[0]
                
            data_temp_items = temp_row.split(split_char)
            if len(data_temp_items) != len(column_names):
                continue
            data_items = [] 
            for i, data_item in enumerate(data_temp_items):
                if i == 0:
                    data_items.append(parse_timestamp(data_item))
                    continue
                if data_item == '':
                    data_items.append(None)
                else:
                    data_items.append(data_item)
                    data_types[i] = parse_data_type(data_item, database)
            temp_data.append(data_items)
    print(f"Table name: {table_name}", flush=True)
    return table_name, temp_data, data_types, column_names

def parse_data_type(value: str, database: str):
    if database == "sqlite":
        if value.startswith('"'):
            return 'TEXT'
        else:
            return 'REAL'
    elif database == "duckdb":
        if value.startswith('"'):
            return 'VARCHAR'
        else:
            return 'DOUBLE'
    else:
        if value.startswith('"'):
            return TSDataType.TEXT
        else:
            return TSDataType.DOUBLE
    
def parse_timestamp(value:str):
    dt = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f%z')
    return int(dt.timestamp())