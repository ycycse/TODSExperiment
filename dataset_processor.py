import csv
import gc
from datetime import datetime

from iotdb.utils.IoTDBConstants import TSDataType
from memory_profiler import profile


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
        reader = csv.reader(csv_file)

        headers = next(reader)  
        if len(headers) != 1:
            column_names = []
            column_names.append('Time')   
            for i in range(1,len(headers)):
                column_names.append(headers[i].split('.')[-1])
            table_name = '_'.join(headers[1].split('.')[:-1])
        else:
            table_name, column_names = extract_meta_data(headers[0], split_char)

        data_types = [get_defalult_type(database)] * len(column_names)
        data_types[0] = parse_timestamp_type(database)
        temp_data = []

        for row in reader:
            if len(row) != 1:
                data_temp_items = ','.join(row).split(split_char)
            else:
                data_temp_items = row[0].split(split_char)

            if len(data_temp_items) != len(column_names):
                print("skip one row")
                continue
            data_items = [] 
            for i, data_item in enumerate(data_temp_items):
                if i == 0:
                    cur_timestamp = parse_timestamp(data_item)
                    data_items.append(cur_timestamp)
                    continue
                if data_item == '':
                    data_items.append(None)
                else:
                    data_items.append(data_item)
                    data_types[i] = parse_data_type(data_item, database)

            if len(data_items) != 0:
                temp_data.append(data_items)
    sorted_data = sorted(temp_data, key=lambda x: x[0])
    temp_data = None
    if not check_sorted(sorted_data):
        raise ValueError("Data is not sorted")

    print(f"Table name: {table_name}", flush=True)
    gc.collect()
    return table_name, sorted_data, data_types, column_names

def check_sorted(sorted_data):
    for i in range(1, len(sorted_data)):
        if sorted_data[i][0] < sorted_data[i-1][0]:
            return False
    return True


def parse_timestamp_type(database:str):
    if database == "sqlite":
        return 'INTEGER'
    elif database == "duckdb":
        return 'BIGINT'
    else:
        return TSDataType.INT64
    

def get_defalult_type(database:str):
    if database == "sqlite":
        return 'TEXT'
    elif database == "duckdb":
        return 'VARCHAR'
    else:
        return TSDataType.TEXT

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


if __name__ == "__main__":
    file_path = "/data/ty_csv/root.cty.trans.07.1001202557.88002510504/s0.csv"
    table_nam, data, data_types, column_names = read_csv_files(file_path, "iotdb", '@')
