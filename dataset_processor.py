import csv
from datetime import datetime


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

def read_csv_files(filepath, split_char='@'):
    with open(filepath, mode='r') as csv_file:
        reader = csv.reader(csv_file)
        headers = next(reader)  
        if len(headers) != 1:
            raise ValueError(f"Invalid header: {headers}")
        table_name, column_names = extract_meta_data(headers[0], split_char)
        values_placeholder = ', '.join(['?'] * len(column_names))  # 构建占位符
        temp_sql = f"INSERT INTO {table_name}({', '.join(column_names)}) VALUES ({values_placeholder})"
        temp_data = []

        data_type = [None] * len(column_names)
        data_type[0] = 'INTEGER'
        create_table_sql = None

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
                    data_type[i] = parse_data_type(data_item)
            temp_data.append(data_items)
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name}({', '.join([f'{column_names[i]} {data_type[i]}' for i in range(len(column_names))])})" 
        print(f"Create table sql: {create_table_sql}", flush=True)
    print(f"Table name: {table_name}", flush=True)
    return create_table_sql, temp_sql, temp_data

def parse_data_type(value: str):
    if value.startswith('"'):
        return 'TEXT'
    else:
        return 'REAL'
    
def parse_timestamp(value:str):
    dt = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f%z')
    return int(dt.timestamp())