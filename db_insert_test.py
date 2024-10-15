import argparse
import os

from sqlalchemy_utils import table_name

from database_builder import setup_database
from dataset_processor import read_csv_files


# for sqlite and duckdb
def measure_insert_time(cursor, create_table_sql, sql, data, conn):
    cursor.execute(create_table_sql)
    cursor.execute("PRAGMA synchronous = OFF")
    start_time = time.time()
    cursor.execute("BEGIN")
    cursor.executemany(sql, data)
    cursor.execute("COMMIT")
    conn.commit() 
    end_time = time.time()
    return end_time - start_time


def generate_sql(table_nam, data_types, column_names):
    values_placeholder = ', '.join(['?'] * len(column_names))  # 构建占位符
    sql = f"INSERT INTO {table_nam}({', '.join(column_names)}) VALUES ({values_placeholder})"
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_nam}({', '.join([f'{column_names[i]} {data_types[i]}' for i in range(len(column_names))])})"

    return sql, create_table_sql

# for sqlite and duckdb
def run_insertions(db_module, db_name, folder_path, database, split_char):
    conn, cursor = setup_database(db_module, db_name)
    print("start inserting test", flush=True)
    total_points = 0
    total_time_cost = 0
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):

                file_path = os.path.join(root, file)

                table_nam, data, data_types, column_names = read_csv_files(file_path, database, split_char)

                insert_sql, create_table_sql = generate_sql(table_nam, data_types, column_names)

                total_points += len(data)
                total_time_cost += measure_insert_time(cursor, create_table_sql, insert_sql, data, conn)

                print("Avg throughput: ", total_points / total_time_cost, flush=True)

    conn.close()
    print(f"{db_module}: Total time to insert : {total_time_cost:.2f} seconds.", flush=True)
    with open("result.txt", 'a') as file:
        file.write(f"ty_dataset_insert: {total_time_cost:.2f} seconds")

# for iotdb
def run_iotdb_insertions(folder_path, split_char):
    session = create_session()
    total_points = 0
    total_time_cost = 0

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                table_nam, data, data_types, column_names= read_csv_files(file_path, "iotdb", split_char)
                total_points += len(data)
                total_time_cost += measure_iotdb_insert_time(session, table_nam, data, data_types, column_names)

                print("Avg throughput: ", total_points / total_time_cost, flush=True)

    # flush_iotdb_buffer(session)
    close_session(session)
    print(f"IoTDB: Total time to insert iotdb: {total_time_cost:.2f} seconds.", flush=True)

def parse_args():
    parser = argparse.ArgumentParser(description="Run timeseries data insertion for specified database.")
    parser.add_argument('--database', type=str, choices=['duckdb', 'sqlite','iotdb'], help='Database to run (duckdb or sqlite)')
    parser.add_argument('--file_path', type=str, help='Path to the csv file')
    parser.add_argument('--split_char', type=str, default='@')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        if args.database == 'duckdb':
            print("start duckdb inserting test", flush=True)
            import duckdb
            run_insertions(duckdb, 'TY.duckdb', args.file_path, "duckdb", args.split_char)
        elif args.database == 'sqlite':
            print("start sqlite inserting test", flush=True)
            import sqlite3
            run_insertions(sqlite3, 'TY_SQLITE.db', args.file_path, "sqlite", args.split_char)
        elif args.database == 'iotdb':
            print("start iotdb inserting test", flush=True)
            from iotdb_tool import *
            run_iotdb_insertions(args.file_path, args.split_char)
        else:
            print("Database not supported. Please choose from duckdb, sqlite, or iotdb.")
    except Exception as e:
        print(f"Error: {e}", flush=True)
