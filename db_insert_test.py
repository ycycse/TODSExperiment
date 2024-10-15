import argparse
import time
import os
import random
from database_builder import setup_database
from dataset_processor import read_csv_files


def generate_ordered_timeseries_data(num_points, start_time):
    return [(start_time + i, round(random.uniform(0.0, 100.0), 2)) for i in range(num_points)]

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

def run_insertions(db_module, db_name, folder_path):
    conn, cursor = setup_database(db_module, db_name)
    print("start inserting test", flush=True)
        # data = generate_ordered_timeseries_data(num_points, start_time)
    total_points = 0
    total_time_cost = 0
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                create_table_sql, sql, data = read_csv_files(file_path)
                total_points += len(data)
                total_time_cost += measure_insert_time(cursor, create_table_sql, sql, data, conn)

                print("Avg throughput: ", total_points / total_time_cost, flush=True)

    conn.close()
    print(f"{db_module}: Total time to insert : {total_time_cost:.2f} seconds.", flush=True)
    with open("result.txt", 'a') as file:
        file.write(f"ty_dataset_insert: {total_time_cost:.2f} seconds")

# for iotdb
def run_iotdb_insertions(batch_size, total_inserts, start_time):
    session = create_session()
    total_time_cost = 0
    for _ in range(total_inserts):
        total_time_cost += measure_insert_time("root.test", session, batch_size, start_time)
        start_time += batch_size
    # flush_iotdb_buffer(session)
    close_session(session)
    print(f"IoTDB: Total time to insert {batch_size} points x {total_inserts} times: {total_time_cost:.2f} seconds.")

def parse_args():
    parser = argparse.ArgumentParser(description="Run timeseries data insertion for specified database.")
    parser.add_argument('--database', type=str, choices=['duckdb', 'sqlite','iotdb'], help='Database to run (duckdb or sqlite)')
    parser.add_argument('--batch_size', type=int, help='Number of points to insert per batch', default=100000)
    parser.add_argument('--total_inserts', type=int, help='Total number of times to insert batch_size points', default=100)
    parser.add_argument('--file_path', type=str, help='Path to the csv file')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    batch_size = args.batch_size
    total_inserts = args.total_inserts
    file_path = args.file_path
    if args.database == 'duckdb':
        print("start duckdb inserting test", flush=True)
        import duckdb
        run_insertions(duckdb, 'TY.duckdb', file_path)
    elif args.database == 'sqlite':
        print("start sqlite inserting test", flush=True)
        import sqlite3
        run_insertions(sqlite3, 'TY_SQLITE.db', file_path)
    elif args.database == 'iotdb':
        from iotdb_tool import *
        run_iotdb_insertions(batch_size, total_inserts, 0)
    else:
        print("Database not supported. Please choose from duckdb, sqlite, or iotdb.")
