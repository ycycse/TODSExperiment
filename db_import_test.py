import argparse
import time
import random
from database_builder import setup_database

def generate_ordered_timeseries_data(num_points, start_time):
    return [(start_time + i, round(random.uniform(0.0, 100.0), 2)) for i in range(num_points)]

# for sqlite and duckdb
def measure_insert_time(cursor, data, conn):
    start_time = time.time()
    cursor.execute("BEGIN")
    cursor.executemany('INSERT INTO timeseriesPoints (timestamp, value) VALUES (?, ?)', data)
    cursor.execute("END")
    conn.commit() 
    end_time = time.time()
    return end_time - start_time

def run_insertions(db_module, db_name, num_points, total_inserts, start_time):
    conn, cursor = setup_database(db_module, db_name)
    total_time_cost = 0

    for _ in range(total_inserts):
        data = generate_ordered_timeseries_data(num_points, start_time)

        total_time_cost += measure_insert_time(cursor, data, conn)
 
        start_time += num_points

    conn.close()
    print(f"IoTDB: Total time to insert {num_points} points x {total_inserts} times: {total_time_cost:.2f} seconds.")

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
    return parser.parse_args()

if __name__ == "__main__":
    # res = '2021-04-20T23:43:51.000+08:00@@@@@@@@@@@1618933431000@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"88001441783"@@@@@@@@@@-108561@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@1618933322439@@@@@@@@@24805550944@24805442383@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"869270047564777"@1618933431000@@@1618933322439@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@1618933431000@@@@@@@"{'La':0.0,'Lo':0.0,'Satellite':0,'Speed':0,'Direction':0,'GSMSignal':23}"@0.2421875@29.261719@3@1618933429000@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"10553A"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"2E"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@1@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
    res = '2021-04-20T23:43:52.000+08:00@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@1618933433000@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"{"ERC_Direction":0,"ERC_Satellite":0,"ERC_MsgTime":1618933433000,"ERC_Speed":0,"ERC_DurationTime":2.0,"ERC_GSMSignal":23,"ERC_BgnSpeed":0.2421875,"ERC_EndSpeed":19.58203125,"ERC_Lo":0.0,"ERC_La":0.0}"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@1618933433000@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@1618933323408@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
    # res = '2021-04-20T23:43:52.000+08:00@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@1618933433000@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"{"ERC_Direction":0ERC_Satellite:0ERC_MsgTime:1618933433000ERC_Speed:0ERC_DurationTime:2.0ERC_GSMSignal:23ERC_BgnSpeed:0.2421875ERC_EndSpeed:19.58203125ERC_Lo:0.0ERC_La:0.0}"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@1618933433000@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@1618933323408@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
    print(res.count("@"))
    # args = parse_args()
    # batch_size = args.batch_size
    # total_inserts = args.total_inserts
    # if args.database == 'duckdb':
    #     import duckdb
    #     run_insertions(duckdb, 'timeseries.duckdb', batch_size, total_inserts, 0)
    # elif args.database == 'sqlite':
    #     import sqlite3
    #     run_insertions(sqlite3, 'timeseries.db', batch_size, total_inserts, 0)
    # elif args.database == 'iotdb':
    #     from iotdb_tool import *
    #     run_iotdb_insertions(batch_size, total_inserts, 0)
    # else:
    #     print("Database not supported. Please choose from duckdb, sqlite, or iotdb.")