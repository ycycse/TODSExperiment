import argparse
import time
from database_builder import *

def test_query_time(cursor):

    start_time = time.time()
    cursor.execute('SELECT * FROM timeseriesPoints')
    results = cursor.fetchall()
    end_time = time.time()

    query_time = end_time - start_time

    print(f"Number of rows retrieved: {len(results)}")
    print(f"Query time: {query_time:.6f} seconds")

def parse_args():
    parser = argparse.ArgumentParser(description="Run timeseries data query for specified database.")
    parser.add_argument('--database', type=str, choices=['duckdb', 'sqlite','iotdb'], help='Database to run (duckdb or sqlite)')
    return parser.parse_args()

def measure_query(db_module, db_name, sql):
    _, cursor = setup_database_for_read(db_module, db_name)

    start_time = time.time()
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    end_time = time.time()

    query_time = end_time - start_time

    # 打印查询结果数量和查询耗时
    print(f"Query: {sql}")
    print(f"Number of rows retrieved: {len(results)}")
    print(f"Query time: {query_time:.6f} seconds")
    print()

if __name__ == "__main__":
    args = parse_args()
    if args.database == 'duckdb':
        import duckdb
        measure_query(duckdb, 'timeseries.duckdb',"SELECT count(*) FROM root_cty_trans_07_1001202557_88002510504")
    elif args.database == 'sqlite':
        import sqlite3
        measure_query(sqlite3, 'TY_SQLITE.db', "SELECT avg(TY_0002_02_1) FROM root_cty_trans_07_1001202557_88002510504")
    elif args.database == 'iotdb':
        from iotdb_tool import *
        session = create_session()
        measure_query_time(session, "SELECT value FROM root.test")
        measure_query_time(session, "SELECT value FROM root.test WHERE time > 7000000 and time < 8000000")

       