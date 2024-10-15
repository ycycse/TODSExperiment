import random
import time

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet


def create_session():
    session = Session.init_from_node_urls(
        node_urls=["127.0.0.1:6667", "127.0.0.1:6668", "127.0.0.1:6669"],
        user="root",
        password="root",
        fetch_size=1024,
        zone_id="UTC+8",
        enable_redirection=True,
    )
    session.open(False)
    return session

def close_session(session):
    session.close()

def measure_iotdb_insert_time(session, table_nam, data, data_types, measurements):

    values= [item[1:] for item in data]
    timestamps = [item[0] for item in data]
    device_id = table_nam.replace('_','.')
    start_time = time.time()
    tablet = Tablet(device_id, measurements, data_types, values, timestamps)
    session.insert_tablet(tablet)
    
    end_time = time.time()
    return end_time - start_time

def flush_iotdb_buffer(session):
    start_time = time.time()
    session.execute_non_query_statement("FLUSH")
    end_time = time.time()
    return end_time - start_time


def measure_query_time(session, sql):
    total_points = 0
    start_time = time.time()
    session_data_set = session.execute_query_statement(sql)
    session_data_set.set_fetch_size(1000000)
    while session_data_set.has_next():
        session_data_set.next()
        # print(session_data_set.next())
        total_points += 1
    end_time = time.time()
    query_time = end_time - start_time
    print(f"Query: {sql}")
    print(f"Number of rows retrieved: {total_points}")
    print(f"Query time: {query_time:.6f} seconds")
    print()

