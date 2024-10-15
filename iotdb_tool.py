from iotdb.Session import Session
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from iotdb.utils.NumpyTablet import NumpyTablet
import random
import time

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

def measure_insert_time(schema_name, session, batch_size, start_time):
    
    data_types = [TSDataType.DOUBLE]
    measurements = ["value"]
    values= [[random.random()] for _ in range(batch_size)]
    timestamps = [start_time + i for i in range(batch_size)]
    
    # session.create_time_series(schema_name + ".value", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY)
    print(f"Inserting {batch_size} points into {schema_name}...with timestamps from {start_time} to {start_time + batch_size - 1}")
    start_time = time.time()

    tablet = Tablet(schema_name, measurements, data_types, values, timestamps)
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

