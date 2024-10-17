import random
import time
import gc

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet
from memory_profiler import profile


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

def measure_iotdb_insert_time(session, table_nam, data, data_types, measurements, batch_size=10000):
    data_types_without_time = data_types[1:]
    measurements_without_time = measurements[1:]
    device_id = "root.test." + table_nam

    total_data_points = len(data)    
    total_time = 0
    for i in range(0, total_data_points, batch_size):
        batch_end = min(i + batch_size, total_data_points)

        batch_data = data[i:batch_end]
        timestamps = [value[0] for value in batch_data]
        values = [value[1:] for value in batch_data]

        for value in values:
            for i in range(len(value)):
                if value[i] is None:
                    continue
                type = data_types_without_time[i]
                if type == TSDataType.INT64:
                    value[i] = int(value[i])
                elif type == TSDataType.FLOAT:
                    value[i] = float(value[i])
                elif type == TSDataType.DOUBLE:
                    value[i] = float(value[i])
        tablet = Tablet(device_id, measurements_without_time, data_types_without_time, values, timestamps)
        start_time = time.time()
        session.insert_tablet(tablet)
        end_time = time.time()

        total_time += end_time - start_time

        tablet = None
        batch_data = None
        timestamps = None
        values = None

        gc.collect()
    

    data = None
    data_types = None
    measurements = None
    gc.collect()
    return total_time

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

