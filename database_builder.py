
def setup_database(db_module, db_name):
    conn = db_module.connect(db_name)
    cursor = conn.cursor()
    # cursor.execute("drop table timeseriesPoints")
    # cursor.execute('''
    # CREATE TABLE IF NOT EXISTS timeseriesPoints (
    #     timestamp INTEGER,
    #     value DOUBLE
    # )
    # ''')
    # conn.commit()
    return conn, cursor


def setup_database_for_read(db_module, db_name):
    conn = db_module.connect(db_name)
    cursor = conn.cursor()
    return conn, cursor