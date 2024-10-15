#!/bin/bash

# nohup python3 db_insert_test.py --database sqlite --file_path /data/ty_csv > ty_sqlite_output.txt 2>&1 &&
nohup python3 db_insert_test.py --database duckdb --file_path /data/ty_csv > ty_duckdb_output.txt 2>&1 &
