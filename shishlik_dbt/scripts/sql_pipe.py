#!/usr/bin/env python

from sqlalchemy import create_engine
from sqlalchemy import text
# import pyodbc
import os 
import sys
import pandas as pd

"""
"""
# Use these to run sql on other databases passing the specs to the command line
import argparse
parser = argparse.ArgumentParser(description=f"""
                                 Execute SQL text from filename against SQL Server Instance with results in stdout.
                                 """)
parser.add_argument('filename', type=str, help='file containing SQL Text to run')
parser.add_argument('--server', type=str, help='Server URI', required=True)
parser.add_argument('--port', type=str, help='Connection Port', required=True)
parser.add_argument('--database', type=str, help='Database name', required=True)
parser.add_argument('--schema', type=str, help='Schema name', required=True)
parser.add_argument('--username', type=str, help='Username', required=True)
parser.add_argument('--password', type=str, help='Password', required=True)
args = parser.parse_args()

server = args.server 
port = args.port 
database = args.database
schema = args.schema 
username = args.username 
password = args.password 
# Microsoft SQL Server
driver = 'ODBC Driver 18 for SQL Server'

engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server},{port}/{database}?driver={driver}')

with engine.connect() as con:
    with open(args.filename) as f:
        query = text(f.read())
        result = con.execute(query)
    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()
    for s in df["text_line"]:
        print(s)
