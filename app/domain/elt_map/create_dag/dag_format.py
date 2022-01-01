dag_format =   data = """
# -*- coding:utf-8 -*-
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sqlalchemy as sql
import pandas as pd
import json
from app.domain.hook.extract import do_extract
from app.domain.hook.load import do_load
backend_engine = sql.create_engine('{backend}')
ex = pd.read_sql_query(
        "select * from metadata where dag_id='{dag_id}' and info_type='extract'",
        backend_engine)
ld = pd.read_sql_query(
        "select * from metadata where dag_id='{dag_id}' and info_type='load'",
        backend_engine)
tr = pd.read_sql_query(
        "select * from metadata_tables_to_replicate where dag_id='{dag_id}' and status='on'",
        backend_engine)
ld_db_information = ld['db_information'][0].replace("'", '"')
for i in range(len(ld_db_information)):
    if ld_db_information[i] == '[':
        i = i + 1
        while (ld_db_information[i] != ']'):
            if ld_db_information[i] == '"':
                ld_db_information = ld_db_information[:i] + "'" + ld_db_information[i + 1:]
            i = i + 1
ld_db_information = json.loads(ld_db_information)
ex_db_information = ex['db_information'][0].replace("'", '"')
for i in range(len(ex_db_information)):
    if ex_db_information[i] == '[':
        i = i + 1
        while (ex_db_information[i] != ']'):
            if ex_db_information[i] == '"':
                ex_db_information = ex_db_information[:i] + "'" + ex_db_information[i + 1:]
            i = i + 1
ex_db_information = json.loads(ex_db_information)
default_args = {{
    'owner': ex['owner'][0],
    'depends_on_past': False,
    'start_date': datetime.strptime(ex['start_date'][0],"(%Y,%m,%d)"),
}}
dag = DAG(
    dag_id= ex['dag_id'][0],
    default_args=default_args,
    catchup=(ex['catchup'][0]==True),
    schedule_interval=ex['schedule_interval'][0])
tr_tables=tr['tables']
tables=[]
for item in tr_tables.iteritems():
        tables.append(item[1])
get_data = {extract_task}
do_load = {load_task}
get_data >> do_load
if __name__ == '__main__':
  dag.clear(reset_dag_runs=True)
  dag.run()
"""
