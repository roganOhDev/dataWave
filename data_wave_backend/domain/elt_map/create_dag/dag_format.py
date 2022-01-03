dag_format = """
# -*- coding:utf-8 -*-
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from data_wave_engine.enum.db_type import Db_Type
from data_wave_engine.hook.extract import do_extract
from data_wave_engine.hook.load import do_load

default_args = {{
    'owner': "{owner}",
    'depends_on_past': False,
    'start_date': "{start_date}"
}}

dag = DAG(
    dag_id= "{dag_id}",
    default_args=default_args,
    catchup="{catchup}",
    schedule_interval="{schedule_interval}")
    
get_data = {extract_task}
do_load = {load_task}
get_data >> do_load
if __name__ == '__main__':
  dag.clear(reset_dag_runs=True)
  dag.run()
"""
