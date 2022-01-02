
# -*- coding:utf-8 -*-
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from app.domain.connection.db_type import Db_Type
from app.domain.hook.extract import do_extract
from app.domain.hook.load import do_load

default_args = {
    'owner': "rogan",
    'depends_on_past': False,
    'start_date': "(2021,12,30)"
}

dag = DAG(
    dag_id= "a2aa",
    default_args=default_args,
    catchup="False",
    schedule_interval="@once")
    
get_data = PythonOperator(
     task_id='do_extract',
     python_callable=getattr(do_extract, Db_Type.MYSQL.name.lower()),
     op_kwargs = {'user': "root",
            'pwd': "fdscbjdcnhd1",
            'host': "127.0.0.1",
            'port': "3306",
            'database': "data_wave",
            'csv_files_directory': "/Users/ohdonggeun/airflow/csv_files",
            'tables': ['a', 'a'],
            'option': '?charset=utf8mb4',
            'columns': [['a', 'b'], ['a', 'b']],
            'pk': ['a', 'b'],
            'upsert': [1, 1],
            'dag_id': "a2aa"},
         dag=dag
     )
do_load = PythonOperator(
     task_id='do_load',
     python_callable=getattr(do_load, Db_Type.MYSQL.name.lower()),
     op_kwargs = {'user': "root",
            'pwd': "fdscbjdcnhd1",
            'host': "127.0.0.1",
            'port': "3306",
            'database': "evil_regex",
            'csv_files_directory': "/Users/ohdonggeun/airflow/csv_files",
            'tables': ['a', 'a'],
            'option': '?charset=utf8mb4',
            'columns': [['a', 'b'], ['a', 'b']],
            'pk': ['a', 'b'],
            'upsert': [1, 1],
            'dag_id': "a2aa"},
         dag=dag
     )
get_data >> do_load
if __name__ == '__main__':
  dag.clear(reset_dag_runs=True)
  dag.run()
