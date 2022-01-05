job_format = """
# -*- coding:utf-8 -*-
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from data_wave_engine.enum.db_type import Db_Type
from data_wave_engine.hook.extract import do_extract
from data_wave_engine.hook.load import do_load

do_load.{extract_db_type}({extract_args})
do_extract.{load_db_type}({load_args})
"""
