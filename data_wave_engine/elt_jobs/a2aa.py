
# -*- coding:utf-8 -*-
# from domain.enums.db_type import Db_Type
# from hook.extract import do_extract
# from hook.load import do_load
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scheduler import sched

def do_etl():
    # do_load.mysql(
    # "a2aa", "root", "fdscbjdcnhd1", "127.0.0.1", "3306", "data_wave", "/Users/ohdonggeun/airflow/csv_files", "@once", ['f749db31-41a2-4ce7-bc7a-02128b3b85de', '74ceeb0d-16c9-4979-b337-947bd3bd4ae7'], "?charset=utf8mb4"
    # )
    # do_extract.mysql(
    # "a2aa", "root", "fdscbjdcnhd1", "127.0.0.1", "3306", "evil_regex", "/Users/ohdonggeun/airflow/csv_files", "@once", ['f749db31-41a2-4ce7-bc7a-02128b3b85de', '74ceeb0d-16c9-4979-b337-947bd3bd4ae7'], "?charset=utf8mb4"
    # )
    #
    print("aba")
    
def add_job():
    sched.add_job(do_etl, 'cron', second = '30', id = __file__)
