job_format = """
# -*- coding:utf-8 -*-
from domain.enums.db_type import Db_Type
from hook.extract import do_extract
from hook.load import do_load
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scheduler import sched

def do_etl():
    do_extract.{extract_db_type}({extract_args})
    do_load.{load_db_type}({load_args})
    
    
def add_job():
    file_name = (os.path.basename(__file__)).split('.')[0]
    sched.add_job(do_etl, 'cron', {cron}, id=file_name)
    
"""
