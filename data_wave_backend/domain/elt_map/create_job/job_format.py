job_format = """
# -*- coding:utf-8 -*-
from domain.enums.db_type import Db_Type
from hook.extract import do_extract
from hook.load import do_load

do_load.{extract_db_type}({extract_args})
do_extract.{load_db_type}({load_args})
"""
