from datetime import datetime
from typing import List

import pandas as pd
from croniter import croniter
from mysql.connector.abstracts import MySQLConnectionAbstract

from common.utils.list_converter import convert_str_list_to_string
from exception.engine_exception import EngineException


class GetDataByCronExpression:
    '''
    mysql : column , table, updated_column, before_time
    snowflake : column , schema, table
    mysql : column , schema, table
    '''
    MYSQL = "select {columns} from {table} where %s >= %s"
    SNOWFLAKE = "select {columns} from {schema}.{table} where %s >= %s"
    AMAZON = "select {columns} from {schema}.{table} where %s >= %s"
