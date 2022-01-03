from datetime import datetime
from typing import List

import pandas as pd
from croniter import croniter
from mysql.connector.abstracts import MySQLConnectionAbstract

from common.utils.list_converter import convert_str_list_to_string
from exception.engine_exception import EngineException


def get_data_by_cron_expression_mysql(csv_files_directory: str, dag_id: str, connection: MySQLConnectionAbstract,
                                      column_list: List[str], table: str, updated_column: str, cron_expression: str):
    column_str = convert_str_list_to_string(column_list)
    before_time = get_before_update_time(cron_expression)

    try:
        sql_select = "select %s from %s where %s >= %s"
        sql_data = (column_str, table, updated_column, str(before_time))
        cursor = connection.cursor()
        cursor.execute(sql_select, sql_data)
        records = cursor.fetchall()
    except Exception as e:
        raise EngineException(str(e))

    data = pd.DataFrame(records)

    data.to_csv(csv_files_directory + '/' + dag_id + '_' + table + '.csv', sep=',', quotechar="'", na_rep='NaN',
                index=False)


def get_before_update_time(cron_expression: str) -> datetime:
    base = datetime.now()
    cron = croniter(cron_expression, base)
    return cron.get_prev(datetime)