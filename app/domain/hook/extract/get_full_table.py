from typing import List

import pandas as pd
from mysql.connector.abstracts import MySQLConnectionAbstract

from app.domain.utils.list_converter_util import *
from app.exception.api_exception import ApiException


def get_full_table_snowflake(i, metadatas, engine):
    filename = metadatas.dag_id[i]
    indata = pd.read_sql_query(
        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=metadatas.column_info[i].lower(),
                                                                           table=filename.lower(),
                                                                           database=metadatas.database.lower(),
                                                                           schema=metadatas.schema.lower(),
                                                                           dag_id=metadatas.dag_id.lower()), engine)
    indata.to_csv(metadatas.csv_files_directory + '/' + metadatas.dag_id + '_' + filename + '.csv', sep=',',
                  quotechar="'", na_rep='NaN', index=False)


def get_full_table_amazon(i, metadatas, engine):
    filename = metadatas.dag_id[i]
    indata = pd.read_sql_query(
        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=metadatas.column_info[i],
                                                                           table=filename,
                                                                           database=metadatas.database,
                                                                           schema=metadatas.schema,
                                                                           dag_id=metadatas.dag_id), engine)
    indata.to_csv(metadatas.csv_files_directory + '/' + metadatas.dag_id + '_' + filename + '.csv', sep=',',
                  quotechar="'", na_rep='NaN', index=False)


def get_full_table_mysql(csv_files_directory: str, dag_id: str, connection: MySQLConnectionAbstract,
                         column_list: List[str], table: str):
    column_str = convert_str_list_to_string(column_list)

    try:
        sql_select = "select %s from %s"
        sql_data = (column_str, table)
        cursor = connection.cursor()
        cursor.execute(sql_select, sql_data)
        records = cursor.fetchall()
    except Exception as e:
        raise ApiException(str(e))

    data = pd.DataFrame(records)

    data.to_csv(csv_files_directory + '/' + dag_id + '_' + table + '.csv', sep=',', quotechar="'", na_rep='NaN',
                index=False)
