# -*- coding:utf-8 -*-

"""Library for extract data from integration db.

This library is made to run a DAG that user made in make_a_job.py
This library and do_load.py are core of querypie_el

Available functions:
- snowflake: get data from snowflake
- mysql: get data from mysql
- redshift: get data from mysql
- postgresql:get data from postgresql

When merge and increasement, check if tables exist at load db
if not, works like truncate and
else, works get max of updated_at(merge)/pk(increasement) and add sql like 'where updated >= max_updated_at_from_load'
in every function
"""
from datetime import datetime
from typing import Any, List

import mysql.connector as mysql_connector
import pandas as pd
import sqlalchemy as sql
from croniter import croniter

from common.utils.json_util import loads
from common.utils.list_converter import convert_str_list_to_string
from domain.call_api import table_api
from domain.enums.elt_map import Rule_Set
from exception.engine_exception import EngineException
from hook.extract.getdatabycronexpression import GetDataByCronExpression
from hook.extract.get_data_by_max_pk import Get_Data_By_Max_Pk
from hook.extract.get_db_info import get_db_info
from hook.extract.get_full_table import Get_Full_table
from hook.get_information_from_user.structs import User_All_Data
from common.utils.logger import logger


def snowflake(job_id, user, pwd, account, database, schema, warehouse, tables, directory, columns, pk, upsert, updated,
              role=''):
    """
    :param job_id: name of user's dag
    :param user: login_id
    :param pwd: login_password
    :param account: host without .snowflakecomputing.com
    :param database: user's database
    :param schema: user's schema
    :param warehouse: user's warehouse
    :param tables: user's tables
    :rtype: list
    :param directory: directory where csv file will be saved.
    :param columns: user's columns
    :rtype: list
    :param upsert: by this param, this function will run differently.
        :param pk: if upsert=increasement, pk=increase column elif upsert=merg, pk=primary key
        :param updated: if upsert=increasement, updated=date column which will be benchmark
        :rtype: list
    :rtype: list
    :param role: user's role in snowflake
    :return: csv file that user want to execute from db.
    """

    metadatas = User_All_Data(job_id, user, pwd, columns, pk, updated, upsert, tables, database, directory,
                              warehouse, role=role, schema=schema, acount=account)

    ds, db_information = get_db_info(job_id, backend_engine)

    if role == '':
        url_role = role
    else:
        url_role = '&role={}'.format(role)
    engine = sql.create_engine(
        'snowflake://{u}:{p}@{a}/{d}/{s}?warehouse={w}&role={r}'.format(
            u=user,
            p=pwd,
            a=account,
            r=url_role,
            d=database[0].lower(),
            s=schema[0].lower(),
            w=warehouse
        )
    )
    for i in range(len(tables)):
        if upsert[i] == 'truncate':
            get_full_table_snowflake(i, metadatas, engine)

        elif upsert[i] in ('increasement', 'merge'):
            get_data_by_max_val_mysql(engine, i, ds, db_information, metadatas)


def postgresql(job_id, user, pwd, host, port, database, schema, tables, directory, pk, upsert, updated, columns):
    """
    :param job_id: id for dag which made in make_a_dag.py
    :param user: login id
    :param pwd: login password
    :param host: db's host
    :param port: db's port
    :param database: user's database
    :param tables: user's talbes
    :rtype: list
    :param directory: directory for csv files will be saved.
    :param upsert: by this param, this function will run differently.
        :param pk: if upsert=increasement, pk=increase column elif upsert=merg, pk=primary key
        :param updated: if upsert=increasement, updated=date column which will be benchmark
        :rtype: list
    :rtype: list
    :param columns: user's columns
    :return: csv file of data
    """
    metadatas = User_All_Data(job_id, user, pwd, columns, pk, updated, upsert, tables, database, directory,
                              schema=schema,
                              host=host, port=port)
    ds, db_information = get_db_info(job_id, backend_engine)

    url = 'postgresql://{u}:{p}@{h}:{port}/{d}'.format(
        u=user,
        p=pwd,
        h=host,
        port=port,
        d=database[0]
    )
    engine = sql.create_engine(url, client_encoding='utf8')
    for i in range(len(tables)):
        if upsert[i] == 'truncate':
            get_full_table_amazon(i, metadatas, engine)

        elif upsert[i] in ('increasement', 'merge'):
            get_data_by_max_val_mysql(engine, i, ds, db_information, metadatas)


def redshift(job_id, user, pwd, host, port, database, schema, tables, directory, pk, upsert, updated, columns):
    """
    :param job_id: id for job which made by make_a_dag.py
    :param user: login id
    :param pwd: login password
    :param host: db's host
    :param port: db's port
    :param database: user's database
    :param tables: user's tables
    :rtype: list
    :param directory: directory where csv files will be saved
    :param columns: user's columns
    :param upsert: by this param, this function will run differently.
        :param pk: if upsert=increasement, pk=increase column elif upsert=merg, pk=primary key
        :param updated: if upsert=increasement, updated=date column which will be benchmark
        :rtype: list
    :rtype: list
    :param columns: user's columns
    :rypte: list
    :return: csv files for data
    """
    metadatas = User_All_Data(job_id, user, pwd, columns, pk, updated, upsert, tables, database, directory,
                              schema=schema,
                              host=host, port=port)
    ds, db_information = get_db_info(job_id, backend_engine)

    url = 'redshift+psycopg2://{u}:{p}@{h}:{port}/{d}'.format(
        u=user,
        p=pwd,
        h=host,
        port=port,
        d=database[0]
    )
    engine = sql.create_engine(url, client_encoding='utf8')
    for i in range(len(tables)):
        if upsert[i] == 'truncate':
            get_full_table_amazon(i, metadatas, engine)

        elif upsert[i] in ('increasement', 'merge'):
            get_data_by_max_val_mysql(engine, i, ds, db_information, metadatas)


def mysql(job_id: str, user: str, pwd: str, host: str, port: str, database: str, csv_files_directory: str,
          table_list_uuids: List[str], option: str, cron_expression: str):
    tables, columns, pks, rule_sets, tables_pk_max, updated_columns = map_table_info(table_list_uuids)

    connection = mysql_connector.connect(host=host, port=port, database=database, user=user, password=pwd,
                                         use_unicode=True,
                                         charset=option)

    for (table, pk, column_list, rule_set, table_pk_max, updated_column) in zip(tables, pks, columns, rule_sets,
                                                                                tables_pk_max, updated_columns):
        column_list_str = convert_str_list_to_string(column_list)

        if rule_set == Rule_Set.TRUNCATE.value:

            sql_data = ()
            extract(Get_Full_table.MYSQL.format(columns=column_list_str, table=table), sql_data, connection,
                    csv_files_directory, job_id, table)

        elif rule_set == Rule_Set.MERGE.value:

            before_time = get_before_update_time(cron_expression)
            sql_data = (updated_column, str(before_time))
            extract(GetDataByCronExpression.MYSQL.format(columns=column_list_str, table=table), sql_data,
                    connection, csv_files_directory, job_id, table)

        elif rule_set == Rule_Set.INCREASEMENT.value:

            sql_data = (pk, table_pk_max)
            extract(Get_Data_By_Max_Pk.MYSQL.format(columns=column_list_str, table=table), sql_data, connection,
                    csv_files_directory, job_id, table)


def get_before_update_time(cron_expression: str) -> datetime:
    base = datetime.now()
    cron = croniter(cron_expression, base)
    return cron.get_prev(datetime)


def extract(sql_select: str, sql_data: tuple, connection: Any, csv_files_directory: str, job_id: str, table: str):
    try:
        cursor = connection.cursor(prepared=True)
        cursor.execute(sql_select, sql_data)
        field_names = [i[0] for i in cursor.description]
        records = cursor.fetchall()
    except Exception as e:
        raise EngineException(str(e))
    finally:
        cursor.close()

    data = pd.DataFrame(records)

    with open(csv_files_directory + '/' + job_id + '_' + table + '.csv', mode='w') as file:
        file.write(convert_str_list_to_string(field_names) + "\n")

    data.to_csv(csv_files_directory + '/' + job_id + '_' + table + '.csv', mode='a', sep=',', quotechar="'",
                na_rep='NaN',
                index=False, header=False)
    logger.log("{job_id} extract complete".format(job_id=job_id))


def map_table_info(table_list_uuids: List[str]) -> ([str], [[str]], [str], [int], [int], [str]):
    updated_columns = []
    tables = []
    columns = []
    pks = []
    rule_sets = []
    tables_pk_max = []
    for table_list_uuid in table_list_uuids:
        table_list = table_api.find(table_list_uuid)
        column_info = loads(table_list.columns_info)
        tables.append(column_info['table_name'])
        columns.append(column_info['columns'])
        pks.append(column_info['pk'])
        rule_sets.append(column_info['rule_set'])
        updated_columns.append(column_info['update_column_name'])
        tables_pk_max.append(table_list.max_pk)

    return tables, columns, pks, rule_sets, tables_pk_max, updated_columns
