# -*- coding:utf-8 -*-

"""Library for extract data from integration db.

This library is made to run a DAG that user made in make_a_dag.py
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

import mysql.connector
import sqlalchemy as sql

from common.utils.json_util import loads
from domain.enums.elt_map import Rule_Set
from domain.services import table
from hook.extract.get_data_by_cron_expression import *
from hook.extract.get_data_by_max_val import *
from hook.extract.get_db_info import get_db_info
from hook.extract.get_full_table import *
from hook.get_information_from_user.structs import User_All_Data


def snowflake(dag_id, user, pwd, account, database, schema, warehouse, tables, directory, columns, pk, upsert, updated,
              role=''):
    """
    :param dag_id: name of user's dag
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

    metadatas = User_All_Data(dag_id, user, pwd, columns, pk, updated, upsert, tables, database, directory,
                              warehouse, role=role, schema=schema, acount=account)

    ds, db_information = get_db_info(dag_id, backend_engine)

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


def postgresql(dag_id, user, pwd, host, port, database, schema, tables, directory, pk, upsert, updated, columns):
    """
    :param dag_id: id for dag which made in make_a_dag.py
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
    metadatas = User_All_Data(dag_id, user, pwd, columns, pk, updated, upsert, tables, database, directory,
                              schema=schema,
                              host=host, port=port)
    ds, db_information = get_db_info(dag_id, backend_engine)

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


def redshift(dag_id, user, pwd, host, port, database, schema, tables, directory, pk, upsert, updated, columns):
    """
    :param dag_id: id for dag which made by make_a_dag.py
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
    metadatas = User_All_Data(dag_id, user, pwd, columns, pk, updated, upsert, tables, database, directory,
                              schema=schema,
                              host=host, port=port)
    ds, db_information = get_db_info(dag_id, backend_engine)

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


def mysql(dag_id: str, user: str, pwd: str, host: str, port: str, database: str, tables: List[str],
          csv_files_directory: str, cron_expression: str, table_list_uuids: List[str], option: str):
    tables, columns, pks, rule_sets, tables_pk_max, updated_columns = map_table_info(table_list_uuids)

    connection = mysql.connector.connect(host=host, database=database, user=user, password=pwd, use_unicode=True,
                                         charset=option)

    for table, pk, column, rule_set, table_pk_max, updated_column in tables, pks, columns, rule_sets, tables_pk_max, updated_columns:
        if rule_set == Rule_Set.TRUNCATE.value or (rule_set == Rule_Set.MERGE.value and cron_expression == "@once"):
            get_full_table_mysql(csv_files_directory, dag_id, connection, column, table)

        elif rule_set == Rule_Set.INCREASEMENT.value:
            get_data_by_max_val_mysql(csv_files_directory, dag_id, connection, table, column, pk, table_pk_max)

        elif rule_set == Rule_Set.MERGE.value:
            get_data_by_cron_expression_mysql(csv_files_directory, dag_id, connection, table, column, updated_column,
                                              cron_expression)


def map_table_info(table_list_uuids: List[str]) -> ([str], [[str]], [str], [int], [int], [str]):
    updated_columns = []
    tables = []
    columns = []
    pks = []
    rule_sets = []
    tables_pk_max = []
    for table_list_uuid in table_list_uuids:
        table_list = table.find(table_list_uuid)
        column_info = loads(table_list.columns_info)
        tables.append(column_info['table_name'])
        columns.append(column_info['columns'])
        pks.append(column_info['pk'])
        rule_sets.append(column_info['rule_set'])
        updated_columns.appnend(table_list.updated_at)  # TODO: updated_column
        tables_pk_max.append(table_list.max_pk)

    return tables, columns, pks, rule_sets, tables_pk_max, updated_columns
