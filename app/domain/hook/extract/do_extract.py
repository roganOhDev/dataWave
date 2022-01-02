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
from typing import List

import mysql.connector
import sqlalchemy as sql

from app.domain.hook.extract.get_data_from_max_val import get_data_from_max_val
from app.domain.hook.extract.get_db_info import get_db_info
from app.domain.hook.extract.get_full_table import *
from get_information_from_user.make_a_dag import get_backend
from get_information_from_user.structs import User_All_Data
from app.domain.elt_map.rule_set import Rule_Set

backend_engine = sql.create_engine(get_backend())


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
            get_data_from_max_val(engine, i, ds, db_information, metadatas)


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
            get_data_from_max_val(engine, i, ds, db_information, metadatas)


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
            get_data_from_max_val(engine, i, ds, db_information, metadatas)


def mysql(dag_id: str, user: str, pwd: str, host: str, port: str, database: str, tables: List[str], csv_files_directory: str,
          columns: List[List[str]], pk: List[str], upsert: List[int], updated, option: str):

    user_data = User_All_Data(dag_id, user, pwd, columns, pk, updated, upsert, tables, database, csv_files_directory,
                              option=option, host=host, port=port)

    connection = mysql.connector.connect(host=host, database=database, user=user, password=pwd, use_unicode=True,
                                         charset=option)

    for table, pk, column, rule_set in tables, pk, columns, upsert:
        if rule_set == Rule_Set.TRUNCATE.value:
            get_full_table_mysql(csv_files_directory, dag_id, connection, column, table)

        elif rule_set in (Rule_Set.INCREASEMENT.value, Rule_Set.MERGE.value):
            get_data_from_max_val(engine, i, ds, db_information, user_data)
