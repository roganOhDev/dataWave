from typing import List

from sqlalchemy.orm import Session

from dto.connection_dto import ConnectionDto
from dto.dag_info_dto import DagInfoDto
from dto.table_list_dto import Table_List_Dto


def make_mysql_raw_code(connection: ConnectionDto, dag: DagInfoDto, table_lists: List[Table_List_Dto],
                        connection_type: str,
                        session: Session) -> str:
    task_id = 'do_extract' if connection_type == 'extract' else 'do_load'
    func_name = 'do_extract' if connection_type == 'extract' else 'do_load'
    table_list_uuids = map(lambda table_list: table_list.uuid, table_lists)
    get_data = '''PythonOperator(
     task_id='{task_id}',
     python_callable=getattr({func_name}, Db_Type.{db_type}.name.lower()),
     op_kwargs = {{'user': "{user}",
            'pwd': "{pwd}",
            'host': "{host}",
            'port': "{port}",
            'database': "{database}",
            'csv_files_directory': "{csv_files_directory}",
            'option': '{option}',
            'dag_id': "{dag_id}",
            'cron_expression': "{cron_expression}",
            'table_list_uuids': "{table_list_uuids}"}},
         dag=dag
     )'''.format(task_id=task_id,
                 func_name=func_name,
                 db_type=connection.db_type,
                 user=connection.user,
                 pwd=connection.password,
                 host=connection.host,
                 port=connection.port,
                 database=connection.database,
                 csv_files_directory=dag.csv_files_directory,
                 option=connection.option,
                 table_list_uuids=table_list_uuids,
                 cron_expression=dag.schedule_interval,
                 dag_id=dag.dag_id,
                 )
    return get_data


def make_snowflake_raw_code(connection: ConnectionDto, dag: DagInfoDto, table_lists: [Table_List_Dto],
                            connection_type: str,
                            session: Session) -> str:
    if connection_type == "ex":
        task_id = 'get_data'
        func_name = 'do_extract'
        database = "tr['database'][0]"
        schema = "tr['schema'][0]"
    elif connection_type == "ld":
        task_id = 'do_load'
        func_name = 'do_load'
        database = "ld_db_information['database']"
        schema = "ld_db_information['schema']"

    get_data = '''PythonOperator(
                task_id='{task_id}',
                python_callable=getattr({func_name},{db_type}),
                op_kwargs = {{'id': {id},
                    'pwd': {pwd},
                    'account': {account},
                    'database': {database},
                    'schema': {schema},
                    'warehouse': {warehouse},
                    'tables': tables,
                    'directory' : {directory},
                    'role' : {role},
                    'columns': {columns},
                    'pk': {pk},
                    'upsert' : {upsert},
                    'dag_id' : {dag_id},
                    'updated' : {updated}}},
                dag=dag
            )'''.format(task_id=task_id,
                        func_name=func_name,
                        db_type=connection_type + "['db_type'][0]",
                        id=connection_type + "_db_information['id']",
                        pwd=connection_type + "_db_information['pwd']",
                        account=connection_type + "_db_information['account']",
                        database=database,
                        schema=schema,
                        warehouse=connection_type + "_db_information['warehouse']",
                        directory=connection_type + "['directory'][0]",
                        role=connection_type + "_db_information['role']",
                        columns="tr['columns'][0]",
                        pk="tr['pk'][0]",
                        upsert="tr['upsert'][0]",
                        dag_id="tr['dag_id'][0]",
                        updated="tr['updated'][0]"
                        )
    return get_data


def make_amazon_raw_code(connection: ConnectionDto, dag: DagInfoDto, table_lists: [Table_List_Dto],
                         connection_type: str,
                         session: Session) -> str:
    if connection_type == 'ex':
        task_id = 'get_data'
        func_name = 'do_extract'
        database = "tr['database'][0]"
        schema = "tr['schema'][0]"
    elif connection_type == 'ld':
        task_id = 'do_load'
        func_name = 'do_load'
        database = "ld_db_information['database']"
        schema = "ld_db_information['schema']"

    get_data = '''PythonOperator(
                 task_id='{task_id}',
                 python_callable=getattr({func_name},{db_type}),
                 op_kwargs = {{'id': {id},
                    'pwd': {pwd},
                    'host': {host},
                    'port': {port},
                    'database': {database},
                    'schema': {schema},
                    'directory': {directory},
                    'tables': tables,
                    'columns': {columns},
                    'pk': {pk},
                    'upsert' : {upsert},
                    'dag_id' : {dag_id},
                    'updated' : {updated}}},
                 dag=dag
             )'''.format(task_id=task_id,
                         func_name=func_name,
                         db_type=connection_type + "['db_type'][0]",
                         id=connection_type + "_db_information['id']",
                         pwd=connection_type + "_db_information['pwd']",
                         host=connection_type + "_db_information['host']",
                         port=connection_type + "_db_information['port']",
                         database=database,
                         schema=schema,
                         directory=connection_type + "['directory'][0]",
                         option=connection_type + "_db_information['option']",
                         columns="tr['columns'][0]",
                         pk="tr['pk'][0]",
                         upsert="tr['upsert'][0]",
                         dag_id="tr['dag_id'][0]",
                         updated="tr['updated'][0]"
                         )


def map_table_info(table_lists: List[Table_List_Dto]) -> [str]:
    uuids = map(lambda table_list : table_list.uuid , table_lists)
    return uuids
