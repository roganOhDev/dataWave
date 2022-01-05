from typing import List

from sqlalchemy.orm import Session

from dto.connection_dto import ConnectionDto
from dto.job_info_dto import JobInfoDto
from dto.table_list_dto import Table_List_Dto


def make_mysql_raw_code(connection: ConnectionDto, job: JobInfoDto, table_lists: List[Table_List_Dto],
                        session: Session) -> str:
    table_list_uuids = list(map(lambda table_list: table_list.uuid, table_lists))
    get_data = '''
    "{job_id}", "{user}", "{pwd}", "{host}", "{port}", "{database}", "{csv_files_directory}", "{cron_expression}", {table_list_uuids}, "{option}"
    '''.format(db_type=connection.db_type,
               user=connection.user,
               pwd=connection.password,
               host=connection.host,
               port=connection.port,
               database=connection.database,
               csv_files_directory=job.csv_files_directory,
               option=connection.option,
               table_list_uuids=table_list_uuids,
               cron_expression=job.schedule_interval,
               job_id=job.job_id,
               )
    return get_data


def make_snowflake_raw_code(connection: ConnectionDto, job: JobInfoDto, table_lists: [Table_List_Dto],
                            session: Session) -> str:
    connection_type = "ex"
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
                    'job_id' : {dag_id},
                    'updated' : {updated}}},
                job=job
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
                        job_id="tr['dag_id'][0]",
                        updated="tr['updated'][0]"
                        )
    return get_data


def make_amazon_raw_code(connection: ConnectionDto, job: JobInfoDto, table_lists: [Table_List_Dto],
                         session: Session) -> str:
    connection_type = 'ex'
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
                    'job_id' : {dag_id},
                    'updated' : {updated}}},
                 job=job
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
                         job_id="tr['dag_id'][0]",
                         updated="tr['updated'][0]"
                         )


def map_table_info(table_lists: List[Table_List_Dto]) -> [str]:
    uuids = map(lambda table_list: table_list.uuid, table_lists)
    return uuids
