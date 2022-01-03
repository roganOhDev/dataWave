import datetime as date
from dataclasses import dataclass
from typing import List

from deprecated import deprecated


@dataclass
@deprecated(version='1.0.0', reason="duplicated")
class dag_info:
    yesterday: date = date.date.today() - date.timedelta(1)
    airflow_home: str = None
    backend_url: str = ''
    dag_id: str = None
    owner: str = 'chequer'
    start_date: str = None
    catchup = 'False'
    schedule_interval: str = '@once'
    csv_files_directory: str = None


@dataclass
@deprecated(version='1.0.0', reason="duplicated")
class db_info:
    db_type: int = None
    host: str = None
    port: str = None
    account: str = None
    id: str = None
    pwd: str = None
    warehouse: str = None
    option: str = None
    role: str = None


@dataclass
class user_data_carrier:
    columns: list = None
    pk: list = None
    updated: list = None
    schema: list = None
    dag_ids: list = None
    status: list = None
    upsert_rule: list = None
    tables: list = None
    status: list = None
    database: list = None
    schema: list = None


@dataclass
class User_All_Data:
    def __init__(self, dag_id: str, id: str, pwd: str, columns: List[List[str]], pk: List[str], updated,
                 upsert_rule: List[int], tables: List[str], database: str, csv_file_directory: str, warehouse: str = '',
                 option: str = '', role: str = '', schema=[], host: str = '', port: str = '', account: str = ''):
        self.dag_id = dag_id
        self.host = host
        self.port = port
        self.id = id
        self.pwd = pwd
        self.columns = columns
        self.pk = pk
        self.updated = updated
        self.upsert_rule = upsert_rule
        self.tables = tables
        self.database = database
        self.csv_files_directory = csv_file_directory
        self.warehouse = warehouse
        self.option = option
        self.role = role
        self.schema = schema
        self.account = account

    dag_id: str = None
    host: str = None
    port: str = None
    account: str = None
    id: str = None
    pwd: str = None
    warehouse: str = None
    option: str = None
    role: str = None
    columns: List[List[str]] = None
    pk: List[str] = None
    updated: list = None
    schema: list = None
    upsert_rule: List[str] = None
    tables: List[str] = None
    database: list = None
    csv_files_directory: str = None
