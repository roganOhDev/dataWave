import datetime as date
from datetime import datetime

from pydantic import BaseModel



class DagCreateDto(BaseModel):
    yesterday: datetime = datetime.today() - date.timedelta(1)
    dag_id: str
    owner: str = ''
    start_date: str = '({year},{month},{day})'.format(year=yesterday.year, month=yesterday.month,
                                                      day=yesterday.day)
    catchup: bool = False
    schedule_interval: str = "@once"
    csv_files_directory: str = ""


class DagUpdateDto(BaseModel):
    yesterday: datetime = datetime.today() - date.timedelta(1)
    dag_id: str = ''
    owner: str = ''
    start_date: str = ''
    catchup: bool = None
    schedule_interval: str = ""
    csv_files_directory: str = ""


class DagInfoDto(BaseModel):
    id: int = None
    uuid: str = None
    dag_id: str = None
    airflow_home: str = None
    owner: str = None
    catchup: bool = None
    schedule_interval: str = None
    csv_files_directory: str = None
    yesterday: str = None
    start_date: str = None
    using: bool = None
    created_at: datetime = None
    updated_at: datetime = None