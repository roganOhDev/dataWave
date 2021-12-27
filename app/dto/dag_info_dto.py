import datetime as date
from datetime import datetime

from pydantic import BaseModel

from app.domain.dag.dag_infoes import DagInfo


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
    created_at: datetime = None
    updated_at: datetime = None


def of(dag_info: DagInfo) -> DagInfoDto:
    dag_info_dto = DagInfoDto()
    dag_info_dto.id = dag_info.id
    dag_info_dto.uuid = dag_info.uuid
    dag_info_dto.dag_id = dag_info.dag_id
    dag_info_dto.airflow_home = dag_info.airflow_home
    dag_info_dto.owner = dag_info.owner
    dag_info_dto.catchup = dag_info.catchup
    dag_info_dto.schedule_interval = dag_info.schedule_interval
    dag_info_dto.csv_files_directory = dag_info.csv_files_directory
    dag_info_dto.yesterday = dag_info.yesterday.strftime("%Y-%m-%d")
    dag_info_dto.start_date = dag_info.start_date
    dag_info_dto.created_at = dag_info.created_at
    dag_info_dto.updated_at = dag_info.updated_at
    return dag_info_dto
