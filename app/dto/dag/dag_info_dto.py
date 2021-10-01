import datetime as date

from pydantic import BaseModel


class dag_info_dto(BaseModel):
    yesterday: date = date.date.today() - date.timedelta(1)
    airflow_home: str = None
    backend_url: str = ''
    dag_id: str = None
    owner: str = 'chequer'
    start_date: str = None
    catchup = 'False'
    schedule_interval: str = '@once'
    csv_files_directory: str = None
