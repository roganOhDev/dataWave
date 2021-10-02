import datetime
from typing import Any

from app.dto.dag.dag_info_dto import DagInfoDto
from pydantic import BaseModel


class DagInfo(BaseModel):
    def __init__(self, request: DagInfoDto, **data: Any):
        super().__init__(**data)
        if request is None:
            self.yesterday: datetime
            self.airflow_home: str
            self.backend_url: str
            self.dag_id: str
            self.owner: str
            self.start_date: str
            self.catchup: str
            self.schedule_interval: str
            self.csv_files_directory: str
        else:
            self.yesterday: datetime = request['yesterday']
            self.airflow_home: str = request['airflow_home']
            self.backend_url: str = request['backend_url']
            self.dag_id: str = request['dag_id']
            self.owner: str = request['owner']
            self.start_date: str = request['star_date']
            self.catchup: str = request['catchup']
            self.schedule_interval: str = request['schedule_interval']
            self.csv_files_directory: str = request['csv_files_directory']
