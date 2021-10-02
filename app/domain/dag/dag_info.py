from datetime import datetime

from app.domain.utils.uuid_util import uuid
from app.dto.dag.dag_info_dto import DagInfoDto


class DagInfo:
    def __init__(self, request: DagInfoDto):
        self.uuid: str = uuid()
        self.yesterday: datetime
        self.airflow_home: str
        self.airflow_home: str
        self.backend_url: str
        self.dag_id: str
        self.owner: str
        self.start_date: str
        self.catchup: str
        self.schedule_interval: str
        self.csv_files_directory: str
        self.created_at: datetime = datetime.now()
        if request is not None:
            self.yesterday = request.yesterday
            self.airflow_home = request.airflow_home
            self.backend_url = request.backend_url
            self.dag_id = request.dag_id
            self.owner = request.owner
            self.start_date = request.start_date
            self.catchup = request.catchup
            self.schedule_interval = request.schedule_interval
            self.csv_files_directory = request.csv_files_directory
