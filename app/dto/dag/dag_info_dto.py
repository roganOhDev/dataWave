from datetime import datetime
import datetime as date
from typing import Any

from pydantic import BaseModel


class DagInfoDto(BaseModel):
    def __init__(self, request, **data: Any):
        super().__init__(**data)
        self.yesterday: datetime = datetime.today() - date.timedelta(1)
        self. airflow_home: str = None
        self.backend_url: str = ''
        self.dag_id: str = request['dag_id']
        self.owner: str = 'chequer'
        self.start_date: str = request['star_date']
        self.catchup = 'False' if request['catchup'] is None else request['catchup']
        self.schedule_interval: str = "@once" if request['schedule_interval'] is None else request['schedule_interval']
        self.csv_files_directory: str = ""
