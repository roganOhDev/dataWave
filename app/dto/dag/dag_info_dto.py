import datetime as date
from datetime import datetime
from typing import Any

from pydantic import BaseModel


class DagInfoDto(BaseModel):
    def __init__(self, **data: Any):
        super().__init__(**data)
        self.yesterday: datetime = datetime.today() - date.timedelta(1)
        self.airflow_home: str = ''
        self.backend_url: str = ''
        self.dag_id: str
        self.owner: str = 'data_wave'
        self.start_date: str = '({year},{month},{day})'.format(year=self.yesterday.year, month=self.yesterday.month,
                                                               day=self.yesterday.day)
        self.catchup: str = 'False'
        self.schedule_interval: str = "@once"
        self.csv_files_directory: str = ""
