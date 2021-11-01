import datetime as date
from datetime import datetime

from pydantic import BaseModel


class DagInfoDto(BaseModel):
    yesterday: datetime = datetime.today() - date.timedelta(1)
    dag_id: str
    owner: str = ''
    start_date: str = '({year},{month},{day})'.format(year=yesterday.year, month=yesterday.month,
                                                      day=yesterday.day)
    catchup: bool = False
    schedule_interval: str = "@once"
    csv_files_directory: str = ""
