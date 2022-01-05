import datetime as date
from datetime import datetime


class JobCreateDto:
    yesterday: datetime = datetime.today() - date.timedelta(1)
    job_id: str
    owner: str = ''
    start_date: str = '({year},{month},{day})'.format(year=yesterday.year, month=yesterday.month,
                                                      day=yesterday.day)
    schedule_interval: str = "@once"
    csv_files_directory: str


class JobUpdateDto:
    job_id: str = ''
    owner: str = ''
    start_date: str = ''
    schedule_interval: str = ""
    csv_files_directory: str = ""


class JobInfoDto:
    id: int = None
    uuid: str = None
    job_id: str = None
    owner: str = None
    schedule_interval: str = None
    csv_files_directory: str = None
    start_date: str = None
    using: bool = None
    created_at: datetime = None
    updated_at: datetime = None

    def __init__(self, **entries):
        self.__dict__.update(entries)
