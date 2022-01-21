from enum import Enum

class EltMapStatus(Enum):
    UNSCHEDULED = 0
    WAITING = 1
    SCHEDULED = 2
    EXECUTING = 3
    LOADING = 4
    SUCCESS = 5
    FAIL = 6