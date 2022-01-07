class ExceptionCode:
    TYPE_EXCEPTION = "DWB-0001"

    class Backend:
        BackendException = "DWE-0100"

    class Scheduler:
        NOT_EXIST_SCHEDULER_ID = "DWE-0200"
        ALREADY_EXIST_SCHEDULER_ID = "DWE-0200"
