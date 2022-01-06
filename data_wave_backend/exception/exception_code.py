class ExceptionCode:
    TYPE_EXCEPTION = "DWB-0001"
    EMPTY_VALUE_EXCEPTION = "DWB-0002"

    class Job:
        ALREADY_EXITS_JOB_ID = "DWB-0100"
        JOB_NOT_FOUND = "DWB-0101"
        USING_JOB = "DWB-0102"
        NOT_VALID_CRON_EXPRESSION = "DWB-0103"

    class Connection:
        ALREADY_EXITS_CONNECTION_NAME = "DWB-0200"
        CONNECTION_NOT_FOUND = "DWB-0201"
        NOT_SUPPORTED_DB_TYPE = "DWB-0202"

    class Hook:
        CANNOT_SHOW_TABLE = "DWB-0300"

    class Table_List:
        TABLE_LIST_NOT_FOUND = "DWB-0400"
        COLUMNS_NOT_INCLUDE_PK = "DWB-0401"

    class Elt_Map:
        ELT_MAP_NOT_FOUND = "DWB-0500"
        USING_JOB = "DWB-0501"
        CONNECTIONS_ARE_NOT_EQUAL = "DWB-0502"
        NOT_EXIST_FILE = "DWB-0503"
