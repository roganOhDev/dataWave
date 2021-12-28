class ExceptionCode:
    TYPE_EXCEPTION = "DWS-0001"
    EMPTY_VALUE_EXCEPTION = "DWS-0002"

    class Dag:
        ALREADY_EXITS_DAG_ID = "DWS-0100"
        DAG_NOT_FOUND = "DWS-0101"

    class Connection:
        ALREADY_EXITS_CONNECTION_NAME = "DWS-0200"
        CONNECTION_NOT_FOUND = "DWS-0201"
        NOT_SUPPORTED_DB_TYPE = "DWS-0202"

    class Hook:
        CANNOT_SHOW_TABLE = "DWS-0300"

    class Table_List:
        TABLE_LIST_NOT_FOUND = "DWS-0301"
