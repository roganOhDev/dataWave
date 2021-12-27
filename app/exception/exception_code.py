class ExceptionCode:
    TYPE_EXCEPTION = "DWS-0001"
    EMPTY_VALUE_EXCEPTION = "DWS-0002"

    class dag:
        ALREADY_EXITS_DAG_ID = "DWS-0100"
        DAG_NOT_FOUND = "DWS-0101"

    class connection:
        ALREADY_EXITS_CONNECTION_NAME = "DWS-0200"
        CONNECTION_NOT_FOUND = "DWS-0201"
        NOT_SUPPORTED_DB_TYPE = "DWS-0202"

    class hook:
        CANNOT_SHOW_TABLE = "DWS-0300"
