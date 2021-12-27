from enum import Enum


class Db_Type(Enum):
    MYSQL = 0
    SNOWFLAKE = 1
    REDSHIFT = 2
    POSTGRESQL = 3
