from enum import Enum


class DbType(Enum):
    MYSQL = 0
    SNOWFLAKE = 1
    REDSHIFT = 2
    POSTGRESQL = 3
