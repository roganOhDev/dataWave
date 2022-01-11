class Get_Full_table:
    '''
    mysql : column , table
    snowflake : column , schema, table
    mysql : column , schema, table
    '''
    MYSQL = "select {columns} from {table}"
    SNOWFLAKE = "select {columns} from {schema}.{table}"
    AMAZON = "select {columns} from {schema}.{table}"
