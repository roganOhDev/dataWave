class Get_Full_table:
    '''
    mysql : column , table
    snowflake : column , schema, table
    mysql : column , schema, table
    '''
    MYSQL = "select {columns} from %s"
    SNOWFLAKE = "select {columns} from %s.%s"
    AMAZON = "select {columns} from %s.%s"
