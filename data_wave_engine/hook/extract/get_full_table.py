class Get_Full_table:
    '''
    mysql : column , table
    snowflake : column , schema, table
    mysql : column , schema, table
    '''
    MYSQL = "select %s from %s"
    SNOWFLAKE = "select %s from %s.%s"
    AMAZON = "select %s from %s.%s"
