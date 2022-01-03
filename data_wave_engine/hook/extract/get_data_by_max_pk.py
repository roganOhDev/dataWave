class Get_Data_By_Max_Pk:
    '''
    mysql : column , table, pk, max_pk
    snowflake : column , schema, table, pk, max_pk
    mysql : column , schema, table, pk, max_pk
    '''
    MYSQL = "select %s from %s where %s > %d"
    SNOWFLAE = "select %s from %s,%s where %s > %d"
    AMAZON = "select %s from %s,%s where %s > %d"
