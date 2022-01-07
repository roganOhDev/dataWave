class Get_Data_By_Max_Pk:
    '''
    mysql : column , table, pk, max_pk
    snowflake : column , schema, table, pk, max_pk
    mysql : column , schema, table, pk, max_pk
    '''
    MYSQL = "select {columns} from %s where %s > %s"
    SNOWFLAE = "select {columns} from %s,%s where %s > %s"
    AMAZON = "select {columns}s from %s,%s where %s > %s"
