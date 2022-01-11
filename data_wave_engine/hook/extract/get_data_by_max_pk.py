class Get_Data_By_Max_Pk:
    '''
    mysql : column , table, pk, max_pk
    snowflake : column , schema, table, pk, max_pk
    mysql : column , schema, table, pk, max_pk
    '''
    MYSQL = "select {columns} from {table} where %s > %s"
    SNOWFLAE = "select {columns} from {schema}.{table} where %s > %s"
    AMAZON = "select {columns}s from {schema}.{table} where %s > %s"
