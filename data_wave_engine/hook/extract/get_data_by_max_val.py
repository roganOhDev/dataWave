from exception.engine_exception import EngineException
from get_full_table import *


def get_data_by_max_val_mysql(csv_files_directory: str, dag_id: str, connection: MySQLConnectionAbstract,
                              column_list: List[str], table: str, pk: str, table_pk_max: int):
    column_str = convert_str_list_to_string(column_list)

    try:
        sql_select = "select %s from %s where %s > %d"
        sql_data = (column_str, table, pk, table_pk_max)
        cursor = connection.cursor()
        cursor.execute(sql_select, sql_data)
        records = cursor.fetchall()
    except Exception as e:
        raise EngineException(str(e))

    data = pd.DataFrame(records)

    data.to_csv(csv_files_directory + '/' + dag_id + '_' + table + '.csv', sep=',', quotechar="'", na_rep='NaN',
                index=False)
