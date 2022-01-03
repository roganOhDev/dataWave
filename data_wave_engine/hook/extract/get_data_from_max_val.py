from get_full_table import *
from hook.get_engine import get_engine
from data_wave_backend.domain.table import table_composite_service


def get_data_from_max_val(tables_pk_max: [int]):
    table_list = table_composite_service.find(table_list_uuids[0])
    filename = metadatas.tables[i]
    database = metadatas.database[i]
    schema = metadatas.schema[i]
    engine_ds = get_engine(ds['db_type'][0], db_information)
    dag_id = metadatas.dag_id
    columns = metadatas.column_info
    directory = metadatas.csv_files_directory
    pk = metadatas.pk
    upsert = metadatas.upsert_rule
    updated = metadatas.updated
    if upsert[i] == 'merge':
        pk[i] = updated[i]

    if ds['db_type'][0] in ('postgresql', 'redshift'):

        try:
            max_value = \
                engine_ds.execute(
                    'select max({pk}) from {database}.{schema}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                           database=
                                                                                           db_information[
                                                                                               'database'],
                                                                                           schema=
                                                                                           db_information[
                                                                                               'schema'],
                                                                                           filename=filename,
                                                                                           dag_id=dag_id)).first()[
                    0]
        except:  # same as truncate if table doesn't exist at load_db
            get_full_table_amazon(i, metadatas, engine)

        else:
            indata = pd.read_sql_query(
                "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                    column=columns[i], database=database, schema=schema,
                    dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value), engine)
            indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                          na_rep='NaN', index=False)

    elif ds['db_type'][0] == 'snowflake':
        try:
            max_value = \
                engine_ds.execute(
                    'select max({pk}) from {database}.{schema}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                           database=
                                                                                           db_information[
                                                                                               'database'],
                                                                                           schema=
                                                                                           db_information[
                                                                                               'schema'],
                                                                                           filename=filename,
                                                                                           dag_id=dag_id)).first()[
                    0]
        except:  # same as truncate if table doesn't exist at load_db
            get_full_table_snowflake(i, metadatas, engine)

        else:
            indata = pd.read_sql_query(
                "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                    column=columns[i].lower(), database=database.lower(), schema=schema.lower(),
                    dag_id=dag_id.lower(), table=filename.lower(), pk=pk[i].lower(), max_date=max_value),
                engine)
            indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                          na_rep='NaN', index=False)

    elif ds['db_type'][0] == 'mysql':
        try:
            max_value = \
                engine_ds.execute(
                    'select max({pk}) from {database}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                  database=
                                                                                  db_information[
                                                                                      'database'],
                                                                                  filename=filename,
                                                                                  dag_id=dag_id)).first()[
                    0]
        except:  # same as truncate if table doesn't exist at load_db
            get_full_table_mysql(i, metadatas, engine)
        else:
            indata = pd.read_sql_query(
                "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                    column=columns[i], database=database, schema=schema,
                    dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value), engine)
            indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                          na_rep='NaN',
                          index=False)
