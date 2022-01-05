# -*- coding:utf-8 -*-

"""Library for extract data from destination db.
This library is made to run a DAG that user made in make_a_job.py
Available functions:
- snowflake: load data in snowflake
- mysql: load data in mysql
- redshift: load data in redshift
- postgresql: load data in postgresql
"""
import pandas as pd
import sqlalchemy as sql


def snowflake(job_id, id, pwd, account, database, schema, warehouse, tables, directory, pk, upsert, columns, updated, role=''):
    """
    load data in snowflake
    :param id: login id
    :param pwd: login password
    :param account: user's account
    :param database: user's database
    :param schema: user's schema
    :param warehouse: user's warehouse
    :param tables: user's tables
    :rtype: list
    :param directory: directory where csv files in
    :param upsert: this function will run differently benchmarking upsert
        :param pk: if upsert=merge then pk=primary key elif upsert=increasement then pk=increase column
        :param updated:if upsert=merge then updated=updated_at
        :rtype: both are list
    :rtype:list
    :param columns: user's columns for each table
    :rtype:list
    :param role: user's role
    """
    if role == '':
        url_role = role
    else:
        url_role = '&role={}'.format(role)
    engine = sql.create_engine(
        'snowflake://{u}:{p}@{a}/{d}/{s}?warehouse={w}&role={r}'.format(
            u=id,
            p=pwd,
            a=account,
            r=url_role,
            d=database.lower(),
            s=schema.lower(),
            w=warehouse
        )
    )
    for i in range(len(tables)):
        filename = tables[i]
        try:
            engine.execute('list @%{job_id}_{filename}'.format(job_id=job_id, filename=filename))
        except:
            print("no exists")
        else:
            engine.execute('remove @%{job_id}_{filename}'.format(job_id=job_id, filename=filename))

        if upsert[i] == 'truncate':
            result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
            engine.execute('truncate table if exists {job_id}_{filename}'.format(job_id=job_id, filename=filename))
            result.head(0).to_sql(job_id.lower() + '_' + filename.lower(), engine, if_exists='replace', index=False)
            engine.execute(
                '''put file://{directory}/{job_id}_{filename}.csv @%{job_id_lower}_{filename_lower}'''.format(directory=directory, job_id=job_id,
                                                                                                              filename=filename, filename_lower=filename.lower(),
                                                                                                              job_id_lower=job_id.lower()))
            engine.execute(
                '''copy into {job_id}_{filename} from @%{job_id}_{filename} file_format=(type="csv" FIELD_OPTIONALLY_ENCLOSED_BY ="'" SKIP_HEADER=1  );'''.format(
                    filename=filename.lower(),job_id=job_id.lower()))
        elif upsert[i] == 'increasement':
            result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
            try:
                engine.execute('select 1 from {filename} limit 1'.format(pk=pk[i], filename=filename.lower()))
            except:
                result.head(0).to_sql(job_id.lower() + '_' + filename.lower(), engine, if_exists='replace', index=False)
                engine.execute(
                    '''put file://{directory}/{job_id}_{filename}.csv @%{job_id_lower}_{filename_lower}'''.format(
                        directory=directory, job_id=job_id,
                        filename=filename, filename_lower=filename.lower(),
                        job_id_lower=job_id.lower()))
                engine.execute(
                    '''copy into {job_id}_{filename} from @%{job_id}_{filename} file_format=(type="csv" FIELD_OPTIONALLY_ENCLOSED_BY ="'" SKIP_HEADER=1  );'''.format(
                        filename=filename.lower(), job_id=job_id.lower()))
            else:
                result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
                indata = pd.read_sql_query('select * from ' + job_id + '_' + filename + ' limit 0', engine)
                # 두 테이블의 칼럼을 하나의 리스트로 합침:
                new_columns = indata.columns.values
                before_columns = result.column_info.values
                cnt = 0  # 추가된 칼럼 없음
                for v in new_columns:
                    if v not in before_columns:
                        cnt = 1  # 추가된 칼럼 있음
                        break
                if cnt:
                    print("Cause number of columns increase, before table must be reformed. It will take long time\n"
                          "Will you continue?")
                    ans = input("y/n : ")
                    while ans not in ('y', 'n'):
                        ans = input("y/n : ")
                    if ans == 'n':
                        print("Querypie ELT jumps this proccess this time")
                    else:
                        print("Querypie ELT continues this proccess")
                        engine.execute('alter table {job_id}_{filename} rename to {job_id}_{filename}_tmp'.format(job_id=job_id.lower(),
                                                                                                                  filename=filename.lower()))
                        # 두 테이블의 칼럼을 하나의 리스트로 합친후 빈 테이블 db에 생성
                        origin_columns = before_columns
                        for v in new_columns:
                            if v not in before_columns:
                                before_columns.append(v)
                        result_columns = before_columns
                        df = pd.DataFrame([], columns=result_columns, index=[])
                        df.to_sql(job_id.lower() + '_' + filename.lower(), engine, if_exists='error', index=False, chunksize=15000)
                        column_list = ','.join(origin_columns)
                        engine.execute('insert into {job_id}_{filename}({columns}) select * from {job_id}_{filename}_tmp'
                                       .format(job_id=job_id.lower(), filename=filename.lower(), columns=column_list))
                        result.to_sql(job_id + '_' + filename, engine, if_exists='append',
                                      index=False)
                        engine.execute('drop table {job_id}_{filename}_tmp'.format(job_id=job_id.lower(),
                                                                                   filename=filename.lower()))
                else:
                    result.to_sql(job_id.lower() + '_' + filename.lower(), engine, if_exists='append', index=False, chunksize=15000)

        elif upsert[i] == 'merge':
            result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
            try:
                engine.execute('select 1 from {filename} limit 1'.format(pk=pk[i], filename=filename.lower()))
            except:
                result.head(0).to_sql(job_id.lower() + '_' + filename.lower(), engine, if_exists='replace', index=False)
                engine.execute(
                    '''put file://{directory}/{job_id}_{filename}.csv @%{job_id_lower}_{filename_lower}'''.format(
                        directory=directory, job_id=job_id,
                        filename=filename, filename_lower=filename.lower(),
                        job_id_lower=job_id.lower()))
                engine.execute(
                    '''copy into {job_id}_{filename} from @%{job_id}_{filename} file_format=(type="csv" FIELD_OPTIONALLY_ENCLOSED_BY ="'" SKIP_HEADER=1  );'''.format(
                        filename=filename.lower(), job_id=job_id.lower()))
            else:
                result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
                indata = pd.read_sql_query('select * from ' + job_id + '_' + filename + ' limit 0', engine)
                # 두 테이블의 칼럼을 하나의 리스트로 합침:
                new_columns = indata.columns.values
                before_columns = result.column_info.values
                cnt = 0  # 추가된 칼럼 없음
                for v in new_columns:
                    if v not in before_columns:
                        cnt = 1  # 추가된 칼럼 있음
                        break
                if cnt:
                    print("Cause number of columns increase, before table must be reformed. It will take long time\n"
                          "Will you continue?")
                    ans = input("y/n : ")
                    while ans not in ('y', 'n'):
                        ans = input("y/n : ")
                    if ans == 'n':
                        print("Querypie ELT jumps this proccess this time")
                    else:
                        print("Querypie ELT continues this proccess")
                        engine.execute('alter table {job_id}_{filename} rename to {job_id}_{filename}_tmp'.format(
                            job_id=job_id.lower(),
                            filename=filename.lower()))
                        # 두 테이블의 칼럼을 하나의 리스트로 합친후 빈 테이블 db에 생성
                        origin_columns = before_columns
                        for v in new_columns:
                            if v not in before_columns:
                                before_columns.append(v)
                        result_columns = before_columns
                        df = pd.DataFrame([], columns=result_columns, index=[])
                        df.to_sql(job_id.lower() + '_' + filename.lower(), engine, if_exists='append', index=False)
                        column_list = ','.join(origin_columns)
                        engine.execute(
                            'insert into {job_id}_{filename}({columns}) select * from {job_id}_{filename}_tmp'
                            .format(job_id=job_id.lower(), filename=filename.lower(), columns=column_list))
                        result.to_sql(job_id + '_' + filename + '_tmp', engine, if_exists='replace', index=False)
                        #기존 테이블을 새 테이블에 새 칼럼과 함께 넣음
                        column_string_to_list = columns[i].split(',')
                        update = ''
                        insert = ''
                        for column_name in column_string_to_list:
                            update_new = ''
                            if (update):
                                update = update + ','
                            if column_name not in origin_columns:
                                update_new = "{column_name}=''".format(column_name=column_name)
                            else:
                                update_new = '{job_id}_{table}.{column_name}={job_id}_{table}_tmp.{column_name}'.format(
                                column_name=column_name,table=filename.lower(), job_id=job_id.lower())

                            update = update + update_new
                        for column_name in column_string_to_list:
                            if (insert):
                                insert = insert + ','
                            if column_name not in origin_columns:
                                insert = "${column_name}".format(column_name=column_name)
                            insert = insert + '{job_id}_{table}_tmp.{column_name}'.format(table=filename.lower(),
                                                                                          job_id=job_id.lower(),
                                                                                          column_name=column_name)
                        other_columns = [x for x in result_columns if x not in new_columns]
                        len_other_columns = len(other_columns)
                        other_columns = ','.join(other_columns)
                        other_columns_null = ['']*len_other_columns
                        other_columns_null = ','.join(other_columns_null)
                        query = '''set ({other_colmuns})=({other_columns_null});
                        merge into {job_id}_{table} using {job_id}_{table}_tmp 
                        on {job_id}_{table}.{pk}={job_id}_{table}_tmp.{pk} when matched then update set {update} 
                        when not matched then insert values ({insert})'''.format(
                            update=update, pk=pk[i], table=filename.lower(), insert=insert, job_id=job_id.lower(),
                            other_colmuns=other_columns, other_columns_null=other_columns_null)

                        result.head(0).to_sql(job_id.lower() + '_' + filename.lower() + '_tmp', engine,
                                              if_exists='replace', index=False)
                        engine.execute(
                            '''put file://{directory}/{job_id}_{filename}.csv @%{job_id_lower}_{filename_lower}_tmp'''.format(
                                directory=directory, job_id=job_id,
                                filename=filename, filename_lower=filename.lower(),
                                job_id_lower=job_id.lower()))
                        engine.execute(
                            '''copy into {job_id}_{filename} from @%{job_id}_{filename}_tmp file_format=(type="csv" FIELD_OPTIONALLY_ENCLOSED_BY ="'" SKIP_HEADER=1  );'''.format(
                                filename=filename.lower(), job_id=job_id.lower()))
                        engine.execute(query)
                        engine.execute('drop table if exists {job_id}_{filename}_tmp'.format(job_id=job_id.lower(),
                                                                                             filename=filename.lower()))

                else:
                    column_string_to_list = columns[i].split(',')
                    update = ''
                    insert = ''
                    for column_name in column_string_to_list:
                        if (update):
                            update = update + ','
                        update = update + '{job_id}_{table}.{column_name}={job_id}_{table}_tmp.{column_name}'.format(column_name=column_name,
                                                                                                                     table=filename.lower(), job_id=job_id.lower())
                    for column_name in column_string_to_list:
                        if (insert):
                            insert = insert + ','
                        insert = insert + '{job_id}_{table}_tmp.{column_name}'.format(table=filename.lower(), job_id=job_id.lower(), column_name=column_name)
                    query = '''merge into {job_id}_{table} using {job_id}_{table}_tmp on {job_id}_{table}.{pk}={job_id}_{table}_tmp.{pk} when matched then update set {update} when not matched then insert values ({insert})'''.format(
                        update=update, pk=pk[i], table=filename.lower(), insert=insert, job_id=job_id.lower())

                    result.head(0).to_sql(job_id.lower() + '_' + filename.lower() + '_tmp', engine, if_exists='replace', index=False)
                    engine.execute(
                        '''put file://{directory}/{job_id}_{filename}.csv @%{job_id_lower}_{filename_lower}_tmp'''.format(
                            directory=directory, job_id=job_id,
                            filename=filename, filename_lower=filename.lower(),
                            job_id_lower=job_id.lower()))
                    engine.execute(
                        '''copy into {job_id}_{filename} from @%{job_id}_{filename}_tmp file_format=(type="csv" FIELD_OPTIONALLY_ENCLOSED_BY ="'" SKIP_HEADER=1  );'''.format(
                            filename=filename.lower(), job_id=job_id.lower()))
                    engine.execute(query)
                    engine.execute('drop table if exists {job_id}_{filename}_tmp'.format(job_id=job_id.lower(), filename=filename.lower()))

def postgresql(job_id, id, pwd, host, port, database,schema, tables, directory, pk, upsert, updated, columns):
    """
    load data in postgresql
    :param id: login id
    :param pwd: login password
    :param host: db's host
    :param port: db's port
    :param database: users database
    :param tables: user's tables
    :rtype:list
    :param directory: directory where csv files in
    :param upsert: this function will run differently benchmarking upsert
        :param pk: if upsert=merge then pk=primary key elif upsert=increasement then pk=increase column
        :param updated:if upsert=merge then updated=updated_at
        :rtype: both are list
    :rtype:list
    :param columns: user's columns
    :rtype: list
    """
    url = 'postgresql://{u}:{p}@{h}:{port}/{d}'.format(
        u=id,
        p=pwd,
        h=host,
        port=port,
        d=database
    )
    engine = sql.create_engine(url, client_encoding='utf8')
    for i in range(len(tables)):
        schema=schema[i]
        filename = tables[i]
        if upsert[i] == 'truncate':
            result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
            result.to_sql(filename,con= engine, schema=schema, if_exists='replace', index=False)
        elif upsert[i] == 'increasement':
            result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
            indata = pd.read_sql_query('select * from ' + job_id + '_' + filename + ' limit 0', engine)
            # 두 테이블의 칼럼을 하나의 리스트로 합침
            new_columns = indata.columns.values
            before_columns = result.column_info.values
            cnt = 0  # 추가된 칼럼 없음
            for v in new_columns:
                if v not in before_columns:
                    cnt = 1  # 추가된 칼럼 있음
                    break
            if cnt:
                print("Cause number of columns increase, before table must be reformed. It will take long time\n"
                      "Will you continue?")
                ans = input("y/n : ")
                while ans not in ('y', 'n'):
                    ans = input("y/n : ")
                if ans == 'n':
                    print("Querypie ELT jumps this proccess this time")
                else:
                    print("Querypie ELT continues this proccess")
                    engine.execute(
                        'alter table {schema}.{job_id}_{filename} rename to {job_id}_{filename}_tmp'.format(job_id=job_id,
                                                                                                    filename=filename,
                                                                                                    schema=schema))
                    # 두 테이블의 칼럼을 하나의 리스트로 합친후 빈 테이블 db에 생성
                    origin_columns = before_columns
                    for v in new_columns:
                        if v not in before_columns:
                            before_columns.append(v)
                    result_columns = before_columns
                    df = pd.DataFrame([], columns=result_columns, index=[])
                    df.to_sql(job_id + '_' + filename, engine, if_exists='error', index=False)

                    column_list = ','.join(origin_columns)
                    engine.execute(
                        'insert into {schema}.{job_id}_{filename}({columns}) select * from {schema}.{job_id}_{filename}_tmp'
                            .format(job_id=job_id,filename=filename,columns=column_list, schema=schema))
                    result.to_sql(job_id + '_' + filename, engine, if_exists='append',
                                  index=False)
                    engine.execute(
                        'drop table if exists {schema}.{job_id}_{table}_tmp'.format(schema=schema, job_id=job_id,
                                                                                    table=filename))
            else:
                result.to_sql(job_id + '_' + filename, engine, if_exists='append',
                              index=False)
        elif upsert[i] == 'merge':
            try:
                engine.execute('select 1 from {schema}.{job_id}_{table}'.format(schema=schema, job_id=job_id,filename=filename))
            except:
                result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
                result.to_sql(filename, schema=schema, con=engine, if_exists='replace', index=False)
            else:
                result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
                indata = pd.read_sql_query('select * from ' + job_id + '_' + filename + ' limit 0', engine)
                # 두 테이블의 칼럼을 하나의 리스트로 합침
                # before_columns: 기존에 있었던 칼럼들
                # new_columns: 새로 추가될 테이블의 칼럼
                before_columns = indata.columns.values
                new_columns = result.column_info.values
                cnt = 0  # 추가된 칼럼 없음
                for v in new_columns:
                    if v not in before_columns:
                        cnt = 1  # 추가된 칼럼 있음
                        break
                if cnt:
                    print("Cause number of columns increase, before table must be reformed. It will take long time\n"
                          "Will you continue?")
                    ans = input("y/n : ")
                    while ans not in ('y', 'n'):
                        ans = input("y/n : ")
                    if ans == 'n':
                        print("Querypie ELT jumps this proccess this time")
                    else:
                        print("Querypie ELT continues this proccess")
                        engine.execute(
                            'rename table {job_id}_{filename} to {job_id}_{filename}_tmp'.format(job_id=job_id,
                                                                                                 filename=filename))
                        # 두 테이블의 칼럼을 하나의 리스트로 합친후 빈 테이블 db에 생성
                        origin_columns = before_columns
                        for c in new_columns:
                            if c not in before_columns:
                                before_columns.append(c)
                        result_columns = before_columns
                        df = pd.DataFrame([], columns=result_columns, index=[])
                        df.to_sql(job_id + '_' + filename, engine, if_exists='error', index=False)

                        column_list = ','.join(origin_columns)
                        engine.execute(
                            'insert into {schema}.{job_id}_{filename}({columns}) select * from {schema}.{job_id}_{filename}_tmp'
                                .format(job_id=job_id, filename=filename, columns=column_list, schema=schema))
                        result.to_sql(job_id + '_' + filename + '_tmp', engine, if_exists='replace',
                                      index=False)
                        # truncate와 같이 null로 엎어치기한다음 옮길 데이터를 _tmp에 넣어놓음
                        column_string_to_list = columns[i].split(',')
                        update = ''
                        for j in column_string_to_list:
                            update_new = ''
                            if (update):
                                update = update + ','
                            if j not in origin_columns:
                                update_new = "{column_name}=''".format(column_name=j)
                            else:
                                update_new = '{column_name}=tmp.{column_name}'.format(column_name=j)
                            update = update + update_new
                        engine.execute(
                            'ALTER TABLE {schema}.{job_id}_{table} ADD UNIQUE ({pk});'.format(job_id=job_id, pk=pk[i],
                                                                                              schema=schema, table=filename))
                        new_columns_with_tmp=['tmp.'+x for x in new_columns]
                        new_columns_with_tmp=','.join(new_columns_with_tmp)
                        new_columns=','.join(new_columns)

                        query1 = "UPDATE {schema}.{job_id}_{filename} SET {update} " \
                                 "FROM {schema}.{job_id}_{filename}_tmp tmp WHERE {job_id}_{table}.{pk} = tmp.{pk};" \
                            .format(schema=schema, job_id=job_id, filename=filename, update=update, pk=pk[i])
                        query2 = "INSERT INTO {schema}.{job_id}_{filename}({columns}) SELECT {columns_with_tmp} " \
                                 "FROM {schema}.{job_id}_{filename}_tmp tmp " \
                                 "WHERE tmp.{pk} NOT IN ( SELECT {pk} FROM {schema}.{job_id}_{filename})"\
                            .format(schema=schema, job_id=job_id, filename=filename, update=update, pk=pk[i],
                                    columns=new_columns, columns_with_tmp=new_columns_with_tmp)
                        engine.execute(query1)
                        engine.execute(query2)
                        engine.execute(
                            'drop table if exists {schema}.{job_id}_{filename}_tmp'.format(job_id=job_id, filename=filename,
                                                                                           schema=schema))

                else:
                    column_string_to_list = columns[i].split(',')
                    update = ''
                    for j in column_string_to_list:
                        update_new = ''
                        if (update):
                            update = update + ','
                        if j not in origin_columns:
                            update_new = "{column_name}=''".format(column_name=j)
                        else:
                            update_new = '{column_name}=tmp.{column_name}'.format(column_name=j)
                        update = update + update_new
                    engine.execute(
                        'ALTER TABLE {schema}.{job_id}_{table} ADD UNIQUE ({pk});'.format(job_id=job_id, pk=pk[i],
                                                                                          schema=schema,
                                                                                          table=filename))
                    new_columns_with_tmp = ['tmp.' + x for x in new_columns]
                    new_columns_with_tmp = ','.join(new_columns_with_tmp)
                    new_columns = ','.join(new_columns)

                    query1 = "UPDATE {schema}.{job_id}_{filename} SET {update} " \
                             "FROM {schema}.{job_id}_{filename}_tmp tmp WHERE {job_id}_{table}.{pk} = tmp.{pk};" \
                        .format(schema=schema, job_id=job_id, filename=filename, update=update, pk=pk[i])
                    query2 = "INSERT INTO {schema}.{job_id}_{filename}({columns}) SELECT {columns_with_tmp} " \
                             "FROM {schema}.{job_id}_{filename}_tmp tmp " \
                             "WHERE tmp.{pk} NOT IN ( SELECT {pk} FROM {schema}.{job_id}_{filename})" \
                        .format(schema=schema, job_id=job_id, filename=filename, update=update, pk=pk[i],
                                columns=new_columns, columns_with_tmp=new_columns_with_tmp)
                    engine.execute(query1)
                    engine.execute(query2)
                    engine.execute(
                        'drop table if exists {schema}.{job_id}_{filename}_tmp'.format(job_id=job_id, filename=filename,
                                                                                       schema=schema))

#사실 redshift 의 머지와 postgresql의 머지는 같은 구문임(표현만 다를뿐)
def redshift(job_id, id, pwd, host, port, database,schema, tables, directory, pk, upsert, updated, columns):
    """
    load data in redshift
    :param id: login id
    :param pwd: login password
    :param host: db's host
    :param port: db's port
    :param database: user's database
    :param tables: user's tables
    :rtype:list
    :param directory: directory where csv files in
    :param upsert: this function will run differently benchmarking upsert
        :param pk: if upsert=merge then pk=primary key elif upsert=increasement then pk=increase column
        :param updated:if upsert=merge then updated=updated_at
        :rtype: both are list
    :rtype:list
    :param columns: user's columns
    :rtype:list
    """
    url = 'redshift+psycopg2://{u}:{p}@{h}:{port}/{d}'.format(
        u=id,
        p=pwd,
        h=host,
        port=port,
        d=database
    )
    engine = sql.create_engine(url, client_encoding='utf8')
    for i in range(len(tables)):
        filename = tables[i]
        if upsert[i] == 'truncate':
            result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
            result.to_sql(job_id+'_'+filename,schema=schema, con=engine, if_exists='replace', index=False)
        elif upsert[i] == 'increasement':
            result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
            indata = pd.read_sql_query('select * from ' + job_id + '_' + filename + ' limit 0', engine)
            # 두 테이블의 칼럼을 하나의 리스트로 합침
            new_columns = indata.columns.values
            before_columns = result.column_info.values
            cnt = 0  # 추가된 칼럼 없음
            for v in new_columns:
                if v not in before_columns:
                    cnt = 1  # 추가된 칼럼 있음
                    break
            if cnt:
                print("Cause number of columns increase, before table must be reformed. It will take long time\n"
                      "Will you continue?")
                ans = input("y/n : ")
                while ans not in ('y', 'n'):
                    ans = input("y/n : ")
                if ans == 'n':
                    print("Querypie ELT jumps this proccess this time")
                else:
                    print("Querypie ELT continues this proccess")
                    engine.execute('alter table {job_id}_{filename} rename to {job_id}_{filename}_tmp'.format(job_id=job_id,
                                                                                                    filename=filename))
                    # 두 테이블의 칼럼을 하나의 리스트로 합친후 빈 테이블 db에 생성
                    origin_columns = before_columns
                    for v in new_columns:
                        if v not in before_columns:
                            before_columns.append(v)
                    result_columns = before_columns
                    df = pd.DataFrame([], columns=result_columns, index=[])
                    df.to_sql(job_id + '_' + filename, engine, if_exists='error', index=False)

                    column_list = ','.join(origin_columns)
                    engine.execute(
                        'insert into {job_id}_{filename}({columns}) (select * from {job_id}_{filename}_tmp)'.format(job_id=job_id,
                                                                                                       filename=filename,
                                                                                                       columns=column_list))
                    result.to_sql(job_id + '_' + filename, engine, if_exists='append',index=False)
                    engine.execute(
                        'drop table if exists {schema}.{job_id}_{table}_tmp'.format(schema=schema, job_id=job_id,
                                                                                    table=filename))
            else:
                result.to_sql(job_id + '_' + filename, engine, if_exists='append',
                              index=False)


        elif upsert[i] == 'merge':
            try:
                engine.execute('select 1 from {schema}.{job_id}_{table}'.format(schema=schema,job_id=job_id,filename=filename))
            except:
                result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
                result.to_sql(job_id+'_'+filename,schema=schema, con=engine, if_exists='replace', index=False)
            else:
                result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
                indata = pd.read_sql_query('select * from ' + job_id + '_' + filename + ' limit 0', engine)
                # 두 테이블의 칼럼을 하나의 리스트로 합침
                new_columns = indata.columns.values
                before_columns = result.column_info.values
                cnt = 0  # 추가된 칼럼 없음
                for v in new_columns:
                    if v not in before_columns:
                        cnt = 1  # 추가된 칼럼 있음
                        break
                if cnt:
                    print("Cause number of columns increase, before table must be reformed. It will take long time\n"
                          "Will you continue?")
                    ans = input("y/n : ")
                    while ans not in ('y', 'n'):
                        ans = input("y/n : ")
                    if ans == 'n':
                        print("Querypie ELT jumps this proccess this time")
                    else:
                        print("Querypie ELT continues this proccess")
                        engine.execute(
                            'alter table {job_id}_{filename} rename to {job_id}_{filename}_tmp'.format(job_id=job_id,
                                                                                                       filename=filename))
                        # 두 테이블의 칼럼을 하나의 리스트로 합친후 빈 테이블 db에 생성
                        origin_columns = before_columns
                        for v in new_columns:
                            if v not in before_columns:
                                before_columns.append(v)
                        result_columns = before_columns
                        df = pd.DataFrame([], columns=result_columns, index=[])
                        df.to_sql(job_id + '_' + filename, engine, if_exists='error', index=False)

                        column_list = ','.join(origin_columns)
                        engine.execute(
                            'insert into {job_id}_{filename}({columns}) (select * from {job_id}_{filename}_tmp)'.format(
                                job_id=job_id,
                                filename=filename,
                                columns=column_list))
                        result.to_sql(job_id + '_' + filename + '_tmp', engine, if_exists='replace', index=False)
                        #여기 밑에 머지 넣으면 됨

                        column_string_to_list = columns[i].split(',')
                        update = ''
                        for j in column_string_to_list:
                            update_new = ''
                            if (update):
                                update = update + ','
                            if j not in origin_columns:
                                update_new = "{column_name}=''".format(column_name=j)
                            else:
                                update_new = '{column_name}=tmp.{column_name}'.format(column_name=j)
                            update = update + update_new
                        engine.execute(
                            'ALTER TABLE {schema}.{job_id}_{table} ADD UNIQUE ({pk});'.format(job_id=job_id, pk=pk[i],
                                                                                              schema=schema,
                                                                                              table=filename))
                        new_columns_with_tmp = ['tmp.' + x for x in new_columns]
                        new_columns_with_tmp = ','.join(new_columns_with_tmp)
                        new_columns = ','.join(new_columns)

                        query1 = "UPDATE {schema}.{job_id}_{filename} SET {update} " \
                                 "FROM {schema}.{job_id}_{filename}_tmp tmp WHERE {job_id}_{table}.{pk} = tmp.{pk};" \
                            .format(schema=schema, job_id=job_id, filename=filename, update=update, pk=pk[i])
                        query2 = "INSERT INTO {schema}.{job_id}_{filename}({columns}) SELECT {columns_with_tmp} " \
                                 "FROM {schema}.{job_id}_{filename}_tmp tmp " \
                                 "WHERE tmp.{pk} NOT IN ( SELECT {pk} FROM {schema}.{job_id}_{filename})" \
                            .format(schema=schema, job_id=job_id, filename=filename, update=update, pk=pk[i],
                                    columns=new_columns, columns_with_tmp=new_columns_with_tmp)
                        engine.execute(query1)
                        engine.execute(query2)
                        engine.execute(
                            'drop table if exists {schema}.{job_id}_{filename}_tmp'.format(job_id=job_id,
                                                                                           filename=filename,
                                                                                           schema=schema))
                else:
                    # column_string_to_list = columns[i].split(',')
                    # update = ''
                    # for j in column_string_to_list:
                    #     if (update):
                    #         update = update + ','
                    #     update = update + '{column_name}=s.{column_name}'.format(column_name=j)
                    # update_query = "update {schema}.{job_id}_{table} e set {update} from {schema}.{job_id}_{table}_tmp s where e.{pk}=s.{pk};".format(
                    #     table=filename, pk=pk[i], update=update, job_id=job_id, schema=schema)
                    # result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
                    # result.to_sql('{job_id}_{table}_tmp'.format(job_id=job_id,filename=filename),schema=schema, con=engine, if_exists='replace', index=False)
                    # engine.execute(update_query)
                    # insert_query = "insert into {schema}.{job_id}_{table} e select s.* from {job_id}_{table}_tmp s left join e on s.{pk}=e.{pk} where e.{pk} is NULL;".format(
                    #     table=filename, pk=pk, job_id=job_id)
                    # engine.execute(insert_query)
                    # engine.execute('drop table if exists {schema}.{job_id}_{table}_tmp'.format(schema=schema,job_id=job_id, table=filename))
                    column_string_to_list = columns[i].split(',')
                    update = ''
                    for j in column_string_to_list:
                        update_new = ''
                        if (update):
                            update = update + ','
                        if j not in origin_columns:
                            update_new = "{column_name}=''".format(column_name=j)
                        else:
                            update_new = '{column_name}=tmp.{column_name}'.format(column_name=j)
                        update = update + update_new
                    engine.execute(
                        'ALTER TABLE {schema}.{job_id}_{table} ADD UNIQUE ({pk});'.format(job_id=job_id, pk=pk[i],
                                                                                          schema=schema,
                                                                                          table=filename))
                    new_columns_with_tmp = ['tmp.' + x for x in new_columns]
                    new_columns_with_tmp = ','.join(new_columns_with_tmp)
                    new_columns = ','.join(new_columns)

                    query1 = "UPDATE {schema}.{job_id}_{filename} SET {update} " \
                             "FROM {schema}.{job_id}_{filename}_tmp tmp WHERE {job_id}_{table}.{pk} = tmp.{pk};" \
                        .format(schema=schema, job_id=job_id, filename=filename, update=update, pk=pk[i])
                    query2 = "INSERT INTO {schema}.{job_id}_{filename}({columns}) SELECT {columns_with_tmp} " \
                             "FROM {schema}.{job_id}_{filename}_tmp tmp " \
                             "WHERE tmp.{pk} NOT IN ( SELECT {pk} FROM {schema}.{job_id}_{filename})" \
                        .format(schema=schema, job_id=job_id, filename=filename, update=update, pk=pk[i],
                                columns=new_columns, columns_with_tmp=new_columns_with_tmp)
                    engine.execute(query1)
                    engine.execute(query2)
                    engine.execute(
                        'drop table if exists {schema}.{job_id}_{filename}_tmp'.format(job_id=job_id, filename=filename,
                                                                                       schema=schema))


def mysql(job_id, id, pwd, host, port, database, tables, directory, option, pk, upsert, updated, columns):
    """
    load data in mysql
    :param id:login id
    :param pwd: login password
    :param host: db's host
    :param port: db's host
    :param database: user's database
    :param tables: user's database
    :rypte:list
    :param directory: directory where csv files in
    :param upsert: this function will run differently benchmarking upsert
        :param pk: if upsert=merge then pk=primary key elif upsert=increasement then pk=increase column
        :param updated:if upsert=merge then updated=updated_at
        :rtype: both are list
    :rtype:list
    :param option: user's option
    :param columns: user's columns
    :rtype: list
    """
    engine = sql.create_engine('mysql+pymysql://{u}:{p}@{h}:{port}/{d}{option}'.format(
        u=id,
        p=pwd,
        h=host,
        port=port,
        d=database,
        option=option,
    ), encoding='utf-8')
    for i in range(len(tables)):
        filename = tables[i]
        if upsert[i] == 'truncate':
            result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
            result.to_sql(job_id+'_'+filename, engine, if_exists='replace', index=False)
        elif upsert[i] == 'increasement':
            result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
            indata = pd.read_sql_query('select * from ' + job_id+'_'+filename+' limit 0', engine)
            # 두 테이블의 칼럼을 하나의 리스트로 합침
            #before_columns: 기존에 있었던 칼럼들
            #new_columns: 새로 추가될 테이블의 칼럼
            before_columns = indata.columns.values
            new_columns = result.column_info.values
            cnt=0 # 추가된 칼럼 없음
            for v in new_columns:
                if v not in before_columns:
                    cnt=1 # 추가된 칼럼 있음
                    break
            if cnt:
                print("Cause number of columns increase, before table must be reformed. It will take long time\n"
                      "Will you continue?")
                ans=input("y/n : ")
                while ans not in ('y','n'):
                    ans = input("y/n : ")
                if ans == 'n':
                    print("Querypie ELT jumps this proccess this time")
                else:
                    print("Querypie ELT continues this proccess")
                    engine.execute('rename table {job_id}_{filename} to {job_id}_{filename}_tmp'.format(job_id=job_id,
                                                                                                  filename=filename))
                    # 두 테이블의 칼럼을 하나의 리스트로 합친후 빈 테이블 db에 생성
                    origin_columns = before_columns
                    for c in new_columns:
                        if c not in before_columns:
                            before_columns.append(c)
                    result_columns=before_columns
                    df = pd.DataFrame([], columns=result_columns, index=[])
                    df.to_sql(job_id+'_'+filename, engine, if_exists='error', index=False)

                    column_list = ','.join(origin_columns)
                    engine.execute('insert into {job_id}_{filename}({columns}) select * from {job_id}_{filename}_tmp'
                                   .format(job_id=job_id,filename=filename,columns=column_list))
                    result.to_sql(job_id + '_' + filename, engine, if_exists='append',
                                  index=False)
                    engine.execute('drop table {job_id}_{filename}_tmp'.format(job_id=job_id,filename=filename))
            else:
                result.to_sql(job_id + '_' + filename, engine, if_exists='append',
                              index=False)

        elif upsert[i] == 'merge':
            try:
                #table이 있는지 확인
                engine.execute('select 1 from {job_id}_{filename}'.format(job_id=job_id,filename=filename))
            except:
                result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
                result.to_sql(job_id+'_'+filename, engine, if_exists='replace', index=False)
            else:
                result = pd.read_csv(directory + '/' + job_id + '_' + filename + '.csv', sep=',', quotechar="'")
                indata = pd.read_sql_query('select * from ' + job_id + '_' + filename + ' limit 0', engine)
                # 두 테이블의 칼럼을 하나의 리스트로 합침
                # before_columns: 기존에 있었던 칼럼들
                # new_columns: 새로 추가될 테이블의 칼럼
                before_columns = indata.columns.values
                new_columns = result.column_info.values
                cnt = 0  # 추가된 칼럼 없음
                for v in new_columns:
                    if v not in before_columns:
                        cnt = 1  # 추가된 칼럼 있음
                        break
                if cnt:
                    print("Cause number of columns increase, before table must be reformed. It will take long time\n"
                          "Will you continue?")
                    ans = input("y/n : ")
                    while ans not in ('y', 'n'):
                        ans = input("y/n : ")
                    if ans == 'n':
                        print("Querypie ELT jumps this proccess this time")
                    else:
                        print("Querypie ELT continues this proccess")
                        engine.execute(
                            'rename table {job_id}_{filename} to {job_id}_{filename}_tmp'.format(job_id=job_id,
                                                                                                 filename=filename))
                        # 두 테이블의 칼럼을 하나의 리스트로 합친후 빈 테이블 db에 생성
                        origin_columns = before_columns
                        for c in new_columns:
                            if c not in before_columns:
                                before_columns.append(c)
                        result_columns = before_columns
                        df = pd.DataFrame([], columns=result_columns, index=[])
                        df.to_sql(job_id + '_' + filename, engine, if_exists='error', index=False)

                        column_list = ','.join(origin_columns)
                        engine.execute(
                            'insert into {job_id}_{filename}({columns}) select * from {job_id}_{filename}_tmp'
                            .format(job_id=job_id, filename=filename, columns=column_list))
                        result.to_sql(job_id + '_' + filename+'_tmp', engine, if_exists='replace',
                                      index=False)
                        #truncate와 같이 null로 엎어치기한다음 옮길 데이터를 _tmp에 넣어놓음
                        column_string_to_list = columns[i].split(',')
                        update = ''
                        for j in column_string_to_list:
                            update_new = ''
                            if (update):
                                update = update + ','
                            if j not in origin_columns:
                                update_new = "{column_name}=''".format(column_name=j)
                            else:
                                update_new = '{column_name}={job_id}_{table}_tmp.{column_name}'.format(column_name=j,
                                                                                                        job_id=job_id,
                                                                                                        table=filename)
                            update = update + update_new
                        other_columns = ['@'+x for x in result_columns if x not in new_columns]
                        other_columns = ','.join(other_columns)
                        engine.execute('ALTER TABLE {job_id}_{table} ADD UNIQUE ({pk});'.format(job_id=job_id,pk=pk[i]))
                        query = "insert into {job_id}_{table} select {job_id}_{table}_tmp.*,{other_columns} " \
                                "from {job_id}_{table}_tmp where {job_id}_{table}_tmp.{pk}={pk} on duplicate key update " \
                                "{update}".format(update=update, pk=pk[i], table=filename, job_id=job_id,
                                                  other_columns=other_columns)
                        engine.execute(query)
                        engine.execute(
                            'drop table if exists {job_id}_{filename}_tmp'.format(job_id=job_id, filename=filename))

                else:
                    column_string_to_list = columns[i].split(',')
                    update = ''
                    for j in column_string_to_list:
                        if (update):
                            update = update + ','
                        update = update + '{column_name}={job_id}_{table}_tmp.{column_name}'.format(column_name=j, job_id=job_id, table=filename)
                    query = "insert into {job_id}_{table} select * from {job_id}_{table}_tmp where {job_id}_{table}_tmp.{pk}={pk} on duplicate key update {update}".format(
                        update=update, pk=pk[i], table=filename, job_id=job_id)
                    result.to_sql('{job_id}_{filename}_tmp'.format(job_id=job_id,filename=filename), engine, if_exists='replace', index=False)
                    engine.execute(query)
                    engine.execute('drop table if exists {job_id}_{filename}_tmp'.format(job_id=job_id,filename=filename))

#mysql 은 기존 머지를 하되 없어진 칼럼은 ''로 하고, 나머지는 increasement 와 같이 처리(increasement와 같이 하는 처리를 먼저해야함)