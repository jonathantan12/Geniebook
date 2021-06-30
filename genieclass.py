from mysql.connector import connect, Error
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
import pandas

def my_func():
        # QUERYING STUDENTS ANSWERS
        # start = "2020-01-01 00:00:00"
        # end = "2020-02-01 00:00:00"

        query = '''
                SELECT *
                FROM geniebook.online_lesson_log;
                '''

        # GET FROM GENIEBOOK DATABASE
        engine = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/geniebook', pool_recycle=3600)
        dbConnection = engine.connect()

        results = pandas.read_sql_query(query, dbConnection) 
        print(results)

        #QUERYING EFFECTIVE STUDENTS ONLY
        query2 = '''
                SELECT *
                FROM t_student_effective_date_w_amount;
                '''

        # GET FROM WAREHOUSE DATABASE
        engine2 = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse', pool_recycle=3600)
        dbConnection2 = engine2.connect()

        results2 = pandas.read_sql_query(query2, dbConnection2) 
        print(results2)

        # JOIN geniebook.online_lesson_log and t_student_effective_date_w_amount
        df = results.merge(results2, how='inner', left_on='user_id', right_on='student_id')
        df = df[df['date_created'] >= df['payment_date']]  
        print(df)

        # CREATE NEW TABLE
        table_name = 't_genieclass'
        df.to_sql(name = table_name, con = dbConnection2, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

        # ------------------------------------------------------------------------------------------------------------------------------------
        # Query table t_genieclass
        # ------------------------------------------------------------------------------------------------------------------------------------
        query3 = '''
                SELECT *
                FROM t_genieclass;
                '''

        results3 = pandas.read_sql_query(query3, dbConnection2)
        print(results3)

        # ------------------------------------------------------------------------------------------------------------------------------------
        # TO FIND OUT THE DAU
        # Dataframe: df2
        # Table Name: CREATE t_genieclass_dau
        # ------------------------------------------------------------------------------------------------------------------------------------
        table_name2 = 't_genieclass_dau'

        df2 = results3
        
        df2['date_created'] = df2['date_created'].dt.date
        
        # TO FIND NUMBER OF USERS PER YEAR MONTH
        df2 = df2.groupby(['date_created']).student_id.nunique().reset_index(name='number_of_users')
        print(df2)

        df2.to_sql(name = table_name2, con = dbConnection2, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

        # ------------------------------------------------------------------------------------------------------------------------------------
        # TO FIND OUT THE WAU 
        # Dataframe: df3
        # Table Name: CREATE t_genieclass_wau
        # ------------------------------------------------------------------------------------------------------------------------------------
        table_name3 = 't_genieclass_wau'

        df3 = results3

        df3['date_created'] = pd.to_datetime(df3['date_created'])
        df3['week_number'] = df3['date_created'].dt.isocalendar().week
        df3['year'] = df3['date_created'].dt.isocalendar().year 

        df3 = df3.groupby(['student_id', 'week_number', 'year']).agg({'date_created' : 'min'}).reset_index().rename(columns={'date_created' : 'earliest_date_submitted'})
        
        print(df3)
        df3.to_sql(name = table_name3, con = dbConnection2, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

        # ------------------------------------------------------------------------------------------------------------------------------------
        # TO FIND OUT THE MAU 
        # Dataframe: df4
        # Table Name: CREATE t_genieclass_mau
        # ------------------------------------------------------------------------------------------------------------------------------------
        table_name4 = 't_genieclass_mau'

        df4 = results3
        
        df4['date'] = df4['date_created'].dt.strftime('%Y-%m')
        print(df4)

        # TO FIND NUMBER OF USERS PER YEAR WEEK
        df4 = df4.groupby(['date']).student_id.nunique().reset_index(name='number_of_users')
        df4['date'] = pd.to_datetime(df4['date'] ,format='%Y-%m')
        print(df4)

        df4.to_sql(name = table_name4, con = dbConnection2, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

        # CLOSE CONNECTION
        dbConnection.close()
        dbConnection2.close()


my_func()
