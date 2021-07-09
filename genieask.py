from mysql.connector import connect, Error
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
import pandas

def genieask():
    query = '''
            SELECT *
            FROM t_genieask;
            '''

    # GET FROM GENIEBOOK.WAREHOUSE DATABASE
    engine = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse', pool_recycle=3600)
    dbConnection = engine.connect()
    results = pandas.read_sql_query(query, dbConnection) 
    # print(results)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # TO FIND OUT THE TIME SPENT ON GENIEASK
    # Dataframe: df
    # Table Name: t_genieask_time
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name = 't_genieask_time'

    df = results
    df['date_time'] = pd.to_datetime(df['date_time'])
    df['diff'] = df.sort_values(['user_id', 'class_id','date_time']).groupby(['user_id', 'class_id'])['date_time'].diff()
    df['diff'] = df['diff'].dt.seconds

    df.to_sql(name = table_name, con = dbConnection, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # TO FIND OUT THE TIME SPENT ON GENIEASK WITH FILTER 10MINUTES SESSION
    # Dataframe: df6
    # Table Name: t_genieask_time_10mins
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name6 = 't_genieask_time_10mins'

    df6 = results

    # CHANGING date_time TO A DATE TIME VALUE OTHERWISE WILL HAVE ERROR
    df6['date_time'] = pd.to_datetime(df['date_time'])

    # FIND TIME DIFFERENCE BETWEEN EACH USER MESSAGE
    df6['diff'] = df6.sort_values(['user_id', 'class_id','date_time']).groupby(['user_id', 'class_id'])['date_time'].diff()
    df6['diff'] = df6['diff'].dt.seconds

    # FILTER BASED ON TIME DIFFERENCE LESS THAN OR EQUALS TO 600 SECONDS
    df6 = df6[df['diff'] <= 600]  

    # CONVERT date_time to YEAR MONTH IN ORDER TO GROUP BY 
    df6['date_time'] = df6['date_time'].dt.strftime('%Y-%m')

    # FIND SUM OF TIME DIFFERENCE AND NUMBER OF UNIQUE STUDENTS
    df6 = df6.groupby(['date_time']).agg({'diff': 'sum', 'user_id': 'nunique'}).reset_index()

    # CHANGING date_time TO A DATE TIME VALUE FOR GOOGLE DATA STUDIO
    df6['date_time'] = pd.to_datetime(df6['date_time'] ,format='%Y-%m')
    df6 = df6.rename(columns={"diff" : "total_time", "user_id": "number_of_users"})

    df6.to_sql(name = table_name6, con = dbConnection, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # TO FIND OUT THE TIME SPENT ON GENIEASK WITH FILTER 10MINUTES SESSION
    # Dataframe: df
    # Table Name: t_genieask_time_10mins_per_class
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name2 = 't_genieask_time_10mins_per_class'

    df2 = results
    df2['date_time'] = pd.to_datetime(df2['date_time'])
    df2['diff'] = df2.sort_values(['user_id','date_time']).groupby('user_id')['date_time'].diff()
    df2['diff'] = df2['diff'].dt.seconds

    # 10 MINUTES 
    df2 = df2[df2['diff'] <= 600]  
    
    # CONVERT TO YEAR MONTH
    df2['date_time'] = df2['date_time'].dt.strftime('%Y-%m')


    df2 = df2.groupby(['user_id', 'user_name', 'class_id', 'class_name', 'date_time'])['diff'].sum().reset_index(name='total_time')
    df2['date_time'] = pd.to_datetime(df2['date_time'] ,format='%Y-%m')

    # FIND SUM OF TIME DIFFERENCE AND NUMBER OF UNIQUE STUDENTS
    df2 = df2.groupby(['class_id', 'class_name', 'date_time']).agg({'total_time': 'sum', 'user_id': 'nunique'}).reset_index()
    df2 = df2.rename(columns={"user_id": "number_of_users"})

    df2.to_sql(name = table_name2, con = dbConnection, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)
    
    # ------------------------------------------------------------------------------------------------------------------------------------  
    # TO FIND OUT THE DAU
    # Dataframe: df3
    # Table Name: t_genieask_dau
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name3 = 't_genieask_dau'

    df3 = results
    
    # REFORMAT date_time TO JUST SHOW THE DATE
    df3['date_time'] = df3['date_time'].dt.date
    
    # TO FIND NUMBER OF USERS PER YEAR MONTH
    df3 = df3.groupby(['date_time']).user_id.nunique().reset_index(name='number_of_users')
    # print(df3)

    df3.to_sql(name = table_name3, con = dbConnection, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------  
    # TO FIND OUT THE WAU
    # Dataframe: df4
    # Table Name: t_genieask_wau
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name4 = 't_genieask_wau'

    df4 = results

    # REFORMATTING THE DATA TO SEPARATE OUT THE WEEK AND YEAR
    df4['date_time'] = pd.to_datetime(df4['date_time'])
    df4['week_number'] = df4['date_time'].dt.isocalendar().week
    df4['year'] = df4['date_time'].dt.isocalendar().year 

    # TO FIND THE USER AND THE YEAR WEEK THAT THEY ARE ACTIVE IN
    df4 = df4.groupby(['user_id', 'week_number', 'year']).agg({'date_time' : 'min'}).reset_index().rename(columns={'date_time' : 'earliest_date_submitted'})
    
    # print(df4)
    df4.to_sql(name = table_name4, con = dbConnection, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------  
    # TO FIND OUT THE DAU
    # Dataframe: df5
    # Table Name: t_genieask_mau
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name5 = 't_genieask_mau'

    df5 = results
    
    df5['date'] = df5['date_time'].dt.strftime('%Y-%m')

    # TO FIND NUMBER OF USERS PER MONTH
    df5 = df5.groupby(['date']).user_id.nunique().reset_index(name='number_of_users')
    df5['date'] = pd.to_datetime(df5['date'] ,format='%Y-%m')
    
    # print(df5)
    df5.to_sql(name = table_name5, con = dbConnection, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)


    # CLOSE CONNECTION
    dbConnection.close()

genieask()