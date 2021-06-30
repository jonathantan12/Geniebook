from mysql.connector import connect, Error
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from time import sleep
from datetime import datetime
from dateutil.relativedelta import *

def update_effective_students():
    table_name = f't_student_effective_date_w_amount' #RMB TO CHANGE TABLE NAME

    query = '''
            SELECT student_id, min(effective_date) as effective_date, min(payment_date) as payment_date, sum(amount_paid) as total_amount_paid
            FROM geniebook.student_subscription_payment
            WHERE amount_paid > 0
            GROUP BY student_id
            HAVING student_id not in (SELECT student_id FROM warehouse.t_test_student_ids);
            '''

    # GET FROM GENIEBOOK.WAREHOUSE DATABASE
    engine = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/geniebook', pool_recycle=3600)
    dbConnection = engine.connect()

    results = pandas.read_sql_query(query, dbConnection) 
    print(results)

    engine2 = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse', pool_recycle=3600)
    dbConnection2 = engine2.connect()

    #IMPORT INTO GENIEBOOK.WAREHOUSE DATABASE
    results.to_sql(name = table_name, con = engine2, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    #CLOSE CONNECTION
    dbConnection.close()
    dbConnection2.close()

def delete_data_tswa():
    start = str(datetime.now().date())

    if start[-2:] == '01':
        print('new month')
        date_change_start = start[:5] + '0' + str(int(start[5:7])-1) + "-01"
        start = date_change_start + " 00:00:00"

        end = str((datetime.now() + relativedelta(months=+1)).date())
        date_change_end = end[:5] + '0' + str(int(end[5:7])-1) + "-01"
        end = date_change_end + " 00:00:00"
    
    else:
        date_change_start = str(start)[:-3] + "-01"
        start = str(date_change_start) + " 00:00:00"

        end = (datetime.now() + relativedelta(months=+1)).date()
        date_change_end = str(end)[:-3] + "-01"
        end = str(date_change_end) + " 00:00:00"

    #CONNECTING TO ENGINE
    engine = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse', pool_recycle=3600)
    dbConnection = engine.connect()
    # ------------------------------------------------------------------------------------------------------------------------------------
    # DELETE FROM t_student_worksheet_answer
    # Table Name: t_student_worksheet_answer
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name = 't_student_worksheet_answer'
    delete_query = "DELETE FROM %s WHERE date_created >= '%s' AND date_created < '%s';" % (table_name, start, end)
    dbConnection.execute(delete_query)

    print(f'successfully deleted from {table_name}. For date ranging from {start} to {end}')
    #CLOSE CONNECTION
    dbConnection.close()

def update_data_tswa():
    start = str(datetime.now().date())

    if start[-2:] == '01':
        print('new month')
        date_change_start = start[:5] + '0' + str(int(start[5:7])-1) + "-01"
        start = date_change_start + " 00:00:00"

        end = str((datetime.now() + relativedelta(months=+1)).date())
        date_change_end = end[:5] + '0' + str(int(end[5:7])-1) + "-01"
        end = date_change_end + " 00:00:00"
    
    else:
        date_change_start = str(start)[:-3] + "-01"
        start = str(date_change_start) + " 00:00:00"

        end = (datetime.now() + relativedelta(months=+1)).date()
        date_change_end = str(end)[:-3] + "-01"
        end = str(date_change_end) + " 00:00:00"

    table_name = f't_student_worksheet_answer' #RMB TO CHANGE TABLE NAME

    query = '''
            SELECT answer_id, student_id, worksheet_id, part_id, answer, answer_type, max_marks, answer_time, date_created, date_submitted
            FROM student_worksheet_answer 
            WHERE date_created >= %s AND date_created < %s;
            '''
        
    # GET FROM GENIEBOOK.WAREHOUSE DATABASE
    engine = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/geniebook', pool_recycle=3600)
    dbConnection = engine.connect()

    results = pandas.read_sql_query(query, dbConnection, params=[start, end]) 
    # print(results)
    
    engine2 = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse', pool_recycle=3600)
    dbConnection2 = engine2.connect()
    #IMPORT INTO GENIEBOOK.WAREHOUSE DATABASE
    results.to_sql(name = table_name, con = dbConnection2, schema=None, if_exists='append', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    dbConnection.close()
    dbConnection2.close()

def delete_data():
    #GETTING START AND END DATE
    start = str(datetime.now().date())

    if start[-2:] == '01':
        print('new month')
        date_change_start = start[:5] + '0' + str(int(start[5:7])-1) + "-01"
        start = date_change_start + " 00:00:00"

        end = str((datetime.now() + relativedelta(months=+1)).date())
        date_change_end = end[:5] + '0' + str(int(end[5:7])-1) + "-01"
        end = date_change_end + " 00:00:00"
    
    else:
        date_change_start = str(start)[:-3] + "-01"
        start = str(date_change_start) + " 00:00:00"

        end = (datetime.now() + relativedelta(months=+1)).date()
        date_change_end = str(end)[:-3] + "-01"
        end = str(date_change_end) + " 00:00:00"

    #CONNECTING TO ENGINE
    engine = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse', pool_recycle=3600)
    dbConnection = engine.connect()
    # ------------------------------------------------------------------------------------------------------------------------------------
    # DELETE FROM t_geniesmart_dau_gt0
    # Table Name: t_geniesmart_dau_gt0
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name = 't_geniesmart_dau_gt0'
    delete_query = "DELETE FROM %s WHERE date_submitted >= '%s' AND date_submitted < '%s';" % (table_name, start, end)
    dbConnection.execute(delete_query)

    print(f'successfully deleted from {table_name}. For date ranging from {start} to {end}')

    # ------------------------------------------------------------------------------------------------------------------------------------
    # DELETE FROM t_geniesmart_dau_gt19
    # Table Name: t_geniesmart_dau_gt19
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name2 = 't_geniesmart_dau_gt19'
    delete_query2 = "DELETE FROM %s WHERE date_submitted >= '%s' AND date_submitted < '%s';" % (table_name2, start, end)
    dbConnection.execute(delete_query2)

    print(f'successfully deleted from {table_name2}. For date ranging from {start} to {end}')

    # ------------------------------------------------------------------------------------------------------------------------------------
    # DELETE FROM t_geniesmart_wau_gt0
    # Table Name: t_geniesmart_wau_gt0
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name3 = 't_geniesmart_wau_gt0'

    delete_query3 = "DELETE FROM %s WHERE earliest_date_submitted >= '%s' AND earliest_date_submitted < '%s';" % (table_name3, start, end)
    dbConnection.execute(delete_query3)

    print(f'successfully deleted from {table_name3}. For date ranging from {start} to {end}')

    # ------------------------------------------------------------------------------------------------------------------------------------
    # DELETE FROM t_geniesmart_wau_gt19
    # Table Name: t_geniesmart_wau_gt19
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name4 = 't_geniesmart_wau_gt19'

    delete_query4 = "DELETE FROM %s WHERE earliest_date_submitted >= '%s' AND earliest_date_submitted < '%s';" % (table_name4, start, end)
    dbConnection.execute(delete_query4)

    print(f'successfully deleted from {table_name4}. For date ranging from {start} to {end}')

    # ------------------------------------------------------------------------------------------------------------------------------------
    # DELETE FROM t_geniesmart_mau_gt0
    # Table Name: t_geniesmart_mau_gt0
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name5 = 't_geniesmart_mau_gt0'

    delete_query5 = "DELETE FROM %s WHERE date >= '%s' AND date < '%s';" % (table_name5, start, end)
    dbConnection.execute(delete_query5)

    print(f'successfully deleted from {table_name5}. For date ranging from {start} to {end}')

    # ------------------------------------------------------------------------------------------------------------------------------------
    # DELETE FROM t_geniesmart_mau_gt19
    # Table Name: t_geniesmart_mau_gt19
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name6 = 't_geniesmart_mau_gt19'

    delete_query6 = "DELETE FROM %s WHERE date >= '%s' AND date < '%s';" % (table_name6, start, end)
    dbConnection.execute(delete_query6)

    print(f'successfully deleted from {table_name6}. For date ranging from {start} to {end}')

    # ------------------------------------------------------------------------------------------------------------------------------------
    # DELETE FROM t_geniesmart_time
    # Table Name: t_geniesmart_time
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name7 = 't_geniesmart_time'

    delete_query7 = "DELETE FROM %s WHERE date >= '%s' AND date < '%s';" % (table_name7, start, end)
    dbConnection.execute(delete_query7)

    print(f'successfully deleted from {table_name7}. For date ranging from {start} to {end}')

    dbConnection.close()

def my_func():
    start = str(datetime.now().date())

    if start[-2:] == '01':
        print('new month')
        date_change_start = start[:5] + '0' + str(int(start[5:7])-1) + "-01"
        start = date_change_start + " 00:00:00"

        end = str((datetime.now() + relativedelta(months=+1)).date())
        date_change_end = end[:5] + '0' + str(int(end[5:7])-1) + "-01"
        end = date_change_end + " 00:00:00"
    
    else:
        date_change_start = str(start)[:-3] + "-01"
        start = str(date_change_start) + " 00:00:00"

        end = (datetime.now() + relativedelta(months=+1)).date()
        date_change_end = str(end)[:-3] + "-01"
        end = str(date_change_end) + " 00:00:00"

    #QUERYING STUDENTS ANSWERS
    query = '''
            SELECT *
            FROM t_student_worksheet_answer
            WHERE date_submitted >= %s AND date_submitted < %s;
            '''

    # GET FROM GENIEBOOK.WAREHOUSE DATABASE
    engine = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse', pool_recycle=3600)
    dbConnection = engine.connect()
    results = pandas.read_sql_query(query, dbConnection, params=[start, end]) 

    #QUERYING EFFECTIVE STUDENTS ONLY
    query2 = '''
            SELECT *
            FROM t_student_effective_date_w_amount;
            '''

    results2 = pandas.read_sql_query(query2, dbConnection) 
    # print(results2)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # TO FIND OUT THE DAU (MARKS >= 1)
    # Dataframe: df
    # Table Name: CREATE t_geniesmart_dau_gt0
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name = 't_geniesmart_dau_gt0'

    df = results
    df = df.merge(results2, how='inner', on='student_id')
    df['effective_date'] = pd.to_datetime(df['effective_date'])
    df = df[df['date_submitted'] >= df['effective_date']]  
    
    df['date_submitted'] = df['date_submitted'].dt.date
    # print(df)

    # TO FIND NUMBER OF USERS PER YEAR MONTH
    df = df.groupby(['date_submitted']).student_id.nunique().reset_index(name='number_of_users')
    # print(df)

    df.to_sql(name = table_name, con = engine, schema=None, if_exists='append', index=None, index_label=None, chunksize=10000, dtype=None, method=None)
    # ------------------------------------------------------------------------------------------------------------------------------------
    # TO FIND OUT THE DAU (MARKS >= 20)
    # Dataframe: df2
    # Table Name: t_geniesmart_dau_gt19
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name2 = 't_geniesmart_dau_gt19'

    df2 = results
    df2 = df2.merge(results2, how='inner', on='student_id')
    df2['effective_date'] = pd.to_datetime(df2['effective_date'])
    df2 = df2[df2['date_submitted'] >= df2['effective_date']]  
    
    df2['date_submitted'] = df2['date_submitted'].dt.date
    # print(df2)

    # TO FIND THE INDIVIDUAL STUDENT MARKS
    df2 = df2.groupby(['student_id', 'date_submitted']).max_marks.sum().reset_index(name='total_marks')
    df2 = df2[df2['total_marks'] >= 20] #THIS LINE OF CODE HELPS TO FILTER OUT THE STUDENTS THAT HAVE TOTAL MARKS >= 20
    # print(df2)

    # TO FIND NUMBER OF USERS PER YEAR MONTH
    df2 = df2.groupby(['date_submitted']).student_id.nunique().reset_index(name='number_of_users')
    # print(df2)

    df2.to_sql(name = table_name2, con = engine, schema=None, if_exists='append', index=None, index_label=None, chunksize=10000, dtype=None, method=None)
    # ------------------------------------------------------------------------------------------------------------------------------------
    # TO FIND OUT THE WAU (MARKS >= 1)
    # Dataframe: df3
    # Table Name: t_geniesmart_wau_gt0
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name3 = 't_geniesmart_wau_gt0'
   
    df3 = results
    df3 = df3.merge(results2, how='inner', on='student_id')
    df3['effective_date'] = pd.to_datetime(df3['effective_date'])
    df3 = df3[df3['date_submitted'] >= df3['effective_date']]  
    
    # df['date_submitted'] = pd.to_datetime(df['date_submitted'],format='%Y-%m-%d')
    df3['week_number'] = df3['date_submitted'].dt.isocalendar().week
    df3['year'] = df3['date_submitted'].dt.isocalendar().year 
    # print(df3)

    # TO FIND NUMBER OF USERS PER YEAR WEEK
    df3 = df3.groupby(['student_id', 'week_number', 'year']).agg({'max_marks' : 'sum', 'date_submitted' : 'min'}).reset_index().rename(columns={'max_marks': 'total_marks', 'date_submitted' : 'earliest_date_submitted'})
    # df3 = df3.groupby(['student_id', 'week_number', 'year']).max_marks.sum().reset_index(name='total_marks')
    # print(df3)

    df3.to_sql(name = table_name3, con = engine, schema=None, if_exists='append', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # TO FIND OUT THE WAU (MARKS >= 20)
    # Dataframe: df4
    # Table Name: t_geniesmart_wau_gt19
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name4 = 't_geniesmart_wau_gt19'

    df4 = results
    df4 = df4.merge(results2, how='inner', on='student_id')
    df4['effective_date'] = pd.to_datetime(df4['effective_date'])
    df4 = df4[df4['date_submitted'] >= df4['effective_date']]  
    
    # df['date_submitted'] = pd.to_datetime(df['date_submitted'],format='%Y-%m-%d')
    df4['week_number'] = df4['date_submitted'].dt.isocalendar().week
    df4['year'] = df4['date_submitted'].dt.isocalendar().year 
    # print(df4)

    # TO FIND THE INDIVIDUAL STUDENT MARKS
    df4 = df4.groupby(['student_id', 'week_number', 'year']).agg({'max_marks' : 'sum', 'date_submitted' : 'min'}).reset_index().rename(columns={'max_marks': 'total_marks', 'date_submitted' : 'earliest_date_submitted' })
    # df4 = df4.groupby(['student_id', 'week_number', 'year']).max_marks.sum().reset_index(name='total_marks')
    df4 = df4[df4['total_marks'] >= 20] #THIS LINE OF CODE HELPS TO FILTER OUT THE STUDENTS THAT HAVE TOTAL MARKS >= 20
    # print(df4)

    # TO FIND NUMBER OF USERS PER YEAR WEEK
    # df4 = df4.groupby(['week_number', 'year']).student_id.nunique().reset_index(name='number_of_users')
    # print(df4)

    df4.to_sql(name = table_name4, con = engine, schema=None, if_exists='append', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # TO FIND OUT THE MAU (MARKS >= 1)
    # Dataframe: df5
    # Table Name: t_geniesmart_mau_gt0
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name5 = 't_geniesmart_mau_gt0'

    df5 = results
    df5 = df5.merge(results2, how='inner', on='student_id')
    df5['effective_date'] = pd.to_datetime(df5['effective_date'])
    df5 = df5[df5['date_submitted'] >= df5['effective_date']]  
    
    # df['date_submitted'] = pd.to_datetime(df['date_submitted'],format='%Y-%m-%d')
    # df5['month'] = df5['date_submitted'].dt.month
    # df5['year'] = df5['date_submitted'].dt.year 
    df5['date'] = df5['date_submitted'].dt.strftime('%Y-%m')
    # print(df5)

    # TO FIND NUMBER OF USERS PER YEAR WEEK
    df5 = df5.groupby(['date']).student_id.nunique().reset_index(name='number_of_users')
    df5['date'] = pd.to_datetime(df5['date'] ,format='%Y-%m')
    # print(df5)

    df5.to_sql(name = table_name5, con = engine, schema=None, if_exists='append', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # TO FIND OUT THE MAU (MARKS >= 20)
    # Dataframe: df6
    # Table Name: t_geniesmart_mau_gt19
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name6 = 't_geniesmart_mau_gt19'

    df6 = results
    df6 = df6.merge(results2, how='inner', on='student_id')
    df6['effective_date'] = pd.to_datetime(df6['effective_date'])
    df6 = df6[df6['date_submitted'] >= df6['effective_date']]  
    
    # df['date_submitted'] = pd.to_datetime(df['date_submitted'],format='%Y-%m-%d')
    # df6['month'] = df6['date_submitted'].dt.month
    # df6['year'] = df6['date_submitted'].dt.year 
    df6['date'] = df6['date_submitted'].dt.strftime('%Y-%m')
    # print(df6)

    # TO FIND THE INDIVIDUAL STUDENT MARKS
    df6 = df6.groupby(['student_id', 'date']).max_marks.sum().reset_index(name='total_marks')
    df6 = df6[df6['total_marks'] >= 20] #THIS LINE OF CODE HELPS TO FILTER OUT THE STUDENTS THAT HAVE TOTAL MARKS >= 20
    # print(df6)

    # TO FIND NUMBER OF USERS PER YEAR WEEK
    df6 = df6.groupby(['date']).student_id.nunique().reset_index(name='number_of_users')
    df6['date'] = pd.to_datetime(df6['date'] ,format='%Y-%m')
    # print(df6)
   
    df6.to_sql(name = table_name6, con = engine, schema=None, if_exists='append', index=None, index_label=None, chunksize=10000, dtype=None, method=None)
    
    # ------------------------------------------------------------------------------------------------------------------------------------
    # TO FIND OUT THE AVERAGE TIME SPENT ON GENIESMART
    # Dataframe: df7
    # Table Name: t_geniesmart_time
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name7 = 't_geniesmart_time'

    df7 = results
    df7['diff'] = df7.sort_values(['student_id','date_submitted']).groupby('student_id')['date_submitted'].diff()
    df7['diff'] = df7['diff'].dt.seconds
    # df7 = df7.dropna(subset=['diff'])
    # df7.drop(df7['diff'] , inplace=False)
    df7 = df7[df7['diff'] < 7200]  
    
    #FILTERING ONLY EFFECTIVE STUDENTS
    df7 = df7.merge(results2, how='inner', on='student_id')
    df7['effective_date'] = pd.to_datetime(df7['effective_date'])
    df7 = df7[df7['date_submitted'] >= df7['effective_date']]  
    # print(df7)

    #GETTING SESSION TIMES
    total_time = df7['diff'].sum()
    # print('total_time: ' + str(total_time))

    number_of_users = df5.iloc[0, 1]
    # print(number_of_users)

    avg_user_time = total_time / number_of_users
    # print('average user time: ' + str(avg_user_time))

    geniesmart_time = {"date": [start],
                        "total_time": [total_time],
                        "number_of_users": [number_of_users],
                        "avg_user_time": [avg_user_time]}

    DataFrame = pd.DataFrame(data=geniesmart_time)
    print(DataFrame)

    DataFrame.to_sql(name = table_name7, con = engine, schema=None, if_exists='append', index=None, index_label=None, chunksize=10000, dtype=None, method=None)
   
    print(f"successfully upload onto table for date_submitted start {start} to date_submitted end {end}.")

    #CLOSE CONNECTION
    dbConnection.close()


default_args = {
    'owner': 'Jonathan',
}

with DAG('GenieSmart', default_args=default_args, description='GenieSmart', schedule_interval='@daily', start_date=datetime(2021, 6, 21), catchup=False) as dag:
    update_effective_students = PythonOperator(task_id='update_effective_students', python_callable=update_effective_students)
    delete_data_tswa = PythonOperator(task_id='delete_data_tswa', python_callable=delete_data_tswa)
    update_data_tswa = PythonOperator(task_id='update_data_tswa', python_callable=update_data_tswa)
    delete_data = PythonOperator(task_id='delete_data', python_callable=delete_data)
    my_func = PythonOperator(task_id='my_func', python_callable=my_func)
  
    update_effective_students >> delete_data_tswa >> update_data_tswa >> delete_data >> my_func