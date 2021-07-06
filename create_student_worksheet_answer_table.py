from mysql.connector import connect, Error
import json
import string
import pandas as pd
from io import StringIO
import sys
import csv
from sqlalchemy import create_engine
import pandas

def my_func(start, end):
    table_name_date_reformat = start[:4]
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
    print(results)
    
    engine2 = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse')

    #IMPORT INTO GENIEBOOK.WAREHOUSE DATABASE
    results.to_sql(name = table_name, con = engine2, schema=None, if_exists='append', index=None, index_label=None, chunksize=10000, dtype=None, method=None)
 

def database(year, start_month, end_month):
    for i in range(start_month, end_month+1):
        if i < 9:
            start_date = f'{year}-0{i}-01'
            end_date = f'{year}-0{i+1}-01'

        elif i == 9:
            start_date = f'{year}-0{i}-01'
            end_date = f'{year}-{i+1}-01'

        elif i == 12:
            start_date = f'{year}-{i}-01'
            end_date = f'{year+1}-01-01'

        else:
            start_date = f'{year}-{i}-01'
            end_date = f'{year}-{i+1}-01'

        print(start_date)
        print(end_date)
        print('Database Created for ' + f'{start_date}')

        #Calling my_func
        my_func(start_date, end_date)


year = int(input('Enter year: '))
start_month = int(input('Enter start month required: '))
end_month = int(input('Enter end month required: '))

database(year, start_month, end_month)