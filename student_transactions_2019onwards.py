from mysql.connector import connect, Error
import json
import string
import pandas as pd
from io import StringIO
import sys
import csv
from sqlalchemy import create_engine
import pandas

def student_transactions():
    # ------------------------------------------------------------------------------------------------------------------------------------
    # CONNECT TO GENIEBOOK DATABASE
    # ------------------------------------------------------------------------------------------------------------------------------------
    engine = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/geniebook', pool_recycle=3600)
    dbConnection = engine.connect()

    # ------------------------------------------------------------------------------------------------------------------------------------
    # QUERY STUDENT DETAILS
    # ------------------------------------------------------------------------------------------------------------------------------------

    # query = '''
    #         SELECT student_id, name as student_name, last_name as student_last_name, parent_contact, parent_contact_2, parent_home_number, parent_home_address, 
    #         parent_email_address, parent_email_address_2, parent_father_name, parent_mother_name, 
    #         student_contact, student_gender, student_dob, student_email_address, student_level, date_updated as student_details_date_updated
    #         FROM student;
    #         '''
    query = '''
            SELECT student_id, name as student_name, last_name as student_last_name, student_contact, student_gender, student_dob, student_email_address, student_level, 
            parent_info_id, date_updated as student_details_date_updated
            FROM student;
            '''

    results = pandas.read_sql_query(query, dbConnection) 
    print(results)
    results['student_fullname'] = results['student_last_name'] + ' ' + results['student_name']
    results = results.drop(columns=['student_last_name', 'student_name'])
    results['student_fullname'] = results['student_fullname'].str.strip()
    
    print(results)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # QUERY PARENT DETAILS
    # ------------------------------------------------------------------------------------------------------------------------------------
    query5 = '''
            SELECT parent_info_id, first_name as parent_first_name, last_name as parent_last_name, mobile as parent_mobile, phone as parent_phone, email_address as parent_email_address,
            address, postal_code, date_updated as parent_details_date_updated
            FROM parent_info;
            '''

    results5 = pandas.read_sql_query(query5, dbConnection) 
    results5['parent_fullname'] = results5['parent_last_name'] + ' ' + results5['parent_first_name']
    results5 = results5.drop(columns=['parent_first_name', 'parent_last_name'])
    results5['parent_fullname'] = results5['parent_fullname'].str.strip()
    
    results = results.merge(results5, how='inner', left_on='parent_info_id', right_on='parent_info_id')
    # ------------------------------------------------------------------------------------------------------------------------------------
    # QUERY STUDENT SUBSCRIPTION PAYMENT DETAILS
    # ------------------------------------------------------------------------------------------------------------------------------------

    query2 = '''
            SELECT payment_id, student_id, subject_id, amount_paid, country_id, payment_date, effective_date
            FROM student_subscription_payment
            WHERE payment_date >= "2019-10-01 00:00:00" AND amount_paid > 0 AND student_id not in (SELECT student_id FROM warehouse.t_test_student_ids);
            '''

    results2 = pandas.read_sql_query(query2, dbConnection) 
    print(results2)

    # JOIN student WITH student_subscription_payment
    results = results.merge(results2, how='inner', left_on='student_id', right_on='student_id')
    print(results)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # QUERY SUBJECTS
    # ------------------------------------------------------------------------------------------------------------------------------------

    query3 = '''
            SELECT subject_id, subject, code as subject_code, display_name as subject_display_name
            FROM subject_id;
            '''

    results3 = pandas.read_sql_query(query3, dbConnection) 
    print(results3)

    results = results.merge(results3, how='inner', left_on='subject_id', right_on='subject_id')

    # ------------------------------------------------------------------------------------------------------------------------------------
    # QUERY USER COUNTRY
    # ------------------------------------------------------------------------------------------------------------------------------------

    query4 = '''
            SELECT country_id, code as country_code, name as country_name
            FROM countries;
            '''

    results4 = pandas.read_sql_query(query4, dbConnection) 
    print(results4)

    results = results.merge(results4, how='inner', left_on='country_id', right_on='country_id')
    
    print(results)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # CONNECT TO WAREHOUSE DATABSE
    # ------------------------------------------------------------------------------------------------------------------------------------
    engine2 = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse', pool_recycle=3600)
    dbConnection2 = engine2.connect()

    #IMPORT INTO GENIEBOOK.WAREHOUSE DATABASE
    table_name = 't_student_transactions_from_oct2019'
    results.to_sql(name = table_name, con = dbConnection2, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # CLOSE CONNECTION
    dbConnection.close()
    dbConnection2.close()

student_transactions()