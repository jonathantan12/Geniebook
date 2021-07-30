from mysql.connector import connect, Error
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
import pandas

def bubblestore_order():
    # ------------------------------------------------------------------------------------------------------------------------------------
    # Query table bubbles_store.orders
    # ------------------------------------------------------------------------------------------------------------------------------------
    engine = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.203/bubbles_store', pool_recycle=3600)
    dbConnection = engine.connect()

    query = '''
            SELECT *
            FROM orders;
            '''

    results = pandas.read_sql_query(query, dbConnection) 

    print(results)
    
    # ------------------------------------------------------------------------------------------------------------------------------------
    # Query table bubbles_store.users
    # ------------------------------------------------------------------------------------------------------------------------------------
    query2 = '''
            SELECT *
            FROM users;
            '''

    results2 = pandas.read_sql_query(query2, dbConnection) 
    results2 = results2[results2['user_type'] == 1] #FITLER FOR ONLY STUDENTS 
    results = results.merge(results2, how='inner', left_on='user_id', right_on='user_id')

    print(results)
    # ------------------------------------------------------------------------------------------------------------------------------------
    # Query table geniebook.countries
    # ------------------------------------------------------------------------------------------------------------------------------------
    engine2 = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/geniebook', pool_recycle=3600)
    dbConnection2 = engine2.connect()

    query3 = '''
            SELECT *
            FROM countries;
            '''

    results3 = pandas.read_sql_query(query3, dbConnection2)

    # DROP UNNECESSARY COLUMNS - KEEP country_id, code, name
    results3 = results3[['country_id', 'code', 'name']]

    results = results.merge(results3, how='inner', left_on='country_id', right_on='country_id')
    print(results)

    results = results.rename(columns={'created_at_x' : 'order_created_at' , 'created_at_y' : 'user_created_at', 'name_y' : 'country_name'})
    
    # FILTERING AWAY BUBBLE TEST PRODUCTION
    results = results[results['full_name'] != "Bubble Test Production"]

    print(results)
    
    # ------------------------------------------------------------------------------------------------------------------------------------
    # CONNECT TO WAREHOUSE DATABSE
    # ------------------------------------------------------------------------------------------------------------------------------------
    engine2 = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse', pool_recycle=3600)
    dbConnection2 = engine2.connect()

    # ------------------------------------------------------------------------------------------------------------------------------------
    # BUBBLE STORE ACTIVITY
    # Dataframe: results
    # Table Name: t_bubblestore_activity
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name = 't_bubblestore_activity'
    results.to_sql(name = table_name, con = dbConnection2, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # BUBBLE STORE "ORDERS ONLY" >= 1 - DAU
    # Dataframe: df
    # Table Name: t_bubblestore_orders_only_dau
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name2 = 't_bubblestore_orders_only_dau'

    df = results
    df['order_created_at'] = df['order_created_at'].dt.date
    
    # TO FIND NUMBER OF USERS PER DAY
    df = df.groupby(['order_created_at', 'country_name']).user_id.nunique().reset_index(name='number_of_users')

    print(df)
    df.to_sql(name = table_name2, con = dbConnection2, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # BUBBLE STORE "ORDERS ONLY" >= 1 - WAU
    # Dataframe: df2
    # Table Name: t_bubblestore_orders_only_wau
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name3 = 't_bubblestore_orders_only_wau'

    df2 = results
    
    df2['order_created_at'] = pd.to_datetime(df2['order_created_at'])

    # TO FIND NUMBER OF USERS PER YEAR WEEK
    df2['year_week'] = df2['order_created_at'].dt.strftime('%Y-%U')
    df2 = df2.groupby(['user_id', 'year_week', 'country_name']).agg({'order_created_at' : 'min'}).reset_index().rename(columns={'order_created_at' : 'earliest_date_submitted'})
    df2['earliest_date_submitted'] = df2['earliest_date_submitted'].dt.date

    print(df2)
    df2.to_sql(name = table_name3, con = dbConnection2, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # BUBBLE STORE "ORDERS ONLY" >= 1 - MAU
    # Dataframe: df3
    # Table Name: t_bubblestore_orders_only_mau
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name4 = 't_bubblestore_orders_only_mau'

    df3 = results
    
    df3['date'] = df3['order_created_at'].dt.strftime('%Y-%m')

    # TO FIND NUMBER OF USERS PER MONTH
    df3 = df3.groupby(['date', 'country_name']).user_id.nunique().reset_index(name='number_of_users')
    df3['date'] = pd.to_datetime(df3['date'] ,format='%Y-%m')

    print(df3)
    df3.to_sql(name = table_name4, con = dbConnection2, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)


    # CLOSE CONNECTION
    dbConnection.close()
    dbConnection2.close()

def bubblestore_order_wishlist_search():
    # ------------------------------------------------------------------------------------------------------------------------------------
    # CONNECT TO BUBBLE STORE DATABASE
    # ------------------------------------------------------------------------------------------------------------------------------------
    engine = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.203/bubbles_store', pool_recycle=3600)
    dbConnection = engine.connect()

    # ------------------------------------------------------------------------------------------------------------------------------------
    # QUERY FROM ORDERS
    # ------------------------------------------------------------------------------------------------------------------------------------
    query = '''
            SELECT *
            FROM orders;
            '''

    results = pandas.read_sql_query(query, dbConnection) 
    
    results = results[['user_id', 'created_at']]
    results['activity_type'] = 'order'
    # print(results)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # QUERY FROM WISHLIST
    # ------------------------------------------------------------------------------------------------------------------------------------
    query2 = '''
            SELECT *
            FROM wishlist;
            '''

    results2 = pandas.read_sql_query(query2, dbConnection)

    results2 = results2[['user_id', 'created_at']]
    results2['activity_type'] = 'wishlist'
    # print(results2)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # QUERY FROM SEARCH_HISTORY
    # ------------------------------------------------------------------------------------------------------------------------------------
    query3 = '''
            SELECT *
            FROM search_history;
            '''

    results3 = pandas.read_sql_query(query3, dbConnection)  

    results3 = results3[['student_id', 'created_at']]
    results3['activity_type'] = 'search'
    results3 = results3.rename(columns={'student_id' : 'user_id'})
    # print(results3)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # APPEND ALL 3 RESULTS
    # ------------------------------------------------------------------------------------------------------------------------------------
    frames = [results, results2, results3]
    df = pd.concat(frames)
    # print(df)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # JOIN WITH USER TABLE 
    # ------------------------------------------------------------------------------------------------------------------------------------
    query4 = '''
            SELECT *
            FROM users;
            '''

    results4 = pandas.read_sql_query(query4, dbConnection)  
    results4 = results4[results4['user_type'] == 1] #FITLER FOR ONLY STUDENTS 
    results4['is_moderator'] = results4['email'].str.contains("geniebook.com") #FILTER AWAY PEOPLE WITH EMAIL @GENIEBOOK BECAUSE THEY ARE NOT STUDENTS
    results4 = results4[results4['is_moderator'] == False]
    results4 = results4.drop(columns=['is_moderator'])

    df = df.merge(results4, how='inner', left_on='user_id', right_on='user_id')
    # print(df)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # CONNECT TO GENIEBOOK DATABASE
    # ------------------------------------------------------------------------------------------------------------------------------------
    engine2 = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/geniebook', pool_recycle=3600)
    dbConnection2 = engine2.connect()

    # ------------------------------------------------------------------------------------------------------------------------------------
    # JOIN WITH GENIEBOOK.CONTRIES TABLE 
    # ------------------------------------------------------------------------------------------------------------------------------------
    query5 = '''
            SELECT *
            FROM countries;
            '''

    results5 = pandas.read_sql_query(query5, dbConnection2)

    # DROP UNNECESSARY COLUMNS - KEEP country_id, code, name
    results5 = results5[['country_id', 'code', 'name']]

    df = df.merge(results5, how='inner', left_on='country_id', right_on='country_id')
    df = df.drop(columns=['user_related_id'])
    # print(df)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # RENAME DF COLUMNS AND FILTER DF
    # ------------------------------------------------------------------------------------------------------------------------------------
    df = df.rename(columns={'created_at_x' : 'created_at' , 'created_at_y' : 'user_created_at', 'updated_at': 'user_updated_at','name_x': 'full_name','name_y' : 'country_name'})
    df = df[df['full_name'] != "Bubble Test Production"]

    print(df)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # CONNECT TO WAREHOUSE DATABASE
    # ------------------------------------------------------------------------------------------------------------------------------------
    engine3 = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse', pool_recycle=3600)
    dbConnection3 = engine3.connect()

    df.to_sql(name = 't_bubblestore_order_wishlist_search', con = dbConnection3, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)
    # ------------------------------------------------------------------------------------------------------------------------------------
    # BUBBLE STORE ANY [ORDER, SEARCHES, WISHLIST] >= 1 - DAU
    # Dataframe: df1
    # Table Name: t_bubblestore_any_dau
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name = 't_bubblestore_any_dau'

    df1 = df
    df1['created_at'] = df['created_at'].dt.date
    
    # TO FIND NUMBER OF USERS PER DAY
    df1 = df1.groupby(['created_at', 'country_name']).user_id.nunique().reset_index(name='number_of_users')

    print(df1)
    df1.to_sql(name = table_name, con = dbConnection3, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # BUBBLE STORE "ORDERS ONLY" >= 1 - WAU
    # Dataframe: df2
    # Table Name: t_bubblestore_any_wau
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name2 = 't_bubblestore_any_wau'

    df2 = df
    
    df2['created_at'] = pd.to_datetime(df2['created_at'])

    # TO FIND NUMBER OF USERS PER YEAR WEEK
    df2['year_week'] = df2['created_at'].dt.strftime('%Y-%U')
    df2 = df2.groupby(['user_id', 'year_week', 'country_name']).agg({'created_at' : 'min'}).reset_index().rename(columns={'created_at' : 'earliest_date_submitted'})
    df2['earliest_date_submitted'] = df2['earliest_date_submitted'].dt.date

    print(df2)
    df2.to_sql(name = table_name2, con = dbConnection3, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    # ------------------------------------------------------------------------------------------------------------------------------------
    # BUBBLE STORE "ORDERS ONLY" >= 1 - MAU
    # Dataframe: df3
    # Table Name: t_bubblestore_any_mau
    # ------------------------------------------------------------------------------------------------------------------------------------
    table_name4 = 't_bubblestore_any_mau'

    df3 = df
    
    df3['date'] = df3['created_at'].dt.strftime('%Y-%m')

    # TO FIND NUMBER OF USERS PER MONTH
    df3 = df3.groupby(['date', 'country_name']).user_id.nunique().reset_index(name='number_of_users')
    df3['date'] = pd.to_datetime(df3['date'] ,format='%Y-%m')

    print(df3)
    df3.to_sql(name = table_name4, con = dbConnection3, schema=None, if_exists='replace', index=None, index_label=None, chunksize=10000, dtype=None, method=None)
    
    # CLOSE CONNECTION
    dbConnection.close()
    dbConnection2.close()
    dbConnection3.close()

bubblestore_order()
bubblestore_order_wishlist_search()

