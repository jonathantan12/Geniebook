from mysql.connector import connect, Error
import json
import string
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
import pandas

start = int(input('Enter Start File Number: '))
end = int(input('Enter End File Number: '))

def insertData(data):
    """
    1. EXTRACT DATA FROM JSON
    2. CONVERT INTO PANDAS DATA FRAME
    3. UPLOAD ONTO MYSQL
    """

    dictionary = {"class_name" : [], "class_id" : [], "message_id" : [], "date_time" : [], "user_name" : [], "user_id" : [], "user_text" : [], "media_type" : []}

    class_name = data['name']
    class_id = data['id']

    #messages within the groupchat
    messages = data['messages']
    
    for message in messages:
        #IF NOT A MESSAGE BUT AN USER ACTION
        if 'actor' in message:
            message_id = message['id']
            date = message['date'][:10]
            time = message['date'][11:]
            date_time = date +" " + time

            user_name = ''
            if message['actor'] != None: 
                for letter in message['actor']:
                    if str(letter) != "," and (str(letter).isalnum() or str(letter) == ' ' or str(letter) in string.punctuation):
                        user_name += str(letter)
                    else:
                        user_name += ''
            
            user_id = message['actor_id']
            # COULD POSSIBLY BE PIN_MESSAGE, ADD/REMOVE MEMBERS ETC.
            media_type = message['action']
            user_text = ""

        #TO CHECK THE MEDIA TYPE IF THERE IS MEDIA TYPE. FOR STICKERS ETC.
        elif 'media_type' in message:
            media_type = message['media_type']
            message_id = message['id']
            date = message['date'][:10]
            time = message['date'][11:]
            date_time = date +" " + time
            user_id = message['from_id']
            user_name = ''

            if message['from'] != None: 
                for letter in message['from']:
                    if str(letter) != "," and (str(letter).isalnum() or str(letter) == ' ' or str(letter) in string.punctuation):
                        user_name += str(letter)
                    else:
                        user_name += ''

            user_text = ''
            if message['text'] != None:
                for letter in message['text']:
                    if str(letter) != ',' and (str(letter).isalnum() or str(letter) == ' ' or str(letter) in string.punctuation):
                        user_text += str(letter)
                    else:
                        user_text += ''

        #FOR TEXT
        else:
            message_id = message['id']
            date = message['date'][:10]
            time = message['date'][11:]
            date_time = date + ' ' + time
            user_id = message['from_id']
            media_type = "text"

            user_name = ''
            if message['from'] != None: 
                for letter in message['from']:
                    if str(letter) != "," and (str(letter).isalnum() or str(letter) == ' ' or str(letter) in string.punctuation):
                        user_name += str(letter)
                    else:
                        user_name += ''

            user_text = ''
            if message['text'] != None:
                for letter in message['text']:
                    if str(letter) != "," and (str(letter).isalnum() or str(letter) == ' ' or str(letter) in string.punctuation):
                        user_text += str(letter)
                    else:
                        user_text += ''

            #IF PHOTO EXISTS IN THE MESSAGE
            if 'photo' in message:
                media_type = "photo"

            
        #DUMPING INTO NEW JSON FILE
        dictionary["class_name"].append(class_name)
        dictionary["class_id"].append(class_id)
        dictionary["message_id"].append(message_id)
        dictionary['date_time'].append(date_time)
        dictionary["user_name"].append(user_name)
        dictionary["user_id"].append(user_id)
        dictionary["user_text"].append(user_text)
        dictionary["media_type"].append(media_type)

    df = pd.DataFrame(data=dictionary)
    print(df)

    table_name = 't_genieask'

    engine = create_engine('mysql+pymysql://product.support:T7FgHDbq6apgmGKSFVmDsVRq4DJW5kar@10.148.15.210/warehouse', pool_recycle=3600)
    dbConnection = engine.connect()
    df.to_sql(name = table_name, con = dbConnection, schema=None, if_exists='append', index=None, index_label=None, chunksize=10000, dtype=None, method=None)

    dbConnection.close()

def createDatabase(start, end):
    """
    LOOP THROUGH A RANGE OF FILES
    """
    for i in range(start, end+1):
        print(i)
        with open(f"../Telegram Data asof 24 June/ChatExport_2021-06-25 ({i})/result.json", encoding="utf8") as f:
            data = json.load(f)
            # print(data)

        insertData(data) # FUNCTION THAT IS CALLED TO EXTRACT AND IMPORT DATA

createDatabase(start, end)