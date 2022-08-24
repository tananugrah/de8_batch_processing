import os
import json
import psycopg2
import pandas as pd


path = os.getcwd()
with open(path+'/'+'config.json') as file:
    conf = json.load(file)['postgresql']



def connect(conf):
    conn = None
    try:
        conn = psycopg2.connect(host=conf['host'],
                            database=conf['db'],
                            user=conf['user'],
                            password=conf['pwd'])
    
        print(f"[INFO] success connect postgresql ..")
    except:
        print(f"[INFO] can't connect postgresql ..")
    return conn

def create_table():  #create table dim_user pada schema dwh
    create_table_query = '''CREATE TABLE dwh.dim_user( 
            user_id int PRIMARY KEY NOT NULL,
            user_first_name varchar(255) NOT NULL,
            user_last_name varchar(255) NOT NULL,
            user_gender varchar(50) NOT NULL,
            user_address varchar(255),
            user_birthdate date NOT NULL,
            user_join date NOT NULL); '''
    cursor.execute(create_table_query)
    conn.commit()
    print("table dim_user created successfully in postgresql")
    

def read_data():
    #PostgreSQL_select_Query = None
   
    PostgreSQL_select_Query = """SELECT * FROM tb_users"""
    cursor = conn.cursor()
    cursor.execute(PostgreSQL_select_Query)
   
    print("Selecting rows from tb_users using cursor.fetchall")
    tb_users_records = cursor.fetchall() #mengambil semua baris dari kolom 
    # print(tb_users_records[0], '\n')
    #print(tb_users_records)
    conn.commit()

    return tb_users_records


def insert_data(
    records,
    table,
    cols
    ):
    try:
        cursor = conn.cursor()
       
        tuples = records
        # print(type(tuples))
        values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples] #mogrify bulk insert tupple
        # print (values)
        query  = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values) 
        
        cursor.execute(query, tuples)
        conn.commit()
        print(cursor.rowcount, "Record inserted successfully into dim_user table")
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    
    
conn = connect(conf)
create_table_query = create_table()
coloumns = 'user_id,user_first_name,user_last_name,user_gender,user_address,user_birthdate,user_join'
PostgreSQL_select_Query = insert_data(
    read_data(),
    'dwh.dim_user',
    coloumns
)



    
    
        







    