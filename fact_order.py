import os
import json
import psycopg2


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

def create_table():  #create table fact_order pada schema dwh
    cursor = conn.cursor()
    create_table_query = '''CREATE TABLE dwh.fact_order(
            order_id INT PRIMARY KEY NOT NULL,
            user_id INT NOT NULL,
            order_date DATE NOT NULL,
            payment_id INT NOT NULL,
            shipper_id INT NOT NULL,
            order_price INT NOT NULL,
            order_discount INT,
            voucher_id INT ,
            order_total INT NOT NULL,
            rating_id INT NOT NULL); '''
    cursor.execute(create_table_query)
    conn.commit()
    print("table created successfully in postgresql")
    return create_table_query

def read_data():
    
    PostgreSQL_select_Query = """SELECT * FROM tb_orders"""
    cursor = conn.cursor()
    cursor.execute(PostgreSQL_select_Query)
   
    print("Selecting rows from tb_orders using cursor.fetchall")
    tb_users_records = cursor.fetchall() #mengambil semua baris dari kolom 
    # print(tb_users_records[0], '\n')
    print(tb_users_records)
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
        values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples] #mogrify bulk insert tupple
        # print (values)
        query  = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values) 
        cursor.execute(query, tuples)
        conn.commit()
        print(cursor.rowcount, "Record inserted successfully into fact_order table")
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    
    
conn = connect(conf)
#create_table_query = create_table()
coloumns = 'order_id,order_date,user_id,payment_id,shipper_id,order_price,order_discount,voucher_id,order_total,rating_id'
PostgreSQL_select_Query = insert_data(
    read_data(),
    'dwh.fact_order',
    coloumns
)



    
    
        







    