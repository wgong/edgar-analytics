
# coding: utf-8

# In[9]:


# coding: utf-8

import os
import json
import psycopg2
from psycopg2.extras import execute_values

BATCH_SIZE = 200     # insert in batch

FLAG_CLEANUP = False  # delete rows for next load
FLAG_VERIFY = False  # read back for verification

# config params
file_in = '2017-11-10.json'
#file_in = 'test1.json'

db_host = os.environ.get('AWS_PG_DB_HOST')
db_name = os.environ.get('AWS_PG_DB_NAME')
db_user = os.environ.get('AWS_PG_DB_USER')
password = os.environ.get('AWS_PG_DB_PASS')
schema_name = 'public'

# table to Json obj
mapTable2JsonObj = {'dm_orders':'orders', 'dm_line_items':'line_items'}

with open(file_in) as f:
    s = f.read()

dic = json.loads(s)

# connect to PostgreSQL
db_connection_string = f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{password}'"
connection = psycopg2.connect(db_connection_string)
cur = connection.cursor()

tbl_1_name = 'dm_orders'
tbl_2_name = 'dm_line_items'

# get column info
sql_1_coldef = f"""
    SELECT sc.table_name, sc.column_name, sc.data_type
    FROM information_schema.columns sc
    WHERE table_schema = '{schema_name}'
      AND table_name = '{tbl_1_name}'
      order by table_name,ordinal_position;
"""

cur.execute(sql_1_coldef)
col_1_defs = cur.fetchall()
col_1_list = [(col[1],col[2]) for col in col_1_defs]

#col_1_list

# get column info
sql_2_coldef = f"""
    SELECT sc.table_name, sc.column_name, sc.data_type
    FROM information_schema.columns sc
    WHERE table_schema = '{schema_name}'
      AND table_name = '{tbl_2_name}'
      order by table_name,ordinal_position;
"""

cur.execute(sql_2_coldef)
col_2_defs = cur.fetchall()
col_2_list = [(col[1],col[2]) for col in col_2_defs]

# col_2_list

# =============================

# build SQL insert
col_1_list_str = ",".join([f"\"{c[0]}\""  for c in col_1_list])
col_2_list_str = ",".join([f"\"{c[0]}\""  for c in col_2_list])

sql_1_insert = f"""
    INSERT INTO \"{tbl_1_name}\" ({col_1_list_str})
    VALUES %s;
""" 

sql_2_insert = f"""
    INSERT INTO \"{tbl_2_name}\" ({col_2_list_str})
    VALUES %s;
""" 

orders = dic[mapTable2JsonObj['dm_orders']]

values_orders, values_line_items = [], []

# process orders
for i_orders in range(len(orders)):
    
    order = orders[i_orders]
    order_id = order['id']
    val_1_list = []
    for c in col_1_list:
        val_1_list.append(order[c[0]])

    values_orders.append(tuple(val_1_list))

    # process line_items
    order_line_items = order[mapTable2JsonObj['dm_line_items']]

    # build SQL insert
    i_line_items = 0
    for i_line_items in range(len(order_line_items)):
        val_2_list = []
        for c in col_2_list:
            col_name, col_type = c[0], c[1]
            if col_name == 'order_id':
                val = order_id
            else:
                val = order_line_items[i_line_items][col_name]

            val_2_list.append(val)
        
        values_line_items.append(tuple(val_2_list))

    # write to DB
    if i_orders > 0 and i_orders % BATCH_SIZE == 0:
        execute_values(cur, sql_1_insert, values_orders)
        execute_values(cur, sql_2_insert, values_line_items)
        connection.commit()  # write to db
        values_orders, values_line_items = [], []

# final flush
if values_orders:
    execute_values(cur, sql_1_insert, values_orders)
    connection.commit()  # write to db
if values_line_items:
    execute_values(cur, sql_2_insert, values_line_items)
    connection.commit()  # write to db


if FLAG_VERIFY:
    # build SQL select
    col_list_str = ",".join([f"\"{c[0]}\""  for c in col_1_list])
    sql_1_select = f"""
        SELECT {col_list_str} FROM \"{tbl_1_name}\";
    """ 
    sql_1_select

    cur.execute(sql_1_select)
    rows = cur.fetchall()

    # build SQL select
    col_list_str = ",".join([f"\"{c[0]}\""  for c in col_2_list])
    sql_2_select = f"""
        SELECT {col_list_str} FROM \"{tbl_2_name}\";
    """ 

    cur.execute(sql_2_select)
    rows = cur.fetchall()

if FLAG_CLEANUP:
    # cleanup
    sql_1_delete = f"""
        DELETE FROM \"{tbl_1_name}\";
    """ 
    cur.execute(sql_1_delete)
    connection.commit()

    sql_2_delete = f"""
        DELETE FROM \"{tbl_2_name}\";
    """ 
    cur.execute(sql_2_delete)
    connection.commit()

# done with DB
connection.close()

