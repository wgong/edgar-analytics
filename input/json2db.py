
# coding: utf-8

import os
import json
import psycopg2

# config params
file_in = 'test1.json'

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

# len(dic[mapTable2JsonObj['dm_orders']])

i_orders = 0  
# get order fields
order_id = dic[mapTable2JsonObj['dm_orders']][i_orders]['id']

# get line_itesm
# dic[mapTable2JsonObj['dm_orders']][i_orders][mapTable2JsonObj['dm_line_items']]

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
# =============================

# build SQL insert
col_1_list_str = ",".join([f"\"{c[0]}\""  for c in col_1_list])

i_orders = 0
val_list_str = ""
iv = 0
for c in col_1_list:
    col_name, col_type = c[0], c[1]
    val = dic[mapTable2JsonObj['dm_orders']][i_orders][col_name]
    if val is None:
        val = "NULL"
    else:
        if "character" in col_type or "text" in col_type or "timestamp" in col_type:
            val = f"'{val}'"
        elif "boolean" in col_type:
            if val:
                val = "CAST( 1 AS BOOLEAN )"
            else:
                val = "CAST( 0 AS BOOLEAN )"
        else:
            val = f"{val}"

    if iv:
        val_list_str += ',' + val
    else:
        val_list_str += val
        
    iv += 1

sql_1_insert = f"""
    INSERT INTO \"{tbl_1_name}\" ({col_list_str})
    VALUES ({val_list_str});
""" 
# sql_1_insert

cur.execute(sql_1_insert)
connection.commit()  # write to db

# build SQL select
col_list_str = ",".join([f"\"{c[0]}\""  for c in col_1_list])
sql_1_select = f"""
    SELECT {col_list_str} FROM \"{tbl_1_name}\";
""" 
# sql_1_select

cur.execute(sql_1_select)
rows = cur.fetchall()

# ==============================


# build SQL insert
col_2_list_str = ",".join([f"\"{c[0]}\""  for c in col_2_list])

i_line_items = 0
val_list_str = ""
iv = 0
for c in col_2_list:
    col_name, col_type = c[0], c[1]
    if col_name == 'order_id':
        val = order_id
    else:
        val = dic[mapTable2JsonObj['dm_orders']][i_orders][mapTable2JsonObj['dm_line_items']][i_line_items][col_name]
    if val is None:
        val = "NULL"
    else:
        if "character" in col_type or "text" in col_type or "timestamp" in col_type:
            val = f"'{val}'"
        elif "boolean" in col_type:
            if val:
                val = "CAST( 1 AS BOOLEAN )"
            else:
                val = "CAST( 0 AS BOOLEAN )"
        else:
            val = f"{val}"

    if iv:
        val_list_str += ',' + val
    else:
        val_list_str += val
        
    iv += 1

sql_2_insert = f"""
    INSERT INTO \"{tbl_2_name}\" ({col_list_str})
    VALUES ({val_list_str});
""" 
# sql_2_insert

cur.execute(sql_2_insert)
connection.commit()  # write to db

# build SQL select
col_list_str = ",".join([f"\"{c[0]}\""  for c in col_2_list])
sql_2_select = f"""
    SELECT {col_list_str} FROM \"{tbl_2_name}\";
""" 
# sql_2_select

cur.execute(sql_2_select)
rows = cur.fetchall()


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

