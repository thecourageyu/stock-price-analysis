#%%
from typing import List, Optional, Union
import psycopg2

from .config import load_config

#%%
import os
import pandas as pd


# ex.
# INSERT INTO tse (日期, 證券代號, 證券名稱, 成交股數, 成交筆數, 成交金額)
# VALUES
# (2024-04-09, 0050, 元大台灣50, 9043462, 15355, 1465418271),
# (2024-04-09, 0051, 元大中型100, 76132, 221, 6040534),
INSERT_TEMPLATE = "INSERT INTO {table_name} {columns}\nVALUES\n{values}"

def build_insert_cmd(tse_data: pd.DataFrame, table_name: Optional[str] = "listed_daily"):
    columns = "("
    for col in tse_data.columns.to_list():
        columns += f"{col}, "
    columns = columns[:-2] + ")"

    values = ""
    for row in tse_data.itertuples():
        tuple_string = "("
        for v in row[1:]:
            tuple_string += f"'{v}', "
        tuple_string = tuple_string[:-2] + "),\n"
        values += tuple_string
    values = values[:-2]
    return INSERT_TEMPLATE.format(table_name=table_name, columns=columns, values=values)

#%%

def insert_data(filename: Union[str, pd.DataFrame], tablename: str, config: str):
    """ Insert a new vendor into the vendors table """

    # sql = """INSERT INTO vendors(vendor_name)
    #          VALUES(%s) RETURNING vendor_id;"""
    if isinstance(filename, str):
        input_data = pd.read_csv(filename)

    sql = build_insert_cmd(input_data, table_name=tablename)
    
    # vendor_id = None
    config = load_config(filename=config)

    try:
        with  psycopg2.connect(**config) as conn:
            with  conn.cursor() as cur:
                # execute the INSERT statement
                # cur.execute(sql, (vendor_name,))
                cur.execute(sql)
                # get the generated id back                
                # rows = cur.fetchone()
                # if rows:
                #     vendor_id = rows[0]

                # commit the changes to the database
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("*** Error-insert-data:\n{}".format(error))
    finally:
        return sql


#%%
def insert_vendor(vendor_name, filename: str):
    """ Insert a new vendor into the vendors table """

    sql = """INSERT INTO vendors(vendor_name)
             VALUES(%s) RETURNING vendor_id;"""
    
    vendor_id = None
    config = load_config(filename=filename)

    try:
        with  psycopg2.connect(**config) as conn:
            with  conn.cursor() as cur:
                # execute the INSERT statement
                cur.execute(sql, (vendor_name,))
                
                # get the generated id back                
                rows = cur.fetchone()
                if rows:
                    vendor_id = rows[0]

                # commit the changes to the database
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("*** Error-insert-vender:\n{}".format(error))
    finally:
        return vendor_id
    

def insert_many_vendors(vendor_list, filename: str):
    """ Insert multiple vendors into the vendors table  """

    sql = "INSERT INTO vendors(vendor_name) VALUES(%s) RETURNING *"
    config = load_config(filename=filename)
    print(config)
    rows = []
    print(vendor_list)
    try:
        with  psycopg2.connect(**config) as conn:
            with  conn.cursor() as cur:
                # execute the INSERT statement
                cur.executemany(sql, vendor_list)
                print(">>>>>>>>>>>>")
                # obtain the inserted rows
                # rows = cur.fetchall()
                
                print(rows)
                # commit the changes to the database
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        # print(error)    
        print("*** Error-insert-vender:\n{}".format(error))
    finally:
        return rows


def insert(vendor_list: List, filename: str):
    """ Insert multiple vendors into the vendors table  """

    sql = "INSERT INTO vendors(vendor_id, vendor_name) VALUES(%s, %s) RETURNING *"
    config = load_config(filename=filename)
    print(config)
    rows = []
    print(vendor_list)
    try:
        with  psycopg2.connect(**config) as conn:
            with  conn.cursor() as cur:
                # execute the INSERT statement
                cur.executemany(sql, vendor_list)
                print(">>>>>>>>>>>>")
                # obtain the inserted rows
                # rows = cur.fetchall()
                
                print(rows)
                # commit the changes to the database
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        # print(error)    
        print("*** Error-insert-vender:\n{}".format(error))
    finally:
        return rows


#%%
if __name__ == '__main__':

    import sys
    input_dir = "C:/Users/YuZhe/OneDrive - Foxconn/Project/tsec/data/tse"
    # tse_data = pd.read_csv(os.path.join(input_dir, "tse_20240409.csv"))

    filename = "C:/Users/YuZhe/OneDrive - Foxconn/Project/tsec/pg/database.ini"
    # insert_vendor("3M Co.", filename=filename)
    sql = insert_data(filename=os.path.join(input_dir, "tse_20240409.csv"), tablename="listed_daily", config=filename)
    sys.exit()
    # insert_many_vendors([
    #     ('AKM Semiconductor Inc.',),
    #     ('Asahi Glass Co Ltd.',),
    #     ('Daikin Industries Ltd.',),
    #     ('Dynacast International Inc.',),
    #     ('Foster Electric Co. Ltd.',),
    #     ('Murata Manufacturing Co. Ltd.',)
    # ], 
    # filename=filename)


    insert([
        ('1', '3M Co.',),
        ('2', 'AKM Semiconductor Inc.',),
        ('3', 'Asahi Glass Co Ltd.',),
        ('4', 'Daikin Industries Ltd.',),
        ('5', 'Dynacast International Inc.',),
        ('6', 'Foster Electric Co. Ltd.',),
        ('7', 'Murata Manufacturing Co. Ltd.',)
    ], 
    filename=filename)
# %%
