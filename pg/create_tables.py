from typing import Dict, Optional

import psycopg2
from config import load_config


CREATE_TABLE_TEMPLATE = """
CREATE TABLE {table_name} (
{columns}
)
"""

tables = {
    "listed_daily": {
        "日期":	"DATE NOT NULL",
        "證券代號":	"VARCHAR(32) NOT NULL",
        "證券名稱":	"VARCHAR(128) NOT NULL",
        "成交股數":	"INTEGER",
        "成交筆數": "INTEGER",
        "成交金額": "REAL",  # float32, single?
        "開盤價": "NUMERIC(8, 4)", 
        "最高價": "NUMERIC(8, 4)", 
        "最低價": "NUMERIC(8, 4)",
        "收盤價": "NUMERIC(8, 4)",
        "最後揭示買價":	"NUMERIC(8, 4)",
        "最後揭示買量": "INTEGER",
        "最後揭示賣價": "NUMERIC(8, 4)", 
        "最後揭示賣量": "INTEGER",
        "本益比": "NUMERIC(8, 4)",  
        "漲跌價差": "NUMERIC(8, 4)", 
    }
}

def create_tables(tables: Dict, filename: Optional[str] ='database.ini'):
    """ Create tables in the PostgreSQL database"""

    commands = []
    for table_name, columns in tables.items():
        s = ""
        for colname, datatype in columns.items():
            s += f"    {colname} {datatype},\n"
        s = s[:-2]

        commands.append(CREATE_TABLE_TEMPLATE.format(table_name=table_name, columns=s))
    

    try:
        config = load_config(filename=filename)
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                for command in commands:
                    print(command)
                    cur.execute(command)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def alter_column_type(table_name: str, col_name: str, data_type: str, config: Optional[str] ='database.ini'):
    cmd = f"ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE {data_type};"
    try:
        config = load_config(filename=config)
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                # for command in commands:
                cur.execute(cmd)
    except (psycopg2.DatabaseError, Exception) as error:
        print(f"config: {config}\ncmd: {cmd}\nerror: {error}")
    return {"config": config, "cmd": cmd}

def _create_tables(filename='database.ini'):
    """ Create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE vendors (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
        """,
        """ CREATE TABLE parts (
                part_id SERIAL PRIMARY KEY,
                part_name VARCHAR(255) NOT NULL
                )
        """,
        """
        CREATE TABLE part_drawings (
                part_id INTEGER PRIMARY KEY,
                file_extension VARCHAR(5) NOT NULL,
                drawing_data BYTEA NOT NULL,
                FOREIGN KEY (part_id)
                REFERENCES parts (part_id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE vendor_parts (
                vendor_id INTEGER NOT NULL,
                part_id INTEGER NOT NULL,
                PRIMARY KEY (vendor_id , part_id),
                FOREIGN KEY (vendor_id)
                    REFERENCES vendors (vendor_id)
                    ON UPDATE CASCADE ON DELETE CASCADE,
                FOREIGN KEY (part_id)
                    REFERENCES parts (part_id)
                    ON UPDATE CASCADE ON DELETE CASCADE
        )
        """)
    try:
        config = load_config(filename=filename)
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                for command in commands:
                    cur.execute(command)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    #create_tables(tables, filename="C:/Users/YuZhe/OneDrive - Foxconn/Project/tsec/pg/database.ini")
    
    change_columns = {
        "開盤價": "NUMERIC(8, 4)", 
        "最高價": "NUMERIC(8, 4)", 
        "最低價": "NUMERIC(8, 4)",
        "收盤價": "NUMERIC(8, 4)",
        "最後揭示買價": "NUMERIC(8, 4)",
        "最後揭示賣價": "NUMERIC(8, 4)",
        "本益比": "REAL",
        "漲跌價差": "REAL"
    }
    for col_name, data_type in change_columns.items():
        sql = alter_column_type("listed_daily", col_name, data_type, config="C:/Users/YuZhe/OneDrive - Foxconn/Project/tsec/pg/database.ini")
        print(sql)