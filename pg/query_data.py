import psycopg2
from config import load_config

def get_vendors(filename: str):
    """ Retrieve data from the vendors table """
    config  = load_config(filename=filename)
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # cur.execute("SELECT vendor_id, vendor_name FROM vendors ORDER BY vendor_name")
                # cur.execute("SELECT * FROM schemaname = maxnerva_exp")
                # cur.execute("SELECT * FROM pg_catalog.pg_tables")
                # cur.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'maxnerva_exp' AND tablename = 'mes_repair_master'")
                cur.execute("SELECT * FROM maxnerva_exp.mes_repair_master")
                print("The number of parts: ", cur.rowcount)
                row = cur.fetchone()

                while row is not None:
                    print(row)
                    row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


import psycopg2
from config import load_config

def iter_row(cursor, size=10):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row

def get_part_vendors(filename: str):
    """ Retrieve data from the vendors table """
    config  = load_config(filename=filename)
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT part_name, vendor_name
                    FROM parts
                    INNER JOIN vendor_parts ON vendor_parts.part_id = parts.part_id
                    INNER JOIN vendors ON vendors.vendor_id = vendor_parts.vendor_id
                    ORDER BY part_name;
                """)
                for row in iter_row(cur, 10):
                    print(row)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# if __name__ == '__main__':
#     get_part_vendors()

if __name__ == '__main__':
    # filename = "C:/Users/YuZhe/OneDrive - Foxconn/Project/tsec/pg/database.ini"
    filename = "C:/Users/YuZhe/OneDrive - Foxconn/Project/tsec/pg/mes.ini"
    get_vendors(filename)        