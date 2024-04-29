import psycopg2
from config import load_config


def query(filename: str, tabel_name: str = "parts"):
    sql = 'SELECT * FROM {}'.format(tabel_name)
    config = load_config(filename=filename)
    results = None
    try:
        with  psycopg2.connect(**config) as conn:
            with  conn.cursor() as cur:
                # execute the UPDATE statement
                cur.execute(sql)
                
                results = cur.fetchall()
            # commit the changes to the database
            # conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)    
    finally:
        return results

def delete_part(filename: str, tabel_name: str = "parts", column_name: str = "part_id", row_id: str = 1):
    """ Delete part by part id """

    rows_deleted  = 0
    # sql = 'DELETE FROM parts WHERE part_id = %s'
    sql = 'DELETE FROM {} WHERE {} = %s'.format(tabel_name, column_name)
    
    config = load_config(filename=filename)

    try:
        with  psycopg2.connect(**config) as conn:
            with  conn.cursor() as cur:
                # execute the UPDATE statement
                cur.execute(sql, (row_id,))
                rows_deleted = cur.rowcount

            # commit the changes to the database
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)    
    finally:
        return rows_deleted

if __name__ == '__main__':
    # deleted_rows = delete_part(2)
    filename = "C:/Users/YuZhe/OneDrive - Foxconn/Project/tsec/pg/database.ini"
    results = query(filename, "vendors")
    for result in results:
        print(result, type(result))
        deleted_rows = delete_part(filename, "vendors", "vendor_id", result[0])
        print('The number of deleted rows: ', deleted_rows)