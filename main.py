import pandas as pd
from sqlalchemy import create_engine

user = 'root'
password = ''
host = 'localhost'
port = '3306'
database = 'tripleten_test'

'''
Create a sqlalchemy engine
'''
def mysql_engine(user = 'root', password = '', host = 'localhost', port = '3306', database = 'tripleten_test'):
    engine = create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))
    return engine


'''
Create a mysql connection from sqlalchemy engine
'''
def mysql_conn(engine):
    conn = engine.raw_connection()
    return conn

def set_avg_movie_duration(conn):
    sql = "UPDATE "\
        "movie_metadata "\
        "SET duration = (SELECT FORMAT(AVG(duration), 0) FROM movie_metadata WHERE duration IS NOT NULL) "\
        "WHERE duration IS NULL;"

    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.commit()

    return sql

re = set_avg_movie_duration(mysql_conn(mysql_engine()))