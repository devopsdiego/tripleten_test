import pandas as pd
from sqlalchemy import create_engine
import pymysql

infile = r'files/movie_metadata.csv'
db = 'tripleten_test'
db_tbl_name = 'movie_metadata'


user = 'root'
password = ''
host = 'localhost'
port = '3306'
database = 'tripleten_test'


#Load a csv file into a dataframe; if csv does not have headers, use the headers arg to create a list of headers; rename unnamed columns to conform to mysql column requirements

def csv_to_df(infile, headers = []):
    if len(headers) == 0:
        df = pd.read_csv(infile)
    else:
        df = pd.read_csv(infile, header = None)
        df.columns = headers

    for r in range(10):
        try:
            df.rename( columns={'Unnamed: {0}'.format(r):'Unnamed{0}'.format(r)},    inplace=True )
        except:
            pass
    return df

'''
Create a mapping of df dtypes to mysql data types (not perfect, but close enough)
'''
def dtype_mapping():
    return {'object' : 'TEXT',
        'int64' : 'INT',
        'float64' : 'FLOAT',
        'datetime64' : 'DATETIME',
        'bool' : 'TINYINT',
        'category' : 'TEXT',
        'timedelta[ns]' : 'TEXT'}

'''
Create a sqlalchemy engine
'''
#def mysql_engine(user, password, host, port, database):
  #  engine = 'mysql+pymysql://' + user + ':' + password + '@' + host + ':' + port + '/' + database
 #   return engine

#db_connection = create_engine(mysql_engine(user, password, host, port, database))

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

'''
Create sql input for table names and types
'''
def gen_tbl_cols_sql(df):
    dmap = dtype_mapping()
    sql = "id INT AUTO_INCREMENT PRIMARY KEY"
    df1 = df.rename(columns = {"" : "nocolname"})
    hdrs = df1.dtypes.index
    hdrs_list = [(hdr, str(df1[hdr].dtype)) for hdr in hdrs]
    for hl in hdrs_list:
        sql += " ,{0} {1}".format(hl[0], dmap[hl[1]])
    return sql


'''
Create a mysql table from a df
'''
def create_mysql_tbl_schema(df, conn, tbl_name):
    tbl_cols_sql = gen_tbl_cols_sql(df)
    sql = "CREATE TABLE {0} ({1})".format(tbl_name, tbl_cols_sql)
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.commit()

'''
Write df data to newly create mysql table
'''
def df_to_mysql(df, engine, tbl_name):
    df.to_sql(tbl_name, engine, if_exists='replace')
    
df = csv_to_df(infile)
create_mysql_tbl_schema(df, mysql_conn(mysql_engine()), db_tbl_name)
df_to_mysql(df, mysql_engine(), db_tbl_name)