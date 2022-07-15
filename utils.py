import json
from configparser import ConfigParser
from sqlalchemy import create_engine
import pandas as pd

def read_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

def write_json(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def list_to_dataframe(list):
    df = pd.DataFrame(list)
    return df

def read_mysql_config(path, section):
    config = ConfigParser()
    config.read(path)

    username = config.get(section, 'username')
    password = config.get(section, 'password')
    ip = config.get(section, 'host')
    database_name = config.get(section, 'database_name')

    return username, password, ip, database_name

def connect_mysql_database(path, section):
    username, password, host, database_name = read_mysql_config(path, section)

    database_connection = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(username, password, 
                                                          host, database_name))
    
    return database_connection

def import_dataframe_to_mysql(path, section, dataframe, table_name, if_exist):
    database_connection = connect_mysql_database(path, section)
    print('Uploading {} to database'.format(table_name))
    dataframe.to_sql(con=database_connection, name=table_name, if_exists=if_exist, index=False)
    print('{} upload complete'.format(table_name), '\n')