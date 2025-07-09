import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
rate_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
rate_csv = pd.read_csv(rate_url)
table_attribs = ['Name', 'MC_USD_Billion']
csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

def log_progress(message):

    ''' This function logs a given message at a given stage of the code execution to a log file.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 

    with open(log_file, 'a') as log1:

        log1.write(timestamp + ':' + message + '\n')

def extract(url, table_attribs):

    ''' This function extracts a required table
    from a given website and saves it to a dataframe. '''

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[1].find('a') is not None:
                data_dict = {"Name": col[1].contents[2],
                            "MC_USD_Billion": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)

    mc_usd = list(df['MC_USD_Billion'])
    mc_usd = [float("".join(mc.split(','))) for mc in mc_usd]
    mc_usd = [round(mc,2) for mc in mc_usd]
    df['MC_USD_Billion'] = mc_usd

    # print(df)
    return df

def transform(df):

    ''' This function converts the MC information from Currency
    format to float value and transforms the information of MC from
    USD to GBP, EUR, and INR, rounding to 2 decimal places.'''

    mc_usd = list(df['MC_USD_Billion'])

    df['MC_GBP_Billion'] = [round(rate_csv['Rate'][1]*mc,2) for mc in mc_usd]
    df['MC_EUR_Billion'] = [round(rate_csv['Rate'][0]*mc,2) for mc in mc_usd]
    df['MC_INR_Billion'] = [round(rate_csv['Rate'][2]*mc,2) for mc in mc_usd]
    # print(df['MC_EUR_Billion'][4])
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path.'''

    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name.'''

    df.to_sql(table_name, sql_connection, if_exists = 'replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query = pd.read_sql(query_statement, sql_connection)
    print(query)

''' ETL PROCESS '''

log_progress('Preliminaries complete. Initiating ETL process.')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process.')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process.')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file.')

sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL connection initiated')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query.')

''' Verifying the complete output'''

query1 = f"SELECT * FROM Largest_banks"
run_query(query1, sql_connection)

''' Average GBP '''

query2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query2, sql_connection)

''' Top 5 largest banks '''

query3 = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query3, sql_connection)

log_progress('Process Complete.')
sql_connection.close()
