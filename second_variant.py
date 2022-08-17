import psycopg2.extras as extras
import psycopg2
import urllib.request, json
import datetime
import pandas as pd
from crds import DB, USERNAME, PASSWORD

LINK = "https://random-data-api.com/api/cannabis/random_cannabis?size=10"
table = 'cannabis_types'

connection = psycopg2.connect(
    host="localhost",
    database=DB,
    user=USERNAME,
    password=PASSWORD)

def write_df(df, table):

   with connection as conn:
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        with conn.cursor() as cursor:
            extras.execute_values(cursor, query, tuples)

def upload_data():
    with urllib.request.urlopen(LINK) as url:
        data = json.loads(url.read().decode())
    
    listkeys = list(data[0].keys())
    listkeys.append('upload_at')

    df = pd.DataFrame([], columns=listkeys)
    q = 0

    for row in data:
        listvalues = list(row.values())
        listvalues.append(datetime.datetime.now())
        
        df.loc[q] = listvalues
        q += 1

    write_df(df, table)    
    return    
 
upload_data()