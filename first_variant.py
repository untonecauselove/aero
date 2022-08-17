import psycopg2
import urllib.request, json
import datetime
from crds import DB, USERNAME, PASSWORD

LINK = "https://random-data-api.com/api/cannabis/random_cannabis?size=10"
table = 'cannabis_types'

connection = psycopg2.connect(
    host="localhost",
    database=DB,
    user=USERNAME,
    password=PASSWORD)

def upload_data():
    with urllib.request.urlopen(LINK) as url:
        data = json.loads(url.read().decode())
    
    for row in data:

        listkeys = list(row.keys()) #getting schema in case it not constant
        all_keys = ''
        for k in listkeys:
            all_keys = all_keys + k + ','
        all_keys = all_keys + 'upload_at' 

        values = ''
        for x in row.keys():

            values = values + "'" + str(row.get(x)) + "'" + ','
        values = values + "'" + str(datetime.datetime.now()) + "'"

        with connection as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO %s (%s) VALUES (%s)" %(table, all_keys, values)) # TODO: fix problems with "'" in query
    return
 
upload_data()