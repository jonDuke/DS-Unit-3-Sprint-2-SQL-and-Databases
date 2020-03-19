### rpg_direct_loader.py
#
# loads the rpg data into a MongoDB database from sqlite, using pandas
#
# keeps the same schema; tables become collections, rows become documents

# load the rpg data from the local sqlite database
import sqlite3
PATH = 'C:/Users/thedo/lambda_u3/sqlite/rpg_db/'
sql_conn = sqlite3.connect(PATH + 'rpg_db.sqlite3')

# define which tables we are moving
table_names = ['armory_item', 'armory_weapon',
               'charactercreator_character',
               'charactercreator_character_inventory',
               'charactercreator_cleric',
               'charactercreator_fighter',
               'charactercreator_mage',
               'charactercreator_necromancer',
               'charactercreator_thief']

# load the tables from sqlite to pandas
import pandas as pd
pandas_tables = []  # an array of DataFrames
for table in table_names:
    pandas_tables.append(pd.read_sql(f"SELECT * FROM {table};", sql_conn))

# load environment variables for the MongoDB login
import os
from dotenv import load_dotenv
load_dotenv()
DB_USER = os.getenv("MONGO_USER", default="OOPS")
DB_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME", default="OOPS")

# open a connection to the MongoDB database
import pymongo
connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
client = pymongo.MongoClient(connection_uri)
db = client.rpg_database

# convert dataframes to an array of dicts then add to the database
import numpy
for i in range(len(table_names)):
    # exec() converts the string table_names[i] to a variable name, then runs
    # collection = db.table_name
    exec("collection = db.%s" % table_names[i])
    
    # insert table into the new collection
    collection.insert_many(pandas_tables[i].to_dict(orient='records'))

    print("Inserted", collection.count_documents({}), 
          "rows into collection", collection.name)
