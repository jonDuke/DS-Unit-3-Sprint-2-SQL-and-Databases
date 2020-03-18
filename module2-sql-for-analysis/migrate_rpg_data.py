### migrates data from the sqlite rpg database to postgresql

### ----- load RPG data into pandas -----
import sqlite3
import pandas as pd

# open sqlite connection
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

# get the tables from sqlite to pandas
pandas_tables = []  # an array of DataFrames
for table in table_names:
    pandas_tables.append(pd.read_sql(f"SELECT * FROM {table};", sql_conn))


### ----- copy the tables from Pandas to PostGreSQL -----

# load environment variables
import os
from dotenv import load_dotenv
load_dotenv()
DB_URL=os.getenv('DB_URL')

# pandas supports converting directly from a Dataframe to PostGreSQL
# uses sqlalchemy instead of psycopg2
from sqlalchemy import create_engine
engine = create_engine(DB_URL)  # uses the single URL postgre login

# convert all tables
for i in range(len(table_names)):
    pandas_tables[i].to_sql(table_names[i], engine, 
                            if_exists='replace', method='multi')
