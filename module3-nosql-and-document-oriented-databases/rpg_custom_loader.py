### rpg_custom_loader.py
#
# loads the rpg data into a MongoDB database from sqlite
#
# changes schema to make more sense for a non-relational database

### ----- open database connections -----
# open a connection to the local sqlite database
import sqlite3
PATH = 'C:/Users/thedo/lambda_u3/sqlite/rpg_db/'
sql_conn = sqlite3.connect(PATH + 'rpg_db.sqlite3')
sql_conn.row_factory = sqlite3.Row
cursor = sql_conn.cursor()

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
mongo_db = client.rpg_database_2


### ----- migrate the armory_item and armory_weapon -----

# this query adds a 'type' field, values 'item' or 'weapon'
query = """
SELECT
	name,
	CASE WHEN armory_weapon.item_ptr_id IS NOT NULL 
		THEN 'weapon' ELSE 'item' END AS type,
	value,
	weight,
	power
FROM armory_item
LEFT JOIN armory_weapon ON armory_item.item_id = armory_weapon.item_ptr_id
"""
result = cursor.execute(query).fetchall()

# use pandas for the convenient to_dict() function
import pandas as pd
df = pd.read_sql(query, sql_conn)
data = df.to_dict(orient='records')

# remove any null values from the rows (nan == nan results in false)
# in this case, power is null if the item is not a weapon
for i in range(len(data)):
    data[i] = {k: v for k, v in data[i].items() if v == v}

# insert items into the DB
mongo_db.items.insert_many(data)


### ----- migrate the characters -----

# It is far easier to migrate the classes individually than to have one 
# monster query migrate all of them at once (from 6 different tables)

# cleric query
query = """
SELECT
	name,
	'cleric' as class,
	level,
	exp,
	hp,
	strength,
	intelligence,
	dexterity,
	wisdom,
    mana,
	using_shield
FROM charactercreator_cleric cl
LEFT JOIN charactercreator_character ch ON cl.character_ptr_id = ch.character_id
"""
# use pandas to convert query results to the correct format for MongoDB insert
df = pd.read_sql(query, sql_conn)
mongo_db.characters.insert_many(df.to_dict(orient='records'))

# fighter query
query = """
SELECT
	name,
	'fighter' as class,
	level,
	exp,
	hp,
	strength,
	intelligence,
	dexterity,
	wisdom,
    rage,
	using_shield
FROM charactercreator_fighter fi
LEFT JOIN charactercreator_character ch ON fi.character_ptr_id = ch.character_id
"""
df = pd.read_sql(query, sql_conn)
mongo_db.characters.insert_many(df.to_dict(orient='records'))

# thief query
query = """
SELECT
	name,
	'thief' as class,
	level,
	exp,
	hp,
	strength,
	intelligence,
	dexterity,
	wisdom,
    energy,
	is_sneaking
FROM charactercreator_thief th
LEFT JOIN charactercreator_character ch ON th.character_ptr_id = ch.character_id
"""
df = pd.read_sql(query, sql_conn)
mongo_db.characters.insert_many(df.to_dict(orient='records'))

# mage/necromancer query
# adds a subclass field if the character is a necromancer
query = """
SELECT
	name,
	'mage' AS class,
	CASE WHEN ne.mage_ptr_id IS NOT NULL 
		THEN 'necromancer' END AS subclass,
	level,
	exp,
	hp,
	strength,
	intelligence,
	dexterity,
	wisdom,
	mana,
	has_pet,
	talisman_charged
FROM charactercreator_mage ma
LEFT JOIN charactercreator_necromancer ne ON ma.character_ptr_id = ne.mage_ptr_id
LEFT JOIN charactercreator_character ch ON ma.character_ptr_id = ch.character_id
"""
df = pd.read_sql(query, sql_conn)
data = df.to_dict(orient='records')

# remove any null values from the rows (nan == nan results in false)
# in this case, subclass and talisman_charged are null for non-necromancers
for i in range(len(data)):
    # remove numpy.nan types (numeric fields)
    data[i] = {k: v for k, v in data[i].items() if v == v}
    # remove NoneType (string fields)
    data[i] = {k: v for k, v in data[i].items() if v is not None}
    # I don't know why I couldn't get those both to work in the same line

mongo_db.characters.insert_many(data)
