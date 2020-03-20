### rpg_queries.py
#
# runs exploratory queries on the MongoDB rpg database,
# answers questions from module 1


### ----- Connect to the database ------
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


### ----- Questions -----
# Using the original schema copied from sqlite
print("----- Using the old sqlite schema -----")
db = client.rpg_database

# How many total characters are there?
print("How many total characters are there?")
count = db.charactercreator_character.count_documents({})
print(count, 'characters')

# How many of each specific subclass?
print('\nHow many of each specific subclass?')
print(db.charactercreator_cleric.count_documents({}), 'clerics')
print(db.charactercreator_fighter.count_documents({}), 'fighters')
print(db.charactercreator_mage.count_documents({}), 'mages')
print(db.charactercreator_necromancer.count_documents({}), 'necromancers')
print(db.charactercreator_thief.count_documents({}), 'thieves')

# How many total items?
print('\nHow many total items?')
item_count = db.armory_item.count_documents({})
print(item_count, 'items')

# How many of the items are weapons?  How many are not?
print('\nHow many of the items are weapons?  How many are not?')
weapon_count = db.armory_weapon.count_documents({})
print(weapon_count, 'are weapons')
print(item_count - weapon_count, 'are not weapons')

# The last four questions required aggregates, 
# and to be honest I could not get them working

# # How many items does each character have? (1st 20 rows)
# print('\nHow many items does each character have? (1st 20 rows)')
# pipeline = [
#     {"$group": {"_id": "$Character_id", "count": {"$sum": 1}}}
# ]
# for line in list(db.charactercreator_inventory.aggregate(pipeline)):
#     print(line)

# # How many weapons does each character have? (1st 20 rows)
# # On average, how many items does each character have?
# # On average, how many weapons does each character have?



### ----- Questions -----
# Using the schema I modified yesterday (only 2 collections)
print('\n\n----- Now using the new schema -----')
db = client.rpg_database_2

# How many total characters are there?
print("How many total characters are there?")
count = db.characters.count_documents({})
print(count, 'characters')

# How many of each specific subclass?
print("\nHow many of each specific class?")
classes = db.characters.distinct('class') #get all distinct classes
for c in classes:
    # count each distinct class
    count = db.characters.count_documents({"class":c})
    print(c + 's:', count)

print("\nHow many of each specific subclass?")
classes = db.characters.distinct('subclass') #get all distinct classes
for c in classes:
    # count each distinct subclass
    count = db.characters.count_documents({"subclass":c})
    # pick the first character with this subclass and get its base class
    base_class = db.characters.find({"subclass":c}, limit=1)[0]['class']
    print(f'{c}s: {count} (base class: {base_class})')

# How many total items?
print('\nHow many total items?')
count = db.items.count_documents({})
print(count, 'items')

# How many of the items are weapons?  How many are not?
print('\nHow many of the items are weapons?  How many are not?')
count = db.items.count_documents({"type":"weapon"})
print(count, 'weapons')
count = db.items.count_documents({"type":{"$ne":"weapon"}})
print(count, 'are not weapons')

# The last 4 questions cannot be answered here because I did not
# migrate inventory data in the new schema 
# (I wasn't sure how to store a many to many relationship in MongoDB)

# How many weapons does each character have? (1st 20 rows)
# How many weapons does each character have? (1st 20 rows)
# On average, how many items does each character have?
# On average, how many weapons does each character have?

# close the connection once we're done
client.close()
