# rpg_queries.py
#
# a collection of sql queries to run on the database

# set up the database connection
import sqlite3
PATH = ''  # blank path since my db is in the same directory
conn = sqlite3.connect(PATH + 'rpg_db.sqlite3')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Helper function
def get_count(query):
    """ returns a single number result from query """
    result = cursor.execute(query).fetchall()
    return result[0][0]


# All functions will print the answer to the question

# How many total characters are there?
def count_characters():
    query = """SELECT count(DISTINCT character_id) as character_count
            FROM charactercreator_character"""
    print('There are', get_count(query), 'characters\n')

# How many of each specific subclass?
def count_subclasses():
    # this database has separate tables for each class
    query = """SELECT COUNT(DISTINCT character_ptr_id) as cleric_count
            FROM charactercreator_cleric"""
    print('There are', get_count(query), 'clerics')

    query = """SELECT COUNT(DISTINCT character_ptr_id) as fighter_count
            FROM charactercreator_fighter"""
    print('There are', get_count(query), 'fighters')

    query = """SELECT COUNT(DISTINCT character_ptr_id) as mage_count
            FROM charactercreator_mage"""
    print('There are', get_count(query), 'mages')

    query = """SELECT COUNT(DISTINCT character_ptr_id) as thief_count
            FROM charactercreator_thief"""
    print('There are', get_count(query), 'thieves')

    # necromancers use a different id key
    # the key implies they are a subset of mages
    query = """SELECT COUNT(DISTINCT mage_ptr_id) as necromancer_count
            FROM charactercreator_necromancer"""
    print('There are', get_count(query), 'necromancers\n')

def count_subclasses_combined():
    # same as above, but with just one query
    query = """SELECT "cleric" as class_name, COUNT(DISTINCT character_ptr_id) as class_count
            FROM charactercreator_cleric
            UNION
            SELECT "fighter" as class_name, COUNT(DISTINCT character_ptr_id) as class_count
            FROM charactercreator_fighter
            UNION
            SELECT "mage" as class_name, COUNT(DISTINCT character_ptr_id) as class_count
            FROM charactercreator_mage
            UNION
            SELECT "thief" as class_name, COUNT(DISTINCT character_ptr_id) as class_count
            FROM charactercreator_thief
            UNION
            SELECT "necromancer" as class_name, COUNT(DISTINCT mage_ptr_id) as class_count
            FROM charactercreator_necromancer"""
    
    result = cursor.execute(query).fetchall()
    for row in result:
        print(row[0], "count:", row[1])
    print()



# How many total items?
def count_items():
    query = """SELECT COUNT(DISTINCT item_id) as item_count
            FROM armory_item"""
    print('There are', get_count(query), 'items\n')


# How many of the items are weapons?  How many are not?
def count_weapons():
    # could also just count id's in armory_weapon
    query = """SELECT COUNT(DISTINCT item_id) as weapon_count
            FROM armory_item
            JOIN armory_weapon on armory_item.item_id = armory_weapon.item_ptr_id"""
    print('There are', get_count(query), 'weapons')

    query = """SELECT COUNT(DISTINCT item_id) as non_weapon_count
            FROM armory_item
            WHERE item_id NOT IN(SELECT item_ptr_id FROM armory_weapon)"""
    print(get_count(query), 'items are not weapons\n')

# How many items does each character have? (1st 20 rows)
def character_item_counts():
    query = """SELECT character_id, count(id) as item_count
            FROM charactercreator_character_inventory
            GROUP BY character_id
            LIMIT 20"""
    result = cursor.execute(query).fetchall()
    print(result[0].keys())
    for row in result:
        print(str(row[0]) + ',', row[1])
    print()

# How many weapons does each character have? (1st 20 rows)
def character_weapon_counts():
    query = """SELECT character_id, count(item_ptr_id) as weapon_count
               FROM charactercreator_character_inventory
               LEFT JOIN armory_weapon on charactercreator_character_inventory.item_id = armory_weapon.item_ptr_id
               GROUP BY character_id
               LIMIT 20"""
    result = cursor.execute(query).fetchall()
    print(result[0].keys())
    for row in result:
        print(str(row[0]) + ',', row[1])
    print()

def character_combined_counts():
    # combining the above two functions into one
    query = """SELECT character_id, count(distinct item_id) as item_count, count(distinct item_ptr_id) as weapon_count
            FROM charactercreator_character_inventory
            LEFT JOIN armory_weapon on charactercreator_character_inventory.item_id = armory_weapon.item_ptr_id
            GROUP BY character_id
            LIMIT 20"""
    result = cursor.execute(query).fetchall()
    print(result[0].keys())
    for row in result:
        print(str(row[0]) + ',', str(row[1]) + ',', row[2])
    print()

# On average, how many items does each character have?
def find_avg_items():
    query = """SELECT round(avg(item_count), 2)
            FROM(SELECT count(id) as item_count
                FROM charactercreator_character_inventory
                GROUP BY character_id)"""
    print("On average, each character has", get_count(query), 'items\n')

# On average, how many weapons does each character have?
def find_avg_weapons():
    query = """SELECT round(avg(weapon_count), 2)
            FROM(SELECT character_id, count(item_ptr_id) as weapon_count
                FROM charactercreator_character_inventory
                LEFT JOIN armory_weapon on charactercreator_character_inventory.item_id = armory_weapon.item_ptr_id
                GROUP BY character_id)"""
    print("On average, each character has", get_count(query), 'weapons\n')

# print results
count_characters()
#count_subclasses()
count_subclasses_combined()
count_items()
count_weapons()
# character_item_counts()
# character_weapon_counts()
character_combined_counts()
find_avg_items()
find_avg_weapons()
