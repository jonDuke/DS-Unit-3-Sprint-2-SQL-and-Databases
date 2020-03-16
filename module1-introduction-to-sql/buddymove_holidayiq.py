# buddymove_holidayiq.py

# load source data
import pandas as pd
url = 'https://raw.githubusercontent.com/jonDuke/DS-Unit-3-Sprint-2-SQL-and-Databases/master/module1-introduction-to-sql/buddymove_holidayiq.csv'
df = pd.read_csv(url)

# open database connection
import sqlite3
PATH = ''  # if blank, will open/create the database in same directory
conn = sqlite3.connect(PATH + 'buddymove_holidayiq.sqlite3')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# create or open the new table
# could also use if_exists='replace' to just overwrite the table no matter what
try:
    df.to_sql('review', conn)
except:
    # table exists, no changes made
    print('Table already exists\n')
else:
    # table does not yet exist, create it
    print('Table added\n')


# assignment questions:

# Count how many rows you have
query = """SELECT count(distinct "User Id")
        FROM review"""
result = cursor.execute(query).fetchall()
print('There are', result[0][0], 'rows')

# How many users who reviewed at least 100 Nature in the category 
# also reviewed at least 100 in the Shopping category?
query = """SELECT count(distinct "User Id")
        FROM review
        WHERE Nature >= 100 AND Shopping >= 100"""
result = cursor.execute(query).fetchall()
print('There are', result[0][0], 'users who had at least 100 reviews in both Nature and Shopping')

# What are the average number of reviews for each category?
query = """SELECT round(avg(Sports), 2) as avgSports, 
        round(avg(Religious), 2) as avgReligious, 
        round(avg(Nature), 2) as avgNature, 
        round(avg(Theatre), 2) as avgTheatre, 
        round(avg(Shopping), 2) as avgShopping, 
        round(avg(Picnic), 2) as avgPicnic
        FROM review"""
result = cursor.execute(query).fetchall()
print('Average number of reviews in each category:')
for key in result[0].keys():
    print(key + ':', result[0][key])
