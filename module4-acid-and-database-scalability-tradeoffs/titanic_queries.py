# load environment variables
import os
from dotenv import load_dotenv
load_dotenv()
DB_NAME=os.getenv('DB_NAME', default='OOPS')
DB_USER=os.getenv('DB_USER')
DB_PASSWORD=os.getenv('DB_PASSWORD')
DB_HOST=os.getenv('DB_HOST')

# open the PostGreSQL connection
import psycopg2
from psycopg2.extras import execute_values
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASSWORD, host=DB_HOST)
cursor = conn.cursor()

### ----- Questions -----
# How many passengers survived, and how many died?
print('How many passengers survived, and how many died?')
query = """
SELECT 
    survived,
    COUNT(DISTINCT id)
FROM passengers
GROUP BY survived;
"""
cursor.execute(query)
result = cursor.fetchall()
print(result[1][1], 'passengers survived')
print(result[0][1], 'passengers did not survive')

# How many passengers were in each class?
print('\nHow many passengers were in each class?')
query = """
SELECT
	class, 
	COUNT(DISTINCT id)
FROM passengers
GROUP BY class;
"""
cursor.execute(query)
result = cursor.fetchall()
for row in result:
    print(f'{row[1]} passengers were in class {row[0]}')

# How many passengers survived/died within each class?
print('\nHow many passengers survived/died within each class?')
query = """
SELECT
	class,
	COUNT(DISTINCT id),
	SUM(survived)
FROM passengers
GROUP BY class;
"""
cursor.execute(query)
result = cursor.fetchall()
for row in result:
    print(f'From class {row[0]}, {row[2]} survived and',
          f'{row[1] - row[2]} did not.')

# What was the average age of survivors vs nonsurvivors?
print('\nWhat was the average age of survivors vs nonsurvivors?')
query = """
SELECT
	survived,
	ROUND(AVG(age), 2)
FROM passengers
GROUP BY survived;
"""
cursor.execute(query)
result = cursor.fetchall()
print('The average age of survivors was', result[1][1])
print('The average age of nonsurvivors was,', result[0][1])

# What was the average age of each passenger class?
print('\nWhat was the average age of each passenger class?')
query = """
SELECT
	class,
	ROUND(AVG(age), 2)
FROM passengers
GROUP BY class;
"""
cursor.execute(query)
result = cursor.fetchall()
for row in result:
    print(f'The average age of passengers in class {row[0]} was {row[1]}')

# What was the average fare by passenger class? By survival?
print('\nWhat was the average fare by passenger class? By survival?')
query = """
SELECT
	class,
	ROUND(AVG(fare), 2)
FROM passengers
GROUP BY class;
"""
cursor.execute(query)
result = cursor.fetchall()
for row in result:
    print(f'The average fare of passengers in class {row[0]} was ${row[1]}')

query = """
SELECT
	survived,
	ROUND(AVG(fare), 2)
FROM passengers
GROUP BY survived;
"""
cursor.execute(query)
result = cursor.fetchall()
print(f'The average fare of survivors was ${result[1][1]}')
print(f'The average fare of nonsurvivors was ${result[0][1]}')

# How many siblings/spouses aboard on average, by passenger class? By survival?
print('\nHow many siblings/spouses aboard on average, by passenger class? By survival?')
query = """
SELECT
	class,
	ROUND(AVG(siblings_spouses), 2)
FROM passengers
GROUP BY class;
"""
cursor.execute(query)
result = cursor.fetchall()
for row in result:
    print('The average number of siblings/spouses of passengers in class',
          f'{row[0]} was {row[1]}')

query = """
SELECT
	survived,
	ROUND(AVG(siblings_spouses), 2)
FROM passengers
GROUP BY survived;
"""
cursor.execute(query)
result = cursor.fetchall()
print('The average number of siblings/spouses of survivors was,',
      result[1][1])
print('The average number of siblings/spouses of nonsurvivors was,',
      result[0][1])

# How many parents/children aboard on average, by passenger class? By survival?
print('\nHow many parents/children aboard on average, by passenger class? By survival?')
query = """
SELECT
	class,
	ROUND(AVG(parents_children), 2)
FROM passengers
GROUP BY class;
"""
cursor.execute(query)
result = cursor.fetchall()
for row in result:
    print('The average number of parents/children of passengers in class',
          f'{row[0]} was {row[1]}')

query = """
SELECT
	survived,
	ROUND(AVG(parents_children), 2)
FROM passengers
GROUP BY survived;
"""
cursor.execute(query)
result = cursor.fetchall()
print('The average number of parents/children of survivors was,',
      result[1][1])
print('The average number of parents/children of nonsurvivors was,',
      result[0][1])

# Do any passengers have the same name?
print('\nDo any passengers have the same name?')
query = """
SELECT
	COUNT(DISTINCT id) AS id_count,
	COUNT(DISTINCT name) AS name_count
FROM passengers
"""
cursor.execute(query)
result = cursor.fetchall()
print(f'There are {result[0][0]} passengers and {result[0][1]} unique names')
if result[0][0] == result[0][1]:
    print('No, each passenger has a unique name')
else:
    print('Yes, some passengers have the same name')
    
    # it takes a different query to list those unique names
    # there are none in this dataset but this would work
    # (I tested by adding a duplicate name myself)
    query = """
    SELECT *
    FROM (
        SELECT
            name,
            COUNT(DISTINCT id) AS name_count
        FROM passengers
        GROUP BY name
    ) AS subquery
    WHERE subquery.name_count > 1;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    for row in result:
        print(f'{row[1]} passengers were named {row[0]}')

# (Bonus! Hard, may require pulling and processing with Python) 
# How many married couples were aboard the Titanic? 
# 
# Assume that two people (one Mr. and one Mrs.) with the same last 
# name and with at least 1 sibling/spouse aboard are a married couple.

print('\nHow many married couples were aboard the Titanic?')
# first, get the names of passengers who have 
# at least 1 sibling/spouse aboard
query = """
SELECT name
FROM passengers
WHERE siblings_spouses >= 1;
"""
cursor.execute(query)
result = cursor.fetchall()

# take each tuple, eg. ('Mr. John Smith',)
# and split each name into an array, eg. ['Mr.', 'John', 'Smith']
names = []
for row in result:
    names.append(row[0].split())

# search the array for matching names
married_count = 0
for i in range(len(names)-1):
    # this passenger's last name
    lastname = names[i][-1] 

    # save the honorific we are looking for
    other = ''
    if(names[i][0] == 'Mr.'):
        other = 'Mrs.'
    elif(names[i][0] == 'Mrs.'):
        other = 'Mr.'
    else:
        continue # honorific is something else, this passenger is not married

    # compare against each other passenger we haven't seen yet
    for j in range(i+1, len(names)):
        # check if last names match and the honorific is correct
        if(lastname == names[j][-1] and other == names[j][0]):
            married_count += 1
            
            # debug statement to print all couple names
            #print('Found couple:', ' '.join(names[i]), 'and', ' '.join(names[j]))

            # assume each passenger can be married only once, 
            # stop checking for more
            break 

print(f'Found {married_count} married couples')


# close the connection once we're done
cursor.close()
conn.close()
