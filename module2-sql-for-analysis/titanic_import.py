### migrates data from titanic.csv to postgresql

# load titanic data
import pandas as pd
url = 'https://raw.githubusercontent.com/jonDuke/DS-Unit-3-Sprint-2-SQL-and-Databases/master/module2-sql-for-analysis/titanic.csv'
df = pd.read_csv(url)

# df.columns = ['Survived', 'Pclass', 'Name', 'Sex', 'Age', 
#       'Siblings/Spouses Aboard', 'Parents/Children Aboard', 'Fare']

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

# create enum we will use (does nothing if the enum already exists)
enum_query = """
DO $$ BEGIN
    CREATE TYPE sex AS ENUM ('male', 'female');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
"""
cursor.execute(enum_query)

# create the table if it doesn't already exist
# SMALLINT limits: [-32768,32768]
# longest name entry in this file is 81 characters
table_query = """
CREATE TABLE IF NOT EXISTS titanic (
  id        SERIAL PRIMARY KEY,
  survived  SMALLINT,
  class     SMALLINT,
  name      VARCHAR(100),
  sex       sex,
  age       SMALLINT,
  siblings_spouses  SMALLINT,
  parents_children  SMALLINT,
  fare      NUMERIC
);"""
cursor.execute(table_query)

# set up the insertion query
cols = 'survived, class, name, sex, age, siblings_spouses, parents_children, fare'
insertion_query = f'INSERT INTO titanic ({cols}) VALUES %s'

# set up the data array
data = []
for i in range(len(df)):
    # data.append(df.iloc[i].tolist())  # I wish this worked

    # psycopg2 doesn't recognize datatype numpy.int64, so convert types myself
    row = df.iloc[i]
    data.append([int(row['Survived']), int(row['Pclass']), row['Name'], 
                 row['Sex'], int(row['Age']), int(row['Siblings/Spouses Aboard']), 
                 int(row['Parents/Children Aboard']), float(row['Fare'])])

# insert rows into the table
execute_values(cursor, insertion_query, data)

# commit changes
conn.commit()

# close the connection once we're done
cursor.close()
conn.close()
