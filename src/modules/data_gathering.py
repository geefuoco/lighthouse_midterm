import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

path = os.path.abspath(os.path.join(__file__, "../../../.env"))

def get_data_sample(sample_size=100_000, table_name=None, q=None, forceRetrieve=False):
  dir_path = os.path.abspath(os.path.join(__file__, "../../../data"))
  data_path = os.path.abspath(os.path.join(__file__, "../../../data/sample.csv"))
  data_file = os.path.exists(data_path)
  df = None

  if data_file and not forceRetrieve:
    print("Data file exists. Retrieving from /data")
    try:
      df = pd.read_csv(data_path)
    except:
      print("Error while reading csv file to pandas dataframe")
  else:
    print("Data file does not exist or retrieving by force. Querying the database...")
    if not load():
      return
    
    if sample_size > 100_000:
      print("Sample size is too large. Max query allowed is 100 000")
      return

    dbparams = dict(database=os.getenv("DB"), user=os.getenv("USERNAME"), password=os.getenv("PASS") ,host=os.getenv("HOSTNAME"))

    query = f"""
    SELECT * FROM {table_name} LIMIT {sample_size}
    """
    connection = connect_to_db(dbparams)
    if connection:
      if q and "limit" in q.lower():
        df = pd.read_sql_query(q, connection)
      else:
        df = pd.read_sql_query(query, connection)
      if not os.path.exists(dir_path):
        os.mkdir(dir_path)
      df.to_csv(data_path, index=False)
  return df

def load():
  bool = load_dotenv(path)
  if not bool:
    print("Environment variables failed to load.")
    print(f"Path used was : {path}")
  return bool

def connect_to_db(dbparams):
  connection = None
  try:
    connection = psycopg2.connect(**dbparams)
  except ConnectionError:
    print("Failed to connect to Database")
  except:
    print("Error has occured")
    print("params: ", dbparams)
  return connection
  
def execute_query(query, connection):
  if not "limit" in query.lower():
    print("Please do not query the entire database. Ensure you are adding a LIMIT statement")
    return
  try:
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    return data
  except:
    print("Error occured while executing the query.")
