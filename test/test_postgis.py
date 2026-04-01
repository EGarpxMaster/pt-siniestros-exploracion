import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
pwd = urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))
engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{pwd}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}")
try:
    df = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'semaforos_PT'", engine)
    print("Tables in semaforos_PT:")
    print(df)
except Exception as e:
    print("Error:", e)
