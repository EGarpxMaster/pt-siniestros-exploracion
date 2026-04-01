import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
pwd = urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))
engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{pwd}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}")
df = pd.read_sql("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'semaforos_PT' AND table_name = 'supermanzanas'", engine)
print("--- SUPERMANZANAS ---")
print(df)
df2 = pd.read_sql("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'semaforos_PT' AND table_name = 'semaforos'", engine)
print("--- SEMAFOROS ---")
print(df2)
