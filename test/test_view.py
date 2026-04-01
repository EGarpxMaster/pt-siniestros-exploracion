import os, urllib.parse, pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}")

# Columnas de alertas
print("=== alertas ===")
print(pd.read_sql("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema='semaforos_PT' AND table_name='alertas'
    ORDER BY ordinal_position
""", engine).to_string())

# Columnas de alertas_historico
print("\n=== alertas_historico ===")
print(pd.read_sql("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema='semaforos_PT' AND table_name='alertas_historico'
    ORDER BY ordinal_position
""", engine).to_string())
