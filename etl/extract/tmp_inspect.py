import os
import urllib.parse
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

load_dotenv()
encoded_pwd = urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))
engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{encoded_pwd}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}")
for t in ['usuarios', 'log_accesos', 'supermanzanas', 'alertas_historico']:
    print(f"--- {t} ---")
    query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'semaforos_PT' AND table_name = '{t}'"
    try:
        import pandas as pd
        df = pd.read_sql(query, engine)
        if not df.empty:
            for _, row in df.iterrows():
                print(f"  {row['column_name']}: {row['data_type']}")
        else:
            print("  (Sin columnas o sin acceso)")
    except Exception as e:
        print(f"  Error al leer tabla {t}: {e}")
