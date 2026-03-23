import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

def extract_data(output_dir="data"):
    # Cargar variables de entorno
    load_dotenv()
    
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        print("Error: Faltan credenciales en el archivo .env")
        return

    encoded_pwd = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql://{DB_USER}:{encoded_pwd}@{DB_HOST}:5432/{DB_NAME}"
    engine = create_engine(connection_string)

    try:
        with engine.connect() as conn:
            print("✅ Conexión exitosa a la base de datos.")
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos:\n{e}")
        return

    inspector = inspect(engine)
    esquemas_interes = ['semaforos', 'semaforos_PT', 'simo', 'siniestralidad']
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for esquema in esquemas_interes:
        try:
            tablas = inspector.get_table_names(schema=esquema)
            print(f"\nEsquema '{esquema}': {len(tablas)} tablas encontradas.")
            
            esquema_dir = os.path.join(output_dir, esquema)
            if not os.path.exists(esquema_dir):
                os.makedirs(esquema_dir)
                
            for tabla in tablas:
                print(f"  -> Extrayendo {esquema}.{tabla}...")
                query = f'SELECT * FROM "{esquema}"."{tabla}"'
                
                try:
                    df = pd.read_sql_query(query, engine)
                    output_file = os.path.join(esquema_dir, f"{tabla}.csv")
                    df.to_csv(output_file, index=False)
                    print(f"     Guardado: {output_file} ({len(df)} filas)")
                except Exception as e:
                    print(f"     ❌ Error al extraer la tabla {tabla}: {e}")
                    
        except Exception as e:
            print(f"❌ Error al inspeccionar el esquema '{esquema}':\n{e}")

if __name__ == "__main__":
    extract_data()
