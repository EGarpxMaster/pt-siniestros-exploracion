import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

def extract_data(output_dir="data"):
    load_dotenv()
    
    encoded_pwd = urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))
    connection_string = f"postgresql://{os.getenv('DB_USER')}:{encoded_pwd}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}"
    engine = create_engine(connection_string)

    try:
        with engine.connect() as conn:
            print("✅ Conexión exitosa a la base de datos.")
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos:\n{e}")
        return

    esquema = 'semaforos_PT'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Las nuevas vistas optimizadas físicamente en PostgreSQL
    tablas_permitidas = ['vw_supermanzanas', 'vw_alertas_historico', 'vw_alertas', 'vw_semaforos']

    esquema_dir = os.path.join(output_dir, esquema)
    if not os.path.exists(esquema_dir):
        os.makedirs(esquema_dir)
        
    print(f"\nEsquema '{esquema}': Descargando {len(tablas_permitidas)} tablas/vistas aceptadas directamente.")
    
    for tabla in tablas_permitidas:
        print(f"  -> Extrayendo {esquema}.{tabla}...")
        query = f'SELECT * FROM "{esquema}"."{tabla}"'
        
        try:
            df = pd.read_sql_query(query, engine)
            
            # Renombrar archivos locales dinámicamente quitando 'vw_'
            nombre_base = tabla.replace('vw_', '')
            output_name = f"{nombre_base}.csv"

            output_file = os.path.join(esquema_dir, output_name)
            df.to_csv(output_file, index=False)
            print(f"     Guardado: {output_file} ({len(df)} filas)")
        except Exception as e:
            print(f"     ❌ Error al extraer la tabla {tabla}: {e}")

if __name__ == "__main__":
    extract_data()
