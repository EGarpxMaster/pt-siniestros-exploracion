import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from shapely import wkb

def extract_specific_data(output_dir="data"):
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
            print("Conexión exitosa a la base de datos.")
    except Exception as e:
        print(f"Error al conectar a la base de datos:\n{e}")
        return

    esquema = 'semaforos_PT'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    tablas_a_extraer = ['mv_alertas_eventos', 'semaforos']
    
    esquema_dir = os.path.join(output_dir, esquema)
    if not os.path.exists(esquema_dir):
        os.makedirs(esquema_dir)
        
    for tabla in tablas_a_extraer:
        print(f"  -> Extrayendo {esquema}.{tabla}...")
        query = f'SELECT * FROM "{esquema}"."{tabla}"'
        
        try:
            df = pd.read_sql_query(query, engine)
            
            if tabla == 'semaforos' and 'geometry' in df.columns:
                print("     Optimizando geometria espacial WKB a coordenadas Lat/Lon...")
                latitudes, longitudes = [], []
                for _, row in df.iterrows():
                    try:
                        geom = wkb.loads(str(row['geometry']), hex=True)
                        latitudes.append(geom.y)
                        longitudes.append(geom.x)
                    except Exception:
                        latitudes.append(None)
                        longitudes.append(None)
                df['lat'] = latitudes
                df['lon'] = longitudes
                df = df.drop(columns=['geometry'], errors='ignore')

            elif tabla == 'mv_alertas_eventos':
                print("     Organizando vista materializada nativa en español...")

                df['primera_alerta_local'] = pd.to_datetime(df['primera_alerta'], utc=True).dt.tz_convert('America/Cancun').dt.tz_localize(None)
                df['ultima_alerta_local'] = pd.to_datetime(df['ultima_alerta'], utc=True).dt.tz_convert('America/Cancun').dt.tz_localize(None)
                
                df['anio'] = df['primera_alerta_local'].dt.year.astype('int16')
                df['mes'] = df['primera_alerta_local'].dt.month.astype('int8')
                df['dia'] = df['primera_alerta_local'].dt.day.astype('int8')
                df['hora'] = df['primera_alerta_local'].dt.hour.astype('int8')

                dia_map = {0:'Lunes', 1:'Martes', 2:'Miercoles', 3:'Jueves',
                           4:'Viernes', 5:'Sabado', 6:'Domingo'}
                df['dia_semana'] = df['primera_alerta_local'].dt.dayofweek.map(dia_map).astype('category')

                df['turno'] = pd.cut(
                    df['hora'],
                    bins=[-1, 5, 11, 17, 23],
                    labels=['Madrugada', 'Manana', 'Tarde', 'Noche']
                ).astype('category')
                
                # Nombres compatibles con el antiguo modelo de datos para q la API no falle:
                df['tipo_es'] = df['tipo']
                df['subtipo_es'] = df['subtipo']
                df['municipio'] = 'Benito Juarez'

                df = df.drop(columns=['primera_alerta', 'ultima_alerta'], errors='ignore')

                print(f"     Listo. Columnas preparadas: {len(df.columns)}")
            
            output_name = f"{'alertas' if tabla == 'mv_alertas_eventos' else tabla}.csv"
            output_file = os.path.join(esquema_dir, output_name)
            
            df.to_csv(output_file, index=False)
            print(f"     Guardado: {output_file} ({len(df)} filas)")
            
        except Exception as e:
            print(f"     ❌ Error al extraer la tabla {tabla}: {e}")

if __name__ == "__main__":
    print("Iniciando extraccion de BD completamente en Español...")
    extract_specific_data()
