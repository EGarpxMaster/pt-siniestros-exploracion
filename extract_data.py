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

    # Definir la vista SQL para alertas_historico
    vista_sql = r"""
    CREATE OR REPLACE VIEW "semaforos_PT".vw_alertas_historico_procesado AS
    SELECT 
        *,
        TO_DATE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                LOWER("Date"),
                'sept', 'sep'), 'ene', 'jan'), 'abr', 'apr'), 'ago', 'aug'), 'dic', 'dec'),
            'DD Mon YYYY'
        ) AS "Fecha_Parseada",
        CAST(SUBSTRING("Location" FROM '(?i)point\(([^ ]+)') AS DOUBLE PRECISION) AS lon,
        CAST(SUBSTRING("Location" FROM '(?i)point\([^ ]+ ([^)]+)\)') AS DOUBLE PRECISION) AS lat,
        CASE 
            WHEN "Type" = 'JAM' THEN 'Tráfico'
            WHEN "Type" = 'HAZARD' THEN 'Peligro'
            WHEN "Type" = 'ACCIDENT' THEN 'Accidente'
            WHEN "Type" = 'ROAD_CLOSED' THEN 'Vía Cerrada'
            ELSE "Type"
        END as "Type_Traducido",
        CASE "Subtype"
            WHEN 'JAM_STAND_STILL_TRAFFIC' THEN 'Tráfico Detenido'
            WHEN 'JAM_HEAVY_TRAFFIC' THEN 'Tráfico Pesado'
            WHEN 'JAM_MODERATE_TRAFFIC' THEN 'Tráfico Moderado'
            WHEN 'JAM_LIGHT_TRAFFIC' THEN 'Tráfico Ligero'
            WHEN 'HAZARD_ON_ROAD_POT_HOLE' THEN 'Baches'
            WHEN 'HAZARD_ON_SHOULDER_CAR_STOPPED' THEN 'Auto Detenido (Acotamiento)'
            WHEN 'HAZARD_ON_ROAD_CAR_STOPPED' THEN 'Auto Detenido'
            WHEN 'HAZARD_ON_ROAD' THEN 'Peligro en Vía'
            WHEN 'HAZARD_ON_ROAD_CONSTRUCTION' THEN 'Obras Viales'
            WHEN 'HAZARD_ON_ROAD_OBJECT' THEN 'Objeto en Vía'
            WHEN 'HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT' THEN 'Semáforo Descompuesto'
            WHEN 'HAZARD_WEATHER_FLOOD' THEN 'Inundación'
            WHEN 'HAZARD_WEATHER' THEN 'Clima Severo / Lluvia'
            WHEN 'ACCIDENT_MAJOR' THEN 'Accidente Mayor'
            WHEN 'ACCIDENT_MINOR' THEN 'Accidente Menor'
            ELSE "Subtype"
        END as "Subtype_Traducido"
    FROM "semaforos_PT"."alertas_historico";
    """
    
    try:
        from sqlalchemy import text
        with engine.begin() as conn: # Usar begin() para transacciones auto-commit
            conn.execute(text(vista_sql))
            print("👁️ Vista SQL `vw_alertas_historico_procesado` creada o actualizada exitosamente.")
            
    except Exception as e:
        print(f"⚠️ Atención: No se pudo crear la vista SQL en Postgres. Fallback a crudo. Error: {e}")

    # Tablas aceptadas limpias (excluimos 'alertas' por ser incorrecta y priorizamos historicos)
    tablas_permitidas = ['alertas_historico', 'semaforos', 'supermanzanas', 'vw_alertas_historico_procesado']

    for esquema in esquemas_interes:
        try:
            tablas_esquema = inspector.get_view_names(schema=esquema) + inspector.get_table_names(schema=esquema)
            tablas = [t for t in tablas_esquema if t in tablas_permitidas]
            
            # Si generamos la vista con exito, descargamos la vista y omitimos extraer la misma tabla en crudo
            if 'vw_alertas_historico_procesado' in tablas and 'alertas_historico' in tablas:
                tablas.remove('alertas_historico')

            print(f"\nEsquema '{esquema}': {len(tablas)} tablas/vistas aceptadas encontradas.")
            
            esquema_dir = os.path.join(output_dir, esquema)
            if not os.path.exists(esquema_dir):
                os.makedirs(esquema_dir)
                
            for tabla in tablas:
                print(f"  -> Extrayendo {esquema}.{tabla}...")
                query = f'SELECT * FROM "{esquema}"."{tabla}"'
                
                try:
                    df = pd.read_sql_query(query, engine)
                    
                    # Para mantener el dashboard funcionando sin refactor extremo, nombramos el CSV igual
                    output_name = "alertas_historico.csv" if tabla == 'vw_alertas_historico_procesado' else f"{tabla}.csv"
                    output_file = os.path.join(esquema_dir, output_name)
                    
                    df.to_csv(output_file, index=False)
                    print(f"     Guardado: {output_file} ({len(df)} filas)")
                except Exception as e:
                    print(f"     ❌ Error al extraer la tabla {tabla}: {e}")
                    
        except Exception as e:
            print(f"❌ Error al inspeccionar el esquema '{esquema}':\n{e}")

if __name__ == "__main__":
    extract_data()
