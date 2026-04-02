import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from shapely import wkb

def execute_phase3():
    print("Iniciando Fase 3: Optimizacion Espacial e Historicos en Base de Datos...")
    load_dotenv()
    
    encoded_pwd = urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))
    connection_string = f"postgresql://{os.getenv('DB_USER')}:{encoded_pwd}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}"
    engine = create_engine(connection_string, execution_options={"isolation_level": "AUTOCOMMIT"})
    
    # ---------------------------------------------------------
    # 1. UNION ESPACIAL: SUPERMANZANAS Y SEMAFOROS (En Python por ausencia de PostGIS)
    # ---------------------------------------------------------
    print("-> Descargando WKB de Supermanzanas y Semáforos para cruce espacial local...")
    df_sm = pd.read_sql('SELECT id_supermanzana, pobtot, geom FROM "semaforos_PT"."supermanzanas"', engine)
    df_sm['geometry'] = df_sm['geom'].apply(lambda x: wkb.loads(str(x), hex=True) if pd.notnull(x) else None)
    
    df_sem = pd.read_sql('SELECT id, "Identificador", ubicacion, geometry FROM "semaforos_PT"."semaforos"', engine)
    df_sem['punto'] = df_sem['geometry'].apply(lambda x: wkb.loads(str(x), hex=True) if pd.notnull(x) else None)

    print("-> Calculando centroides de Supermanzanas y asignando semáforos...")
    
    # Centroides
    df_sm['lat_centroide'] = df_sm['geometry'].apply(lambda g: g.centroid.y if g else None)
    df_sm['lon_centroide'] = df_sm['geometry'].apply(lambda g: g.centroid.x if g else None)
    
    id_supermanzanas_asignadas = []
    for _, sem in df_sem.iterrows():
        punto = sem['punto']
        encontrada = None
        min_dist = float('inf')
        if punto:
            for _, sm in df_sm.iterrows():
                poligono = sm['geometry']
                if poligono:
                    dist = poligono.distance(punto)
                    if dist < min_dist:
                        min_dist = dist
                        encontrada = sm['id_supermanzana']
            # Tolerancia maxima de 0.01 grados (~1.1 km) para asegurar que un semáforo
            # no se asigne a algo lejísimo si está en una carretera sin supermanzana.
            if min_dist > 0.01:
                encontrada = None
        id_supermanzanas_asignadas.append(encontrada)
        
    df_sem['id_supermanzana'] = id_supermanzanas_asignadas
    
    df_sem['lat'] = df_sem['punto'].apply(lambda g: g.y if g else None)
    df_sem['lon'] = df_sem['punto'].apply(lambda g: g.x if g else None)
    
    vista_sm = df_sm.drop(columns=['geometry'])
    vista_sem = df_sem.drop(columns=['geometry', 'punto'])

    print("-> Subiendo las nuevas Vistas físicas pre-calculadas a PostgreSQL...")
    with engine.connect() as conn:
        # Eliminar vistas con nombres antiguos
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_supermanzanas_stats'))
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_supermanzanas_opt'))
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_semaforos_siniestros'))
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_semaforos_opt'))
        # Eliminar vistas actuales para recrear
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_supermanzanas'))
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_semaforos'))
    
    vista_sm.to_sql('vw_supermanzanas', engine, schema='semaforos_PT', if_exists='replace', index=False)
    vista_sem.to_sql('vw_semaforos', engine, schema='semaforos_PT', if_exists='replace', index=False)

    # ---------------------------------------------------------
    # 2. VISTA MATERIALIZADA NATIVA: ALERTAS HISTÓRICAS
    # ---------------------------------------------------------
    print("-> Creando Vista Materializada Nativa para Alertas Historicas...")
    
    cols_query = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'semaforos_PT' AND table_name = 'alertas_historico'"
    cols_df = pd.read_sql(cols_query, engine)
    columns = cols_df['column_name'].tolist()
    
    cols_to_select = [
        f'"{col}"' for col in columns 
        if col not in ['Date', 'Country', 'City', 'Street', 'Location', 'Type', 'Subtype']
    ]
    # Añadimos los renombres manuales que queremos conservar en español
    cols_to_select.append('"Street" as calle')
    cols_str = ", ".join(cols_to_select)
    
    sql_historico = f"""
    -- Eliminar vistas materializadas antiguas
    DROP MATERIALIZED VIEW IF EXISTS "semaforos_PT"."mv_alertas_historico_heatmap";

    DROP MATERIALIZED VIEW IF EXISTS "semaforos_PT"."mv_alertas_historico";
    CREATE MATERIALIZED VIEW "semaforos_PT"."mv_alertas_historico" AS (
        SELECT 
            {cols_str},
            -- Traducción nativa de Subtype y Type re-empleando como columnas base
            CASE 
                WHEN "Type" = 'JAM' THEN 'Tráfico'
                WHEN "Type" = 'HAZARD' THEN 'Peligro'
                WHEN "Type" = 'ACCIDENT' THEN 'Accidente'
                WHEN "Type" = 'ROAD_CLOSED' THEN 'Vía Cerrada'
                ELSE "Type"
            END as tipo,
            CASE "Subtype"
                WHEN 'JAM_STAND_STILL_TRAFFIC' THEN 'Tráfico Detenido'
                WHEN 'JAM_HEAVY_TRAFFIC' THEN 'Tráfico Pesado'
                WHEN 'JAM_MODERATE_TRAFFIC' THEN 'Tráfico Moderado'
                WHEN 'JAM_LIGHT_TRAFFIC' THEN 'Tráfico Ligero'
                WHEN 'HAZARD_ON_ROAD' THEN 'Peligro en Vía'
                WHEN 'HAZARD_ON_ROAD_CAR_STOPPED' THEN 'Auto Detenido en Vía'
                WHEN 'HAZARD_ON_ROAD_CONSTRUCTION' THEN 'Obras Viales'
                WHEN 'HAZARD_ON_ROAD_EMERGENCY_VEHICLE' THEN 'Vehículo de Emergencia'
                WHEN 'HAZARD_ON_ROAD_LANE_CLOSED' THEN 'Carril Cerrado'
                WHEN 'HAZARD_ON_ROAD_OBJECT' THEN 'Objeto en Vía'
                WHEN 'HAZARD_ON_ROAD_OIL' THEN 'Aceite en Vía'
                WHEN 'HAZARD_ON_ROAD_POT_HOLE' THEN 'Bache'
                WHEN 'HAZARD_ON_ROAD_ROAD_KILL' THEN 'Animal Atropellado'
                WHEN 'HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT' THEN 'Semáforo Descompuesto'
                WHEN 'HAZARD_ON_SHOULDER' THEN 'Peligro en Acotamiento'
                WHEN 'HAZARD_ON_SHOULDER_ANIMALS' THEN 'Animales en Acotamiento'
                WHEN 'HAZARD_ON_SHOULDER_CAR_STOPPED' THEN 'Auto Detenido (Acotamiento)'
                WHEN 'HAZARD_ON_SHOULDER_MISSING_SIGN' THEN 'Señal Faltante'
                WHEN 'HAZARD_WEATHER' THEN 'Clima Adverso'
                WHEN 'HAZARD_WEATHER_FLOOD' THEN 'Inundación'
                WHEN 'HAZARD_WEATHER_FOG' THEN 'Neblina'
                WHEN 'HAZARD_WEATHER_HAIL' THEN 'Granizo'
                WHEN 'HAZARD_WEATHER_HEAT_WAVE' THEN 'Ola de Calor'
                WHEN 'HAZARD_WEATHER_HEAVY_RAIN' THEN 'Lluvia Intensa'
                WHEN 'HAZARD_WEATHER_HURRICANE' THEN 'Huracán'
                WHEN 'HAZARD_WEATHER_MONSOON' THEN 'Monzón'
                WHEN 'HAZARD_WEATHER_TORNADO' THEN 'Tornado'
                WHEN 'ACCIDENT_MINOR' THEN 'Accidente Menor'
                WHEN 'ACCIDENT_MAJOR' THEN 'Accidente Mayor'
                WHEN 'ROAD_CLOSED_HAZARD' THEN 'Vía Cerrada (Peligro)'
                WHEN 'ROAD_CLOSED_CONSTRUCTION' THEN 'Vía Cerrada (Obras)'
                WHEN 'ROAD_CLOSED_EVENT' THEN 'Vía Cerrada (Evento)'
                WHEN 'ROAD_CLOSED' THEN 'Vía Cerrada'
                WHEN 'POLICE_VISIBLE' THEN 'Policía Visible'
                WHEN 'POLICE_HIDING' THEN 'Policía Oculto'
                WHEN 'POLICE_WITH_MOBILE_CAMERA' THEN 'Radar Móvil'
                WHEN 'POLICE' THEN 'Policía'
                ELSE 
                    CASE COALESCE(NULLIF("Subtype", ''), "Type")
                        WHEN 'JAM' THEN 'Tráfico'
                        WHEN 'HAZARD' THEN 'Peligro'
                        WHEN 'ACCIDENT' THEN 'Accidente'
                        WHEN 'ROAD_CLOSED' THEN 'Vía Cerrada'
                        WHEN 'POLICE' THEN 'Policía'
                        ELSE COALESCE(NULLIF("Subtype", ''), "Type")
                    END
            END as subtipo,
            
            -- Parsear Coordenadas con Regexp limpias
            CAST(SUBSTRING("Location" FROM '(?i)point\(([^ ]+)') AS DOUBLE PRECISION) AS lon_val,
            CAST(SUBSTRING("Location" FROM '(?i)point\([^ ]+ ([^)]+)\)') AS DOUBLE PRECISION) AS lat_val,
            
            -- Parsear Horas e inyectar Fecha
            CAST(
                TO_DATE(
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        LOWER("Date"),
                        'sept', 'sep'), 'ene', 'jan'), 'abr', 'apr'), 'ago', 'aug'), 'dic', 'dec'),
                    'DD Mon YYYY'
                ) AS TIMESTAMP
            ) as fecha_cierre
        FROM "semaforos_PT"."alertas_historico"
        WHERE "Subtype" NOT IN (
            'HAZARD_WEATHER_HEAVY_SNOW', 
            'HAZARD_WEATHER_FREEZING_RAIN', 
            'HAZARD_ON_ROAD_ICE',
            'HAZARD_WEATHER_SNOW'
        )
    );
    """
    with engine.connect() as conn:
        conn.execute(text(sql_historico))
        
        print("-> Añadiendo indices de mapa a las alertas históricas...")
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_hist_coords ON "semaforos_PT"."mv_alertas_historico" (lat_val, lon_val);'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_hist_fecha ON "semaforos_PT"."mv_alertas_historico" (fecha_cierre);'))

    print("¡Fase 3 Completada Exitosamente!")

if __name__ == "__main__":
    execute_phase3()
