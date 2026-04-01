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
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_supermanzanas_opt'))
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_semaforos_opt'))
    
    vista_sm.to_sql('vw_supermanzanas_opt', engine, schema='semaforos_PT', if_exists='replace', index=False)
    vista_sem.to_sql('vw_semaforos_opt', engine, schema='semaforos_PT', if_exists='replace', index=False)

    # ---------------------------------------------------------
    # 2. VISTA MATERIALIZADA NATIVA: ALERTAS HISTÓRICAS
    # ---------------------------------------------------------
    print("-> Creando Vista Materializada Nativa para Alertas Historicas (Heatmap)...")
    
    sql_historico = r"""
    DROP MATERIALIZED VIEW IF EXISTS "semaforos_PT"."mv_alertas_historico_heatmap";
    CREATE MATERIALIZED VIEW "semaforos_PT"."mv_alertas_historico_heatmap" AS (
        SELECT 
            *,
            -- Traducción nativa de Subtype y Type
            CASE 
                WHEN "Type" = 'JAM' THEN 'Tráfico'
                WHEN "Type" = 'HAZARD' THEN 'Peligro'
                WHEN "Type" = 'ACCIDENT' THEN 'Accidente'
                WHEN "Type" = 'ROAD_CLOSED' THEN 'Vía Cerrada'
                ELSE "Type"
            END as tipo_es,
            CASE "Subtype"
                WHEN 'JAM_STAND_STILL_TRAFFIC' THEN 'Tráfico Detenido'
                WHEN 'JAM_HEAVY_TRAFFIC' THEN 'Tráfico Pesado'
                WHEN 'JAM_MODERATE_TRAFFIC' THEN 'Tráfico Moderado'
                WHEN 'JAM_LIGHT_TRAFFIC' THEN 'Tráfico Ligero'
                WHEN 'HAZARD_ON_ROAD_POT_HOLE' THEN 'Bache'
                WHEN 'HAZARD_ON_SHOULDER_CAR_STOPPED' THEN 'Auto Detenido (Acotamiento)'
                WHEN 'HAZARD_ON_ROAD_CAR_STOPPED' THEN 'Auto Detenido'
                WHEN 'HAZARD_ON_ROAD' THEN 'Peligro en Vía'
                WHEN 'HAZARD_ON_ROAD_CONSTRUCTION' THEN 'Obras Viales'
                WHEN 'HAZARD_ON_ROAD_OBJECT' THEN 'Objeto en Vía'
                WHEN 'HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT' THEN 'Semáforo Descompuesto'
                WHEN 'HAZARD_WEATHER_FLOOD' THEN 'Inundación'
                WHEN 'HAZARD_WEATHER' THEN 'Clima Adverso'
                WHEN 'ACCIDENT_MAJOR' THEN 'Accidente Mayor'
                WHEN 'ACCIDENT_MINOR' THEN 'Accidente Menor'
                ELSE 
                    CASE COALESCE(NULLIF("Subtype", ''), "Type")
                        WHEN 'JAM' THEN 'Tráfico'
                        WHEN 'HAZARD' THEN 'Peligro'
                        WHEN 'ACCIDENT' THEN 'Accidente'
                        WHEN 'ROAD_CLOSED' THEN 'Vía Cerrada'
                        ELSE COALESCE(NULLIF("Subtype", ''), "Type")
                    END
            END as subtipo_es,
            
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
    );
    """
    with engine.connect() as conn:
        conn.execute(text(sql_historico))
        
        print("-> Añadiendo indices de mapa a las alertas históricas...")
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_hist_coords ON "semaforos_PT"."mv_alertas_historico_heatmap" (lat_val, lon_val);'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_hist_fecha ON "semaforos_PT"."mv_alertas_historico_heatmap" (fecha_cierre);'))

    print("¡Fase 3 Completada Exitosamente!")

if __name__ == "__main__":
    execute_phase3()
