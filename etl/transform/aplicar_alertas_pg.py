import os
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def optimize_alertas():
    load_dotenv()
    encoded_pwd = urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))
    engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{encoded_pwd}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}", execution_options={"isolation_level": "AUTOCOMMIT"})
    
    with engine.connect() as conn:
        print("1. Eliminando vistas antiguas...")
        conn.execute(text('DROP MATERIALIZED VIEW IF EXISTS "semaforos_PT".mv_alertas_eventos;'))
        conn.execute(text('DROP MATERIALIZED VIEW IF EXISTS "semaforos_PT".mv_alertas_siniestros;'))

        print("2. Re-creando Vista Materializada 'mv_alertas' con Traducciones a Español Nativas...")
        
        conn.execute(text('DROP MATERIALIZED VIEW IF EXISTS "semaforos_PT".mv_alertas;'))
        
        # Diccionarios de PostgreSQL en formato CASE WHEN
        sql_view = """
        CREATE MATERIALIZED VIEW "semaforos_PT".mv_alertas AS (
            WITH traducciones AS (
                SELECT 
                    id_registro, 
                    calle, 
                    fecha,
                    ROUND(latitud::numeric, 4) as lat_cluster,
                    ROUND(longitud::numeric, 4) as lon_cluster,
                    latitud, longitud,
                    CASE tipo
                        WHEN 'JAM' THEN 'Tráfico'
                        WHEN 'HAZARD' THEN 'Peligro'
                        WHEN 'ACCIDENT' THEN 'Accidente'
                        WHEN 'ROAD_CLOSED' THEN 'Vía Cerrada'
                        WHEN 'POLICE' THEN 'Policía'
                        WHEN 'CHIT_CHAT' THEN 'Chat'
                        ELSE tipo
                    END as tipo_es,
                    CASE COALESCE(NULLIF(subtipo, ''), tipo)
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
                        WHEN 'JAM' THEN 'Tráfico'
                        WHEN 'HAZARD' THEN 'Peligro'
                        WHEN 'ACCIDENT' THEN 'Accidente'
                        WHEN 'CHIT_CHAT' THEN 'Chat'
                        ELSE COALESCE(NULLIF(subtipo, ''), tipo)
                    END as subtipo_es
                FROM "semaforos_PT"."alertas"
                WHERE COALESCE(subtipo, '') NOT IN (
                    'HAZARD_WEATHER_HEAVY_SNOW', 
                    'HAZARD_WEATHER_FREEZING_RAIN', 
                    'HAZARD_ON_ROAD_ICE',
                    'HAZARD_WEATHER_SNOW'
                )
            ),
            diferencias AS (
                SELECT *,
                    EXTRACT(EPOCH FROM (fecha - LAG(fecha) OVER (
                        PARTITION BY tipo_es, subtipo_es, lat_cluster, lon_cluster 
                        ORDER BY fecha
                    ))) / 3600.0 AS diff_horas
                FROM traducciones
            ),
            marcadores AS (
                SELECT *,
                    CASE WHEN diff_horas IS NULL OR diff_horas > 12 THEN 1 ELSE 0 END AS es_nuevo
                FROM diferencias
            ),
            grupos AS (
                SELECT *,
                    SUM(es_nuevo) OVER (
                        PARTITION BY tipo_es, subtipo_es, lat_cluster, lon_cluster 
                        ORDER BY fecha
                    ) AS evento_id_relativo
                FROM marcadores
            )
            SELECT 
                tipo_es as tipo, 
                subtipo_es as subtipo,
                MODE() WITHIN GROUP (ORDER BY calle) as calle_principal,
                AVG(latitud) as latitud_aprox,
                AVG(longitud) as longitud_aprox,
                MIN(fecha) as primera_alerta,
                MAX(fecha) as ultima_alerta,
                COUNT(id_registro) as total_reportes,
                EXTRACT(EPOCH FROM (MAX(fecha) - MIN(fecha))) / 3600.0 AS duracion_horas
            FROM grupos
            GROUP BY tipo_es, subtipo_es, lat_cluster, lon_cluster, evento_id_relativo
        );
        """
        conn.execute(text(sql_view))
        print("3. Re-creando Indice B-Tree de Coordenadas...")
        conn.execute(text('CREATE INDEX idx_mv_alertas_ev_coords ON "semaforos_PT"."mv_alertas" (latitud_aprox, longitud_aprox);'))

        print("4. Re-creando Indice B-Tree de Fechas...")
        conn.execute(text('CREATE INDEX idx_mv_alertas_ev_fecha ON "semaforos_PT"."mv_alertas" (primera_alerta);'))

        print("Exito! Todo completado y en Español nativo :)")

if __name__ == "__main__":
    optimize_alertas()
