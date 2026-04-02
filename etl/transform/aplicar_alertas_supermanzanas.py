import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from shapely import wkb, Point
from shapely.strtree import STRtree

def execute_alertas_supermanzanas():
    print("Iniciando Fase de Optimización Espacial: Alertas vs Supermanzanas...")
    load_dotenv()
    
    encoded_pwd = urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))
    connection_string = f"postgresql://{os.getenv('DB_USER')}:{encoded_pwd}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}"
    engine = create_engine(connection_string, execution_options={"isolation_level": "AUTOCOMMIT"})
    
    # ---------------------------------------------------------
    # 1. CARGAR SUPERMANZANAS Y CONSTRUIR R-TREE (STRtree)
    # ---------------------------------------------------------
    print("-> Descargando WKB de Supermanzanas...")
    df_sm = pd.read_sql('SELECT id_supermanzana, geom FROM "semaforos_PT"."supermanzanas"', engine)
    df_sm['geometry'] = df_sm['geom'].apply(lambda x: wkb.loads(str(x), hex=True) if pd.notnull(x) else None)
    
    df_sm_valid = df_sm[df_sm['geometry'].notnull()].reset_index(drop=True)
    poligonos = df_sm_valid['geometry'].tolist()
    ids_supermanzanas = df_sm_valid['id_supermanzana'].tolist()
    
    print("-> Construyendo índice espacial STRtree para supermanzanas...")
    tree = STRtree(poligonos)
    
    # ---------------------------------------------------------
    # 2. PROCESAR ALERTAS EVENTOS (VISTA MATERIALIZADA)
    # ---------------------------------------------------------
    print("-> Descargando eventos recientes (mv_alertas)...")
    df_ev = pd.read_sql('SELECT * FROM "semaforos_PT"."mv_alertas"', engine)
    print(f"-> Asignando {len(df_ev)} eventos a supermanzanas...")
    
    puntos_ev = [Point(lon, lat) for lon, lat in zip(df_ev['longitud_aprox'], df_ev['latitud_aprox'])]
    
    asignaciones_ev = []
    for p in puntos_ev:
        idx = tree.query(p, predicate='intersects')
        if len(idx) > 0:
            asignaciones_ev.append(ids_supermanzanas[idx[0]])
        else:
            nearest_idx = tree.nearest(p)
            if nearest_idx is not None:
                encontrada = ids_supermanzanas[nearest_idx]
                dist = poligonos[nearest_idx].distance(p)
                if dist > 0.01:
                    encontrada = None
                asignaciones_ev.append(encontrada)
            else:
                asignaciones_ev.append(None)
                
    df_ev['id_supermanzana'] = asignaciones_ev
    
    # ---------------------------------------------------------
    # 3. PROCESAR ALERTAS HISTORICAS (VISTA MATERIALIZADA)
    # ---------------------------------------------------------
    print("-> Descargando histórico (mv_alertas_historico)...")
    df_hist = pd.read_sql('SELECT * FROM "semaforos_PT"."mv_alertas_historico"', engine)
    print(f"-> Asignando {len(df_hist)} históricos a supermanzanas...")
    
    puntos_hist = [Point(lon, lat) for lon, lat in zip(df_hist['lon_val'], df_hist['lat_val'])]
    
    asignaciones_hist = []
    for p in puntos_hist:
        if pd.isna(p.x) or pd.isna(p.y):
            asignaciones_hist.append(None)
            continue
            
        idx = tree.query(p, predicate='intersects')
        if len(idx) > 0:
            asignaciones_hist.append(ids_supermanzanas[idx[0]])
        else:
            nearest_idx = tree.nearest(p)
            if nearest_idx is not None:
                encontrada = ids_supermanzanas[nearest_idx]
                dist = poligonos[nearest_idx].distance(p)
                if dist > 0.01:
                    encontrada = None
                asignaciones_hist.append(encontrada)
            else:
                asignaciones_hist.append(None)
                
    df_hist['id_supermanzana'] = asignaciones_hist

    # ---------------------------------------------------------
    # 4. SUBIR A BASE DE DATOS
    # ---------------------------------------------------------
    print("-> Subiendo Vistas pre-calculadas (vw_alertas y vw_alertas_historico)...")
    with engine.connect() as conn:
        # Eliminar vistas con nombres antiguos
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_alertas_eventos_opt'))
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_alertas_siniestros'))
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_alertas_historico_opt'))
        # Eliminar vistas actuales para recrear
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_alertas'))
        conn.execute(text('DROP TABLE IF EXISTS "semaforos_PT".vw_alertas_historico'))

    df_ev.to_sql('vw_alertas', engine, schema='semaforos_PT', if_exists='replace', index=False)
    df_hist.to_sql('vw_alertas_historico', engine, schema='semaforos_PT', if_exists='replace', index=False, chunksize=10000)

    print("-> Indexando tablas optimizadas...")
    with engine.connect() as conn:
        conn.execute(text('CREATE INDEX idx_vw_ale_ev_sm ON "semaforos_PT".vw_alertas (id_supermanzana)'))
        conn.execute(text('CREATE INDEX idx_vw_ale_ev_fecha ON "semaforos_PT".vw_alertas (primera_alerta)'))
        
        conn.execute(text('CREATE INDEX idx_vw_ale_hist_sm ON "semaforos_PT".vw_alertas_historico (id_supermanzana)'))
        conn.execute(text('CREATE INDEX idx_vw_ale_hist_fecha ON "semaforos_PT".vw_alertas_historico (fecha_cierre)'))
        
    print("¡Proceso de Integración Espacial de Alertas Completado!")

if __name__ == "__main__":
    execute_alertas_supermanzanas()
