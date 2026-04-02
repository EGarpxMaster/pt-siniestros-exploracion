import streamlit as st
import pandas as pd
import folium
from folium.plugins import HeatMap, MarkerCluster
from streamlit_folium import st_folium
import plotly.express as px
import os
from shapely import wkb
import json

st.set_page_config(page_title="Histórico de Alertas - SISV", layout="wide")
st.title("🚦 Análisis Histórico Interactivo")
st.markdown("Explora la siniestralidad vial mediante un mapa de calor por Supermanzanas y realiza un análisis detallado por cruceros.")

# --- Rutas de Datos ---
DATA_DIR = os.path.join("data", "semaforos_PT")
HISTORICO_PATH = os.path.join(DATA_DIR, "alertas_historico.csv")
SEMAFOROS_PATH = os.path.join(DATA_DIR, "semaforos.csv")
SUPERMANZANAS_PATH = os.path.join(DATA_DIR, "supermanzanas.csv")

# --- Gestión de Estado ---
if 'sm_activa' not in st.session_state:
    st.session_state.sm_activa = None
if 'sem_activo' not in st.session_state:
    st.session_state.sem_activo = None

@st.cache_data
<<<<<<< HEAD
def load_historical_data():
    if not os.path.exists(HISTORICO_PATH): return None
    df = pd.read_csv(HISTORICO_PATH)
    # Nueva columna estandarizada
    df['fecha_cierre'] = pd.to_datetime(df['fecha_cierre'], errors='coerce')
=======
def load_and_process_data(filepath):
    df = pd.read_csv(filepath)
    
    # 1. Parsear Fechas Españolas
    if 'Date' in df.columns:
        def parse_date(d_str):
            try:
                parts = str(d_str).lower().strip().split()
                if len(parts) >= 3:
                    dia = parts[0].zfill(2)
                    mes = meses_es.get(parts[1][:3], '01')
                    anio = parts[2]
                    return pd.to_datetime(f"{anio}-{mes}-{dia}")
                return pd.to_datetime(d_str, errors='coerce')
            except:
                return pd.NaT
        
        df['Fecha_Parseada'] = df['Date'].apply(parse_date)
        
        meses_nombres = {1:'Enero', 2:'Febrero', 3:'Marzo', 4:'Abril', 5:'Mayo', 6:'Junio', 
                         7:'Julio', 8:'Agosto', 9:'Septiembre', 10:'Octubre', 11:'Noviembre', 12:'Diciembre'}
        dias_nombres = {0:'Lunes', 1:'Martes', 2:'Miércoles', 3:'Jueves', 4:'Viernes', 5:'Sábado', 6:'Domingo'}
        
        df['Mes'] = df['Fecha_Parseada'].dt.month.map(meses_nombres)
        df['Dia_Semana'] = df['Fecha_Parseada'].dt.dayofweek.map(dias_nombres)
        df['Orden_Dia'] = df['Fecha_Parseada'].dt.dayofweek # Para ordenar en gráficas
        df['Trimestre'] = df['Fecha_Parseada'].dt.to_period('Q').astype(str)

    # 2. Parsear Coordenadas "Point(lon lat)"
    if 'Location' in df.columns:
        coords = df['Location'].astype(str).str.extract(r'(?i)Point\(([-.\d]+)\s+([-.\d]+)\)')
        df['lon'] = coords[0].astype(float)
        df['lat'] = coords[1].astype(float)

    # 3. Traducir Tipos y Subtipos
    if 'Type' in df.columns:
        mapa_tipos = {
            'JAM': 'Tráfico',
            'HAZARD': 'Peligro',
            'ACCIDENT': 'Accidente',
            'ROAD_CLOSED': 'Vía Cerrada'
        }
        df['Type'] = df['Type'].map(mapa_tipos).fillna(df['Type'])
        
        # Crear variable numérica binaria para correlación
        df['Es_Accidente'] = (df['Type'] == 'Accidente').astype(int)
        
    if 'Fecha_Parseada' in df.columns:
        df['Mes_Num'] = df['Fecha_Parseada'].dt.month

    if 'Subtype' in df.columns:
        mapa_subtipos = {
            'JAM_STAND_STILL_TRAFFIC': 'Tráfico Detenido',
            'JAM_HEAVY_TRAFFIC': 'Tráfico Pesado',
            'JAM_MODERATE_TRAFFIC': 'Tráfico Moderado',
            'JAM_LIGHT_TRAFFIC': 'Tráfico Ligero',
            'HAZARD_ON_ROAD_POT_HOLE': 'Baches',
            'HAZARD_ON_SHOULDER_CAR_STOPPED': 'Auto Detenido (Acotamiento)',
            'HAZARD_ON_ROAD_CAR_STOPPED': 'Auto Detenido',
            'HAZARD_ON_ROAD': 'Peligro en Vía',
            'HAZARD_ON_ROAD_CONSTRUCTION': 'Obras Viales',
            'HAZARD_ON_ROAD_OBJECT': 'Objeto en Vía',
            'HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT': 'Semáforo Descompuesto',
            'HAZARD_WEATHER_FLOOD': 'Inundación',
            'HAZARD_WEATHER': 'Clima Severo / Lluvia',
            'ACCIDENT_MAJOR': 'Accidente Mayor',
            'ACCIDENT_MINOR': 'Accidente Menor'
        }
        df['Subtype'] = df['Subtype'].map(mapa_subtipos).fillna(df['Subtype'])

>>>>>>> parent of b2a9f5b (fix: graphs and optimization)
    return df

@st.cache_data
def load_catalogos():
    df_sem = pd.read_csv(SEMAFOROS_PATH) if os.path.exists(SEMAFOROS_PATH) else pd.DataFrame()
    df_sm = pd.read_csv(SUPERMANZANAS_PATH) if os.path.exists(SUPERMANZANAS_PATH) else pd.DataFrame()
    
    # Pre-procesar GeoJSON para Choropleth
    geojson_sm = {"type": "FeatureCollection", "features": []}
    if not df_sm.empty and 'geom' in df_sm.columns:
        for _, row in df_sm.iterrows():
            try:
                geom = wkb.loads(str(row['geom']), hex=True)
                feature = {
                    "type": "Feature",
                    "id": str(row['id_supermanzana']),
                    "properties": {
                        "id_supermanzana": str(row['id_supermanzana']), 
                        "pobtot": row['pobtot']
                    },
                    "geometry": geom.__geo_interface__
                }
                geojson_sm["features"].append(feature)
            except: continue
            
    return df_sem, df_sm, geojson_sm

def reset_view():
    st.session_state.sm_activa = None
    st.session_state.sem_activo = None

# --- Carga de Datos ---
df_hist = load_historical_data()
df_sem, df_sm, geojson_sm = load_catalogos()

if df_hist is None:
    st.error("No se encontró el archivo de histórico de alertas.")
    st.stop()

# --- Sidebar: Filtros Globales ---
st.sidebar.header("Filtros de Análisis")
min_date = df_hist['fecha_cierre'].min()
max_date = df_hist['fecha_cierre'].max()

# Selección de fecha con validación
rango = st.sidebar.date_input("Periodo de Observación", 
                             [max_date - pd.Timedelta(days=30), max_date], 
                             min_value=min_date, max_value=max_date)

if isinstance(rango, list) and len(rango) == 2:
    start_date, end_date = pd.to_datetime(rango[0]), pd.to_datetime(rango[1])
    df_filtrado = df_hist[(df_hist['fecha_cierre'] >= start_date) & (df_hist['fecha_cierre'] <= end_date)]
else:
    df_filtrado = df_hist

st.sidebar.button("Reiniciar Vista Mapa 🏠", on_click=reset_view)

# --- Métricas Generales ---
m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Alertas", len(df_filtrado))
m2.metric("Siniestros", len(df_filtrado[df_filtrado['tipo'] == 'Accidente']))
m3.metric("Peligros", len(df_filtrado[df_filtrado['tipo'] == 'Peligro']))
m4.metric("Supermanzanas Impactadas", df_filtrado['id_supermanzana'].nunique())

# --- Cuerpo Principal ---
tab_mapa, tab_stats, tab_raw = st.tabs(["📍 Mapa Interactivo", "📊 Estadísticas", "📋 Datos"])

with tab_mapa:
    st.subheader("Mapa de Calor y Localización por Supermanzanas")
    
    if not st.session_state.sm_activa:
        st.info("💡 Haz click en una Supermanzana en el mapa para ver el detalle de cruceros.")
    else:
        st.success(f"📍 Viendo detalle de Supermanzana: {st.session_state.sm_activa}")

    # Calcular densidad por SM para Choropleth
    sm_stats = df_filtrado.groupby('id_supermanzana').size().reset_index(name='conteo')
    
    # Lógica de Mapa
    if st.session_state.sm_activa:
        # 1. Vista de Detalle (Zoom en la SM elegida)
        centro_sm = df_sm[df_sm['id_supermanzana'] == st.session_state.sm_activa]
        m = folium.Map(location=[centro_sm.iloc[0]['lat_centroide'], centro_sm.iloc[0]['lon_centroide']], zoom_start=15)
        
        # Capa de Semáforos
        df_sem_sm = df_sem[df_sem['id_supermanzana'] == st.session_state.sm_activa]
        for _, sem in df_sem_sm.iterrows():
            folium.CircleMarker(
                location=[sem['lat'], sem['lon']],
                radius=8, color='black', fill=True, fill_color='yellow',
                popup=f"Semáforo: {sem['Identificador']}\nUbicación: {sem['ubicacion']}",
                tooltip=f"🚦 {sem['Identificador']}"
            ).add_to(m)
            
        # Capa de Alertas
        df_ale_sm = df_filtrado[df_filtrado['id_supermanzana'] == st.session_state.sm_activa]
        
        # Filtro Proximidad si hay semáforo seleccionado
        if st.session_state.sem_activo:
            target_sem = df_sem[df_sem['Identificador'] == st.session_state.sem_activo].iloc[0]
            # Filtro simple por radio cuadrado (aprox 200m)
            df_ale_sm = df_ale_sm[
                (abs(df_ale_sm['lat_val'] - target_sem['lat']) < 0.002) & 
                (abs(df_ale_sm['lon_val'] - target_sem['lon']) < 0.002)
            ]
            st.warning(f"Filtrando alertas cercanas al semáforo {st.session_state.sem_activo}")

        marker_cluster = MarkerCluster().add_to(m)
        for _, ale in df_ale_sm.iterrows():
            folium.Marker(
                location=[ale['lat_val'], ale['lon_val']],
                icon=folium.Icon(color='red' if ale['tipo'] == 'Accidente' else 'orange', icon='info-sign'),
                popup=f"Tipo: {ale['tipo']}\nSubtipo: {ale['subtipo']}\nCalle: {ale['calle']}",
                tooltip=ale['tipo']
            ).add_to(marker_cluster)

    else:
        # 2. Vista Global (Choropleth)
        m = folium.Map(location=[21.14, -86.85], zoom_start=12)
        
        folium.Choropleth(
            geo_data=geojson_sm,
            name="Choropleth",
            data=sm_stats,
            columns=["id_supermanzana", "conteo"],
            key_on="feature.id",
            fill_color="YlOrRd",
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name="Intensidad de Alertas",
            highlight=True
        ).add_to(m)
        
        # Tooltips para detección de ID
        folium.GeoJson(
            geojson_sm,
            style_function=lambda x: {'fillColor': '#ffffff00', 'color': 'gray', 'weight': 0.5},
            tooltip=folium.GeoJsonTooltip(fields=['id_supermanzana'], aliases=['Supermanzana: '])
        ).add_to(m)

    # Renderizado y captura de eventos
    map_data = st_folium(m, width=1200, height=600, key="mapa_siniestralidad")

    # --- Procesar Clicks ---
    if map_data['last_active_drawing']:
        props = map_data['last_active_drawing'].get('properties')
        if props and 'id_supermanzana' in props:
            nueva_sm = props['id_supermanzana']
            if nueva_sm != st.session_state.sm_activa:
                st.session_state.sm_activa = nueva_sm
                st.session_state.sem_activo = None
                st.rerun()

    if map_data['last_object_clicked_tooltip'] and "🚦" in map_data['last_object_clicked_tooltip']:
        nuevo_sem = map_data['last_object_clicked_tooltip'].replace("🚦 ", "").strip()
        if nuevo_sem != st.session_state.sem_activo:
            st.session_state.sem_activo = nuevo_sem
            st.rerun()

with tab_stats:
    st.subheader("Análisis de Tendencias")
    c1, c2 = st.columns(2)
    
    with c1:
        df_d = df_filtrado.groupby(df_filtrado['fecha_cierre'].dt.date).size().reset_index(name='count')
        fig1 = px.line(df_d, x='fecha_cierre', y='count', title="Histórico de Alertas", template="plotly_white")
        st.plotly_chart(fig1, use_container_width=True)
        
    with c2:
        df_t = df_filtrado['tipo'].value_counts().reset_index()
        fig2 = px.pie(df_t, names='tipo', values='count', hole=0.4, title="Distribución por Categoría")
        st.plotly_chart(fig2, use_container_width=True)

    df_sub = df_filtrado['subtipo'].value_counts().head(15).reset_index()
    fig3 = px.bar(df_sub, x='count', y='subtipo', orientation='h', title="Top 15 Subtipos Críticos", color='count')
    st.plotly_chart(fig3, use_container_width=True)

with tab_raw:
    st.subheader("Explorador de Datos")
    st.dataframe(df_filtrado, use_container_width=True)
