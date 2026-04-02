import streamlit as st
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
import os
from shapely import wkb
import json

st.set_page_config(page_title="Mapa de Semáforos y Alertas - SISV", page_icon="🚦", layout="wide")
st.title("🚦 Mapa Interactivo de Infraestructura y Alertas")
st.markdown("Monitor de red semafórica y eventos de tráfico en tiempo casi-real por Supermanzana.")

# --- Rutas de Datos ---
DATA_DIR = os.path.join("data", "semaforos_PT")
ALERTAS_PATH = os.path.join(DATA_DIR, "alertas.csv")
SEMAFOROS_PATH = os.path.join(DATA_DIR, "semaforos.csv")
SUPERMANZANAS_PATH = os.path.join(DATA_DIR, "supermanzanas.csv")

# --- Gestión de Estado ---
if 'sm_activa_sem' not in st.session_state:
    st.session_state.sm_activa_sem = None

@st.cache_data
def load_data():
    df_ale = pd.read_csv(ALERTAS_PATH) if os.path.exists(ALERTAS_PATH) else pd.DataFrame()
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
                    "properties": {"id_supermanzana": str(row['id_supermanzana'])},
                    "geometry": geom.__geo_interface__
                }
                geojson_sm["features"].append(feature)
            except: continue
            
    return df_ale, df_sem, df_sm, geojson_sm

def reset_view_sem():
    st.session_state.sm_activa_sem = None

# --- Carga ---
df_ale, df_sem, df_sm, geojson_sm = load_data()

# --- Sidebar ---
st.sidebar.header("Opciones de Mapa")
st.sidebar.button("Reiniciar Vista Global 🌎", on_click=reset_view_sem)
st.sidebar.info("Este mapa muestra las alertas activas (últimos reportes) y la ubicación técnica de los semáforos.")

# --- Cuerpo Principal ---
if df_sm.empty or df_sem.empty:
    st.error("Faltan archivos de datos necesarios para el mapa.")
    st.stop()

# Calcular densidad por SM basándose en alertas RECIENTES
sm_stats = df_ale.groupby('id_supermanzana').size().reset_index(name='conteo')

if st.session_state.sm_activa_sem:
    st.success(f"📍 Detalle de Supermanzana: {st.session_state.sm_activa_sem}")
    centro_sm = df_sm[df_sm['id_supermanzana'] == st.session_state.sm_activa_sem].iloc[0]
    m = folium.Map(location=[centro_sm['lat_centroide'], centro_sm['lon_centroide']], zoom_start=15)
    
    # Capa de Semáforos (Icono 🚦)
    df_sem_sm = df_sem[df_sem['id_supermanzana'] == st.session_state.sm_activa_sem]
    for _, sem in df_sem_sm.iterrows():
        folium.Marker(
            location=[sem['lat'], sem['lon']],
            popup=f"<b>{sem['Identificador']}</b><br>{sem['ubicacion']}",
            icon=folium.DivIcon(html='<div style="font-size: 24px;">🚦</div>', icon_anchor=(12, 12))
        ).add_to(m)
        
    # Capa de Alertas Recientes de esa SM
    df_ale_sm = df_ale[df_ale['id_supermanzana'] == st.session_state.sm_activa_sem]
    marker_cluster = MarkerCluster().add_to(m)
    for _, ale in df_ale_sm.iterrows():
        folium.Marker(
            location=[ale['latitud_aprox'], ale['longitud_aprox']],
            icon=folium.Icon(color='red' if ale['tipo'] == 'Accidente' else 'blue', icon='info-sign'),
            popup=f"<b>{ale['tipo']}</b><br>{ale['subtipo']}<br><i>{ale['calle_principal']}</i>",
            tooltip=ale['tipo']
        ).add_to(marker_cluster)

else:
    # Vista Global con Choropleth
    st.info("💡 Haz click en una Supermanzana para inspeccionar su red de semáforos y alertas.")
    m = folium.Map(location=[21.145, -86.85], zoom_start=12)
    
    # Choropleth de intensidad de alertas
    folium.Choropleth(
        geo_data=geojson_sm,
        name="densidad_alertas",
        data=sm_stats,
        columns=["id_supermanzana", "conteo"],
        key_on="feature.id",
        fill_color="YlOrRd",
        fill_opacity=0.6,
        line_opacity=0.2,
        legend_name="Alertas Recientes por SM",
        highlight=True
    ).add_to(m)
    
    # Capa de detección de clicks
    folium.GeoJson(
        geojson_sm,
        style_function=lambda x: {'fillColor': '#ffffff00', 'color': 'gray', 'weight': 0.5},
        tooltip=folium.GeoJsonTooltip(fields=['id_supermanzana'], aliases=['Supermanzana: '])
    ).add_to(m)

# Renderizado
map_data = st_folium(m, width=1200, height=650, key="mapa_infra")

# --- Lógica de Click ---
if map_data['last_active_drawing']:
    props = map_data['last_active_drawing'].get('properties')
    if props and 'id_supermanzana' in props:
        nueva_sm = props['id_supermanzana']
        if nueva_sm != st.session_state.sm_activa_sem:
            st.session_state.sm_activa_sem = nueva_sm
            st.rerun()
