import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import os
from shapely import wkb

st.set_page_config(page_title="Mapa Geográfico", page_icon="🗺️", layout="wide")
st.title("🗺️ Mapa Interactivo de Semáforos")
st.markdown("Visualización geográfica de los dispositivos de la red semafórica extraídos de PostGIS.")

data_path = os.path.join("data", "semaforos_PT", "semaforos.csv")

if not os.path.exists(data_path):
    st.info("El archivo `semaforos.csv` no se encuentra en `data/semaforos_PT/`. Asegúrate de haber ejecutado la extracción de datos.")
else:
    try:
        df = pd.read_csv(data_path)
        
        if 'lat' in df.columns and 'lon' in df.columns:
            df_mapa = df.dropna(subset=['lat', 'lon'])
            validos = len(df_mapa)
            
            st.write(f"Cargados **{validos}** semáforos operables en el mapa de un total de {len(df)}.")
            
            if validos > 0:
                # Centroide aproximado
                centro_lat = df_mapa['lat'].mean()
                centro_lon = df_mapa['lon'].mean()
                
                m = folium.Map(location=[centro_lat, centro_lon], zoom_start=13)
                
                for idx, row in df_mapa.iterrows():
                    identificador = row.get('Identificador', row.get('id', 'Desconocido'))
                    ubicacion = row.get('ubicacion', 'Ubicación no disponible')
                    popup_text = f"<b>Semaforo ID:</b> {identificador}<br><br><b>Ubicación:</b> {ubicacion}"
                    
                    folium.Marker(
                        location=[row['lat'], row['lon']],
                        popup=folium.Popup(popup_text, max_width=300),
                        icon=folium.DivIcon(
                            html='<div style="font-size: 24px;">🚦</div>',
                            icon_anchor=(12, 12)
                        )
                    ).add_to(m)
                    
                st_folium(m, width=1200, height=600)
            else:
                st.warning("No se logró localizar ninguna coordenada válida.")
            
        else:
            st.warning("El dataset no contiene las columnas preprocesadas 'lat' y 'lon'. Revisar estructura:")
            st.dataframe(df.head())

    except Exception as e:
        st.error(f"Error procesando el mapa o los datos: {e}")
