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
        
        # Procesar geometría PostGIS WKB a lat/lon
        if 'geometry' in df.columns:
            st.write("Decodificando geometrías espaciales de PostGIS...")
            
            latitudes = []
            longitudes = []
            validos = 0
            
            for index, row in df.iterrows():
                try:
                    # loads from hex converts PostGIS EWKB hex string to a Shapely geometry object
                    geom = wkb.loads(str(row['geometry']), hex=True)
                    # En formato típico QGIS/PostGIS (EPSG:4326), X es Longitud y Y es Latitud
                    latitudes.append(geom.y)
                    longitudes.append(geom.x)
                    validos += 1
                except Exception as e:
                    latitudes.append(None)
                    longitudes.append(None)
                    
            df['lat'] = latitudes
            df['lon'] = longitudes
            
            # Filtramos los que sí se decodificaron exitosamente
            df_mapa = df.dropna(subset=['lat', 'lon'])
            
            st.write(f"Cargados **{validos}** semáforos operables en el mapa de un total de {len(df)}.")
            
            if validos > 0:
                # Centroide aproximado
                centro_lat = df_mapa['lat'].mean()
                centro_lon = df_mapa['lon'].mean()
                
                m = folium.Map(location=[centro_lat, centro_lon], zoom_start=13)
                
                for idx, row in df_mapa.iterrows():
                    identificador = row.get('Identificador', row.get('id', 'Desconocido'))
                    popup_text = f"<b>Semaforo ID:</b> {identificador}"
                    
                    folium.Marker(
                        location=[row['lat'], row['lon']],
                        popup=folium.Popup(popup_text, max_width=300),
                        icon=folium.Icon(color='red', icon='info-sign')
                    ).add_to(m)
                    
                st_folium(m, width=1200, height=600)
            else:
                st.warning("No se logró decodificar ninguna coordenada válida.")
            
        else:
            st.warning("El dataset no contiene la columna 'geometry'. Revisar estructura:")
            st.dataframe(df.head())

    except Exception as e:
        st.error(f"Error procesando el mapa o los datos: {e}")
