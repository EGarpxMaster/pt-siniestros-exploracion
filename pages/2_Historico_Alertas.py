import streamlit as st
import pandas as pd
import folium
from folium.plugins import HeatMap
from streamlit_folium import st_folium
import plotly.express as px
import os

st.set_page_config(page_title="Deep Dive - Histórico de Alertas", page_icon="📉", layout="wide")
st.title("📉 Análisis Profundo: Histórico de Alertas")
st.markdown("Estudio detallado de factores de siniestralidad, análisis temporal, categórico y geográfico basándonos en el `alertas_historico.csv`.")

data_path = os.path.join("data", "semaforos_PT", "alertas_historico.csv")

# Diccionario para meses en español
meses_es = {
    'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
    'jul': '07', 'ago': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'
}

@st.cache_data
def load_and_process_data(filepath, limit=50000):
    df = pd.read_csv(filepath, nrows=limit)
    
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

    return df

if not os.path.exists(data_path):
    st.info("El archivo `alertas_historico.csv` no se encuentra.")
else:
    try:
        limite = st.sidebar.slider("Registros a analizar (rendimiento):", 1000, 150000, 25000, 5000)
        with st.spinner("Procesando histórico de alertas..."):
            df = load_and_process_data(data_path, limite)
            
        st.write(f"### Análisis Dinámico sobre los primeros **{len(df)}** registros.")
        
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📅 Análisis Temporal", "📊 Análisis Categórico", "🔥 Mapa de Calor", "🗂 Datos Procesados", "🧮 Correlación"])
        
        with tab1:
            st.subheader("Distribución Temporal de los Eventos")
            col_t1, col_t2 = st.columns(2)
            
            with col_t1:
                if 'Mes' in df.columns:
                    conteo_mes = df['Mes'].value_counts().reset_index()
                    conteo_mes.columns = ['Mes', 'Cantidad']
                    fig_mes = px.bar(conteo_mes, x='Mes', y='Cantidad', title="Eventos por Mes", color='Cantidad', color_continuous_scale='Blues')
                    st.plotly_chart(fig_mes, use_container_width=True)
            
            with col_t2:
                if 'Dia_Semana' in df.columns:
                    conteo_dia = df.groupby(['Dia_Semana', 'Orden_Dia']).size().reset_index(name='Cantidad')
                    conteo_dia = conteo_dia.sort_values('Orden_Dia')
                    fig_dia = px.bar(conteo_dia, x='Dia_Semana', y='Cantidad', title="Eventos por Día de la Semana", color='Cantidad', color_continuous_scale='Oranges')
                    st.plotly_chart(fig_dia, use_container_width=True)
                    
        with tab2:
            st.subheader("Tipología y Clasificación de Alertas")
            col_c1, col_c2 = st.columns(2)
            
            with col_c1:
                if 'Type' in df.columns:
                    conteo_tipo = df['Type'].value_counts().reset_index()
                    conteo_tipo.columns = ['Tipo', 'Cantidad']
                    fig_tipo = px.pie(conteo_tipo, values='Cantidad', names='Tipo', title="Proporción por Tipo General", hole=0.4)
                    st.plotly_chart(fig_tipo, use_container_width=True)
            
            with col_c2:
                if 'Subtype' in df.columns:
                    conteo_subt = df['Subtype'].dropna().value_counts().head(10).reset_index()
                    conteo_subt.columns = ['Subtipo', 'Cantidad']
                    fig_subt = px.bar(conteo_subt, x='Cantidad', y='Subtipo', orientation='h', title="Top 10 Subtipos Específicos", color='Cantidad', color_continuous_scale='Reds')
                    fig_subt.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig_subt, use_container_width=True)
                    
            if 'Street' in df.columns:
                st.write("### Calles con Mayor Siniestralidad y Problemas")
                conteo_calles = df['Street'].dropna().value_counts().head(15).reset_index()
                conteo_calles.columns = ['Calle / Avenida', 'Cantidad']
                fig_calles = px.bar(conteo_calles, x='Calle / Avenida', y='Cantidad', title="Top 15 Vías Afectadas", color='Cantidad', color_continuous_scale='Purples')
                st.plotly_chart(fig_calles, use_container_width=True)
                
        with tab3:
            st.subheader("Mapa de Calor de Siniestralidad y Alertas")
            st.markdown("Identifica los focos rojos y zonas de mayor concentración de problemas basándote en un mapeo de intensidad.")
            
            if 'lat' in df.columns and 'lon' in df.columns:
                df_heat = df.dropna(subset=['lat', 'lon'])
                if not df_heat.empty:
                    # Folium se congela si hay demasiados puntos. Tomamos una muestra aleatoria para pintar.
                    max_points = 3000
                    if len(df_heat) > max_points:
                        st.info(f"Mostrando una muestra representativa de {max_points} puntos (de {len(df_heat)}) para optimizar el rendimiento del mapa.")
                        df_heat = df_heat.sample(max_points, random_state=42)
                    
                    centro_lat = df_heat['lat'].mean()
                    centro_lon = df_heat['lon'].mean()
                    
                    m_heat = folium.Map(location=[centro_lat, centro_lon], zoom_start=13)
                    heat_data = df_heat[['lat', 'lon']].values.tolist()
                    
                    HeatMap(heat_data, radius=15, blur=10).add_to(m_heat)
                    
                    st_folium(m_heat, width=1200, height=600, returned_objects=[])
                else:
                    st.info("No hay coordenadas válidas para dibujar el mapa. Verifica las columnas de Location.")
            else:
                st.info("Es necesario procesar correctamente la columna Location para desplegar el mapa de calor.")
                
        with tab4:
            st.subheader("Datos Procesados En Bruto")
            st.write("Vista a nivel de registro tras extraer fechas, latitudes y longitudes.")
            # Ocultamos variables creadas solo para cálculos
            st.dataframe(df.drop(columns=['Orden_Dia', 'Es_Accidente', 'Mes_Num'], errors='ignore'), use_container_width=True)

        with tab5:
            st.subheader("Matriz de Correlación: Siniestros vs Contexto")
            st.markdown("Analiza la relación matemática entre las variables. Observamos qué factores están más vinculados matemáticamente con que un evento sea un **Accidente**.")
            
            # Buscar que existan nuestras variables numéricas
            cols_req = ['Es_Accidente', 'Mes_Num', 'Orden_Dia', 'lat', 'lon']
            cols_disp = [c for c in cols_req if c in df.columns]
            
            if len(cols_disp) > 1:
                df_corr = df[cols_disp].dropna()
                if not df_corr.empty and df_corr['Es_Accidente'].nunique() > 1:
                    # Renombrar columnas para lectura amigable
                    rename_dict = {
                        'Es_Accidente': 'Es Accidente (0/1)',
                        'Mes_Num': 'Mes',
                        'Orden_Dia': 'Día de la Semana',
                        'lat': 'Latitud',
                        'lon': 'Longitud'
                    }
                    df_corr = df_corr.rename(columns=rename_dict)
                    
                    matriz_corr = df_corr.corr()
                    fig_corr = px.imshow(matriz_corr, text_auto=".2f", aspect="auto", 
                                         color_continuous_scale='RdBu_r', 
                                         title="Coeficientes de Correlación de Pearson")
                    st.plotly_chart(fig_corr, use_container_width=True)
                    
                    st.info("💡 **Cómo interpretar:** Valores cerca de **1** significan una alta relación directa. Cerca de **-1** alta relación inversa. Valores cercanos a **0** significan nula correlación lineal.")
                else:
                    st.warning("No hay suficientes variaciones en los datos para trazar una matriz (se requieren al menos incidentes de tipos mixtos).")
            else:
                st.warning("Faltan variables numéricas para calcular la correlación.")

    except Exception as e:
        st.error(f"Error procesando el histórico: {e}")
