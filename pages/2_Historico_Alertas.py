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
        coords = df['Location'].str.extract(r'Point\(([-.\d]+)\s+([-.\d]+)\)')
        df['lon'] = coords[0].astype(float)
        df['lat'] = coords[1].astype(float)

    return df

if not os.path.exists(data_path):
    st.info("El archivo `alertas_historico.csv` no se encuentra.")
else:
    try:
        limite = st.sidebar.slider("Registros a analizar (rendimiento):", 1000, 150000, 25000, 5000)
        with st.spinner("Procesando histórico de alertas..."):
            df = load_and_process_data(data_path, limite)
            
        st.write(f"### Análisis Dinámico sobre los primeros **{len(df)}** registros.")
        
        tab1, tab2, tab3, tab4 = st.tabs(["📅 Análisis Temporal", "📊 Análisis Categórico", "🔥 Mapa de Calor", "🗂 Datos Procesados"])
        
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
                    centro_lat = df_heat['lat'].mean()
                    centro_lon = df_heat['lon'].mean()
                    
                    m_heat = folium.Map(location=[centro_lat, centro_lon], zoom_start=13)
                    heat_data = [[row['lat'], row['lon']] for index, row in df_heat.iterrows()]
                    
                    HeatMap(heat_data, radius=15, blur=10).add_to(m_heat)
                    
                    st_folium(m_heat, width=1200, height=600)
                else:
                    st.info("No hay coordenadas válidas para dibujar el mapa. Verifica las columnas de Location.")
            else:
                st.info("Es necesario procesar correctamente la columna Location para desplegar el mapa de calor.")
                
        with tab4:
            st.subheader("Datos Procesados En Bruto")
            st.write("Vista a nivel de registro tras extraer fechas, latitudes y longitudes.")
            st.dataframe(df.drop(columns=['Orden_Dia'], errors='ignore'), use_container_width=True)

    except Exception as e:
        st.error(f"Error procesando el histórico: {e}")
