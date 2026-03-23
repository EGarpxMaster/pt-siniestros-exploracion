import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Dashboard Global", page_icon="📊", layout="wide")
st.title("📊 Dashboard de Semaforización")
st.markdown("Análisis combinado de alertas y estado de semáforos en la red PT.")

data_dir = os.path.join("data", "semaforos_PT")

path_alertas = os.path.join(data_dir, "alertas.csv")
path_historico = os.path.join(data_dir, "alertas_historico.csv")

col1, col2 = st.columns(2)

# Gráfica 1: Alertas Actuales
with col1:
    st.subheader("🚨 Resumen de Alertas Actuales")
    if os.path.exists(path_alertas):
        try:
            df_alertas = pd.read_csv(path_alertas)
            if 'descripcion' in df_alertas.columns:
                conteo = df_alertas['descripcion'].value_counts()
                if not conteo.empty:
                    st.bar_chart(conteo)
                else:
                    st.info("No hay descripciones válidas para graficar.")
            else:
                st.info("El dataset no contiene la columna 'descripcion' para generar métricas automáticas.")
        except Exception as e:
            st.error(f"Error al leer alertas: {e}")
    else:
        st.info("Sin datos actuales.")

# Gráfica 2: Top Fallas Históricas
with col2:
    st.subheader("📉 Top 10 Eventos Históricos")
    if os.path.exists(path_historico):
        try:
            # Tomamos una muestra para no sobrecargar
            df_hist = pd.read_csv(path_historico, nrows=50000)
            if 'descripcion' in df_hist.columns:
                conteo_hist = df_hist['descripcion'].value_counts().head(10)
                if not conteo_hist.empty:
                    st.bar_chart(conteo_hist)
                else:
                    st.info("No hay descripciones en el histórico.")
            else:
                st.info("El dataset no contiene la columna 'descripcion'.")
        except Exception as e:
            st.error(f"Error al leer histórico: {e}")
    else:
        st.info("Sin datos históricos.")
