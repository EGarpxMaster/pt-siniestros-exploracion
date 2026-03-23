import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Alertas Actuales", page_icon="⚠️", layout="wide")
st.title("⚠️ Alertas Actuales")
st.markdown("Revisión de la tabla `alertas.csv` del esquema Semaforos_PT.")

data_path = os.path.join("data", "semaforos_PT", "alertas.csv")

if not os.path.exists(data_path):
    st.info("El archivo `alertas.csv` no se encuentra. Asegúrate de haber ejecutado la extracción de datos.")
else:
    try:
        df = pd.read_csv(data_path)
        st.write(f"**Total de alertas registradas:** {len(df)}")
        
        # Filtros básicos
        if 'descripcion' in df.columns:
            tipos = df['descripcion'].dropna().unique().tolist()
            filtro_tipo = st.selectbox("Filtrar por tipo de alerta:", ["Todos"] + tipos)
            if filtro_tipo != "Todos":
                df = df[df['descripcion'] == filtro_tipo]
                
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error al leer el archivo: {e}")
