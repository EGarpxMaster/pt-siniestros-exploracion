import streamlit as st
import pandas as pd
import os

def render_schema_explorer(esquema: str, titulo: str):
    st.title(f"Esquema: {titulo}")
    st.markdown(f"Exploración de datos locales para el esquema **{esquema}**.")

    data_dir = os.path.join("data", esquema)

    if not os.path.exists(data_dir):
        st.warning(f"No se encontró el directorio de datos para el esquema '{esquema}'. Es posible que no se hayan extraído los datos o hubo un error de permisos en la base de datos.")
        return

    archivos_csv = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    if not archivos_csv:
        st.info(f"El directorio '{data_dir}' existe pero no contiene archivos CSV. Verifica si las tablas estaban vacías o no se pudieron extraer.")
        return

    nombres_tablas = [f.replace('.csv', '') for f in archivos_csv]
    
    tabla_seleccionada = st.selectbox("1. Selecciona una Tabla para explorar:", ["(Selecciona una tabla...)"] + sorted(nombres_tablas))
    
    if tabla_seleccionada != "(Selecciona una tabla...)":
        col1, col2 = st.columns([1, 2])
        with col1:
            limite = st.number_input("Cantidad de registros a visualizar:", min_value=1, max_value=50000, value=50, step=10)
        
        with col2:
            st.write("")
            st.write("")
            cargar = st.button("Cargar Datos 🚀")
        
        if cargar:
            archivo_path = os.path.join(data_dir, f"{tabla_seleccionada}.csv")
            with st.spinner(f"Cargando datos desde {archivo_path}..."):
                try:
                    df = pd.read_csv(archivo_path, nrows=limite)
                    
                    if df.empty:
                        st.info("La tabla está vacía. No hay registros para mostrar.")
                    else:
                        st.subheader(f"Vista previa: `{esquema}.{tabla_seleccionada}`")
                        st.dataframe(df, use_container_width=True)
                        
                        with st.expander("Ver estructura de columnas (Tipos de datos)"):
                            info_df = pd.DataFrame({
                                "Columna": df.columns,
                                "Tipo de Dato (Pandas)": df.dtypes.astype(str)
                            }).reset_index(drop=True)
                            st.dataframe(info_df, use_container_width=True)
                            st.caption(f"Total de columnas: {len(df.columns)}")
                except Exception as e:
                    st.error(f"Error al leer el archivo CSV:\n{e}")
