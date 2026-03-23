import streamlit as st

st.set_page_config(
    page_title="Dashboard Semáforos PT",
    page_icon="🚦",
    layout="wide"
)

st.title("🚦 Exploración y Análisis - Semáforos PT")

st.markdown("""
Bienvenido a la aplicación de exploración analítica.

Debido a la disponibilidad de datos, esta herramienta se enfoca exclusivamente en el esquema **Semaforos_PT**, analizando la infraestructura semafórica y las alertas registradas en el municipio.

---

### 🗂 Secciones Disponibles

Navega utilizando el menú lateral a la izquierda:

*   **Página 1 - Alertas Actuales**: Visualización rápida de los incidentes y reportes de estado actual de los semáforos, tales como fallas, baches o encharcamientos reportados.
*   **Página 2 - Histórico de Alertas**: Herramienta de filtrado y exploración profunda sobre el histórico de siniestros, fallas y eventos para encontrar patrones de siniestralidad.
*   **Página 3 - Mapa de Semáforos**: Vista geográfica interactiva de la red de semáforos. Podrás ubicar cada dispositivo y conocer sus coordenadas exactas.
*   **Página 4 - Dashboard Global**: Resumen analítico con gráficas clave sobre el funcionamiento de la red y las interrupciones más frecuentes.

---
""")
