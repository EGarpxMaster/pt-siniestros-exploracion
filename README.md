# Explorador de Siniestralidad y Semáforos (ETL & Data Sandbox)

Este repositorio aloja los componentes del backend y los procesos DDL/Espaciales para la plataforma de siniestralidad vial en Benito Juárez. Aquí se realizan los flujos de extracción, transformación y agregación de grandes volúmenes de datos geoespaciales provenientes de la API de Waze y censos urbanos, habilitando datos listos para consumo web sin depender de costosas ejecuciones en tiempo de usuario.

**Nota técnica**: Este entorno usa herramientas en Python para superar la falta de la extensión PostGIS nativa en el servidor en la nube, pre-procesando polígonos y puntos vectoriales en memoria local antes de subirlos a producción en la base de datos PostgreSQL.

## Arquitectura de Archivos

Se ha implementado un patrón de arquitectura **ETL** para su fácil mantenimiento general conforme el catálogo de datos aumente o lleguen futuros periodos históricos:

*   `etl/extract/`: Scripts (`extract_data.py`, etc.) dedicados única y exclusivamente a descargar **información segura** de vistas específicas para volcarlos en archivos locales en bruto (`.csv`). No alteran la base ni realizan transformaciones transversales, ideales para refrescar el backend.
*   `etl/transform/`: Contiene el núcleo geo-espacial (`aplicar_alertas_supermanzanas.py`, `aplicar_fase3_pg.py`). Estos archivos toman datos crudos, aplican diccionarios de traducciones nativas y algoritmos de R-Tree (`STRtree`) para inferir relaciones de supermanzanas. Empujan los esquemas estáticos nuevamente a Postgres.
*   `etl/load/`: Reservada para inyecciones crudas de repositorios primarios (en desarrollo o futuras bases).
*   `data/`: Data en bruto producida por `extract` como CSV o geojson por si requieres uso desconectado o prototipado vía Sandbox.
*   `test/`: Sandboxes (Streamlit), experimentación de logs residuales en `test/logs` orientados a documentación pre-producción (CodeIgniter4).

## Inicio Rápido

1.  **Variables de Entorno**: Configura tu `.env` con las variables a tu PostgreSQL (`DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_NAME`).
2.  **Dependencias**: Instala las herramientas geoespaciales requeridas vía consola:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Refrescar Vistas Optimizadas Espacialmente**:
    Si la base recibe nuevos lotes inmensos de semáforos, alertas o histórico, corre en secuencia las cargas de cruces matriciales:
    ```bash
    python etl/transform/aplicar_alertas_pg.py
    python etl/transform/aplicar_fase3_pg.py
    python etl/transform/aplicar_alertas_supermanzanas.py
    ```
4.  **Expulsar a Sistema de Archivos**:
    Si requiere hacer copias locales, corre `python etl/extract/extract_data.py`.

## Documentación Adicional

La documentación exhaustiva sobre los diagramas lógicos, árboles de dependencias, scripts de diccionario (Traducción In-line PostgreSQL) e indexado físico de cada vista materializada se puede consultar íntegramente en la **Wiki del repositorio** de GitHub. Para saber detalles arquitectónicos puntuales a la hora de migrar a CI4, remítase a la Wiki.
