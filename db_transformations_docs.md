# Documentación de Transformaciones de Base de Datos

Este documento describe las vistas, vistas materializadas y transformaciones de datos que hemos implementado en la base de datos PostgreSQL (`semaforos_PT`) para optimizar las consultas y visualizaciones en el dashboard de siniestros y semáforos.

---

## 1. Vista Materializada: Eventos de Alertas Agrupados
**Nombre:** `mv_alertas`
**Script Origen:** `aplicar_alertas_pg.py`

Esta vista procesa la tabla cruda de `alertas` (generada por la API de Waze/WKT) para realizar dos tareas fundamentales:
1. **Traducción Nativa:** Utiliza funciones de base de datos (`CASE WHEN`) para traducir los campos `tipo` y `subtipo` al idioma español (ej. de `JAM_HEAVY_TRAFFIC` a `Tráfico Pesado`).
2. **Agrupación de Eventos Temporales:** Emplea funciones analíticas o de ventana (`LAG`, `OVER`, `PARTITION BY`) para agrupar alertas recurrentes en el mismo punto geográfico y de la misma categoría que ocurren con menos de 12 horas de diferencia. Esto reduce la duplicidad y consolida los avisos en **eventos** discretos, permitiendo contar la duración y la cantidad de reportes por incidente.

### Índices Generados:
- `idx_mv_alertas_ev_coords` (sobre latitud_aprox, y longitud_aprox)
- `idx_mv_alertas_ev_fecha` (sobre la primera de las fechas del evento)

---

## 2. Vistas Físicas Optimizadas Espacialmente

Debido a la ausencia de extensión nativa de PostGIS en el motor PostgreSQL actual, las uniones y cálculos espaciales se procesaron en memoria con Python (Pandas y Shapely) de forma transaccional y se volvieron a subir a la base de datos como tablas planas indexadas.

**Scripts Origen:** `etl/transform/aplicar_fase3_pg.py` y `etl/transform/aplicar_alertas_supermanzanas.py`

### A. Supermanzanas
**Nombre:** `vw_supermanzanas`
Se leyó el binario geográfico (WKB) de la tabla local `supermanzanas` y se calculó el **centroide** de cada polígono.
- **Campos principales:** `id_supermanzana`, `pobtot`, `lat_centroide`, `lon_centroide`
- **Uso:** Facilita el paneo del mapa a ubicaciones rápidas en la interfaz sin tener que sobrecargar la transferencia con geometrías completas.

### B. Semáforos con Supermanzanas Asignadas
**Nombre:** `vw_semaforos`
Se unieron espacialmente (`spatial join` con distancias mínimas) los puntos de los semáforos con el polígono de las supermanzanas asociadas.
- **Campos principales:** `id`, `Identificador`, `ubicacion`, `id_supermanzana`, `lat`, `lon`

### C. Alertas (Eventos e Histórico) con Supermanzanas Asignadas
**Nombre:** `vw_alertas` y `vw_alertas_historico`
Mediante el uso del índice espacial R-Tree (`STRtree` de Shapely) en Python, cruzamos al instante decenas de miles de eventos de alertas (actuales y pasadas) con sus respectivas supermanzanas, ahorrando millones de interacciones dinámicas en la plataforma.
- **Campos principales:** `id_supermanzana`, sumado a sus respectivos campos base.
- **Uso de ambas y B:** Permite mostrar capas de siniestralidad enriquecidas y sumamente rápidas usando los campos numéricos indexados (`lat`, `lon` y agrupaciones SQL por `id_supermanzana`) sin depender del motor geoespacial en tiempo de ejecución de usuarios.

---

## 3. Vista Materializada: Histórico de Alertas
**Nombre:** `mv_alertas_historico`
**Script Origen:** `etl/transform/aplicar_fase3_pg.py`

Esta vista pre-calcula y limpia los tipos de datos engorrosos de la tabla cruda `alertas_historico`:

1. **Traducción de Tipos:** Emplea `CASE WHEN` para traducir a español, generando las columnas `tipo` y `subtipo`.
2. **Re-estructuración de Coordenadas:** Utiliza Expresiones Regulares para extraer de `POINT(LON LAT)` las columnas `lon_val` y `lat_val`.
3. **Parseo Nativo de Textos a Fechas:** Implementa limpieza manual inyectada en `TO_DATE()` y re-cast a `TIMESTAMP`, lo cual otorga a CodeIgniter4 datos indexados de serie de tiempo.

### Índices Generados:
- `idx_hist_coords` (sobre lat_val, lon_val)
- `idx_hist_fecha` (sobre fecha_cierre)

---

## Resumen de Nomenclatura de Vistas

| Vista/Tabla | Tipo | Descripción |
| :--- | :--- | :--- |
| `mv_alertas` | Materializada | Eventos de alertas agrupados con traducciones |
| `mv_alertas_historico` | Materializada | Histórico de alertas limpiado y traducido |
| `vw_alertas` | Física (tabla) | Eventos con supermanzana asignada |
| `vw_alertas_historico` | Física (tabla) | Histórico con supermanzana asignada |
| `vw_semaforos` | Física (tabla) | Semáforos con supermanzana asignada |
| `vw_supermanzanas` | Física (tabla) | Supermanzanas con centroides |

---

## Resumen de Beneficios
- **Reducción de Tiempo de Cómputo Local:** Las operaciones espaciales pesadas (`R-Tree` Spatial Join) se realizaron una vez y quedaron empaquetadas como flotantes.
- **Rápida Respuesta de Filtros en UI:** Los índices de cruce territorial, coordenadas y fechas agilizan las peticiones de los filtros territoriales de CodeIgniter4 notablemente.
- **Compatibilidad del Servidor Limitado:** Al pre-procesar en scripts, sobrellevamos la limitación de la falta de un plug-in nativo GIS (PostGIS) en el motor en la nube.
