# Bienvenido a la Wiki de Siniestralidad y Semáforos 🚦

¡Hola y bienvenido a la documentación técnica del proyecto de Siniestralidad Vial de Benito Juárez! 

Esta Wiki centraliza toda la información de arquitectura, los diagramas de flujos de datos y el motor de pre-procesamiento geoespacial que impulsa nuestra plataforma cimentada en **CodeIgniter4**.

Nuestro objetivo con este proyecto es unificar los registros de catastro de semáforos, alertas reportadas por participación ciudadana (API de Waze) y la geometría urbana de las Supermanzanas para crear plataformas operativas ultrarrápidas y tableros interactivos para la toma de decisiones, garantizando rendimientos instantáneos sin sacrificar la profundidad de la data.

---

## Índice de Contenidos

Para facilitarte la lectura y entendimiento de nuestro entorno de trabajo, hemos dividido la wiki en las siguientes secciones (puedes navegar a ellas en el menú lateral):

### 1. [Arquitectura de Datos y ETL](Architecture)
Entiende a vista de pájaro cómo fluyen los datos. En este documento conocerás el esquema base, cómo superamos las limitaciones de geo-procesamiento en la nube usando `Shapely R-Trees` en Python y los diagramas visuales del pipeline implementado.

### 2. [Vistas Optimizadas y Base de Datos](DB-Transformations)
Guía técnica sobre la base de datos `semaforos_PT` en PostgreSQL. Aquí se especifican la composición de nuestras vistas materializadas, tablas físicas, diccionarios en español cargados de manera nativa (vía funciones DDL) y por qué las alertas del dashboard cargan en milisegundos gracias a la infraestructura de indexación temporal y espacial.

---

## ¿A quién va dirigida esta Wiki?
- **Desarrolladores Back-End:** Para comprender cómo extender o importar capas en nuestro esquema, o añadir nuevas lógicas geoespaciales a las `supermanzanas`.
- **Analistas de Datos:** Podrán leer de dónde surgen campos como `duracion_horas` y comprender la fiabilidad que provee nuestro agrupamiento de alertas continuas ("Gaps and Islands").
- **Mantenedores CodeIgniter4:** Para saber a qué vistas exactas tiene que llamar el modelo MVC de CodeIgniter4 (nunca a las tablas en crudo) y qué nombre llevan exactamente las variables y sus convenciones semánticas.

Si tienes alguna duda o encuentras detalles a mejorar en este documento, siéntete libre de proponer modificaciones en los Pull Requests o actualizar directamente la wiki.

¡Gracias por formar parte de los esfuerzos por la mejora vial en Cancún!
