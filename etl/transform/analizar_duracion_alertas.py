import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

def process_alert_durations(output_csv="data/eventos_alertas_duracion.csv"):
    print("⏳ Conectando a la base de datos...")
    load_dotenv()
    
    encoded_pwd = urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))
    engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{encoded_pwd}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}")
    
    # Extraer una muestra significativa para generar la prueba (ej: últimas 300,000 alertas)
    # Seleccionaremos de marzo 2026 por el ejemplo que proporcionaste
    print("⏳ Extrayendo muestra de alertas de marzo de 2026 (puede tardar un momento)...")
    query = """
        SELECT id_registro, subtipo, calle, fecha, longitud, latitud 
        FROM "semaforos_PT"."alertas"
        WHERE fecha >= '2026-03-01' AND fecha < '2026-04-01'
        ORDER BY fecha ASC
    """
    
    try:
        df = pd.read_sql_query(query, engine)
        print(f"✅ Se obtuvieron {len(df)} alertas.")
    except Exception as e:
        print(f"❌ Error al consultar PostgreSQL: {e}")
        return
    
    if df.empty:
        print("La muestra está vacía. Abortando.")
        return

    print("⏳ Procesando agrupaciones espaciales y de tiempo...")
    
    # Asegurar que la fecha es datetime con UTC
    df['fecha_dt'] = pd.to_datetime(df['fecha'], utc=True)
    
    # Redondeamos coordenadas a 3 decimales (Aprox. 110 metros de precisión para hacer clusters del mismo bache/choque)
    df['lat_cluster'] = df['latitud'].round(3)
    df['lon_cluster'] = df['longitud'].round(3)
    
    # Ordenamos el DataFrame de manera óptima para calcular los deltas de tiempo
    df = df.sort_values(by=['subtipo', 'lat_cluster', 'lon_cluster', 'fecha_dt'])
    
    # Agrupamos por las características de "identidad del evento"
    # El mismo subtipo, en la misma cuadrícula de 100m.
    groupings = ['subtipo', 'lat_cluster', 'lon_cluster']
    
    # Calculamos la diferencia de tiempo respecto a la alerta anterior del mismo grupo
    df['time_diff'] = df.groupby(groupings)['fecha_dt'].diff()
    
    # Si la diferencia entre alertas es menor a X horas, consideramos que es el mismo evento.
    # Por ejemplo, si reportan otro "Bache" con 6 horas de diferencia en la misma esquina, sumamos al evento.
    # Si pasan más de 12 horas sin rastro, lo cortamos (asume reparado o disipado).
    TIME_THRESHOLD = pd.Timedelta(hours=12)
    
    # Identificamos el inicio de un 'Nuevo Evento' si es la primera alerta del grupo o pasó demasiado tiempo
    df['is_new_event'] = (df['time_diff'].isnull()) | (df['time_diff'] > TIME_THRESHOLD)
    
    # Asignamos un ID incremental a cada evento único mediante suma acumulada
    df['evento_id'] = df['is_new_event'].cumsum()
    
    print("⏳ Agregando duraciones...")
    
    # Ahora que cada alerta pertenece a un 'evento_id', agregamos las métricas
    eventos_df = df.groupby('evento_id').agg(
        subtipo=('subtipo', 'first'),
        calle_principal=('calle', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        latitud_aprox=('latitud', 'mean'),
        longitud_aprox=('longitud', 'mean'),
        primera_alerta=('fecha_dt', 'min'),
        ultima_alerta=('fecha_dt', 'max'),
        total_reportes=('id_registro', 'count')
    ).reset_index()
    
    # Calcular la duración
    eventos_df['duracion_evento'] = eventos_df['ultima_alerta'] - eventos_df['primera_alerta']
    
    # Formatear la duración a texto o simplemente redondear a horas o minutos para CSV
    eventos_df['duracion_horas'] = eventos_df['duracion_evento'].dt.total_seconds() / 3600.0
    
    # Opcional: Filtramos los eventos de "0 horas" (reportes aislados 1 sola vez) para que el CSV no sea gigante 
    # y te muestre de verdad eventos prolongados.
    # eventos_df = eventos_df[eventos_df['total_reportes'] > 1]
    
    # Ordenamos por los eventos que más duraron (para ver anomalías y cosas interesantes)
    eventos_df = eventos_df.sort_values(by='duracion_evento', ascending=False)
    
    # Guardamos CSV
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    eventos_df.to_csv(output_csv, index=False)
    
    print(f"✅ Análisis completado. Se generaron {len(eventos_df)} eventos únicos (duraciones calculadas). CSV exportado en: {output_csv}")
    
    # Mostrar una muestra en consola
    print("\n--- Top 5 Eventos más prolongados ---")
    print(eventos_df[['subtipo', 'calle_principal', 'total_reportes', 'duracion_horas']].head(5))

if __name__ == "__main__":
    process_alert_durations()
