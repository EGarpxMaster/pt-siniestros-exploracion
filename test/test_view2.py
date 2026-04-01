"""
Test de transformaciones para alertas — valida con 1000 filas antes de la extracción completa.
"""
import os, urllib.parse, pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:"
    f"{urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))}@"
    f"{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}"
)

df = pd.read_sql_query('SELECT * FROM "semaforos_PT"."alertas" LIMIT 1000', engine)
print(f"Filas cargadas: {len(df)}")

# 1. Eliminar columnas inútiles
drop_cols = ['descripcion', 'pais', 'identificador_unico_universal', 'reporte_por_municipio', 'magvar']
df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')

# 2. Parsear fechas
df['fecha_local'] = pd.to_datetime(df['fecha'], utc=True).dt.tz_convert('America/Cancun')
df['anio']        = df['fecha_local'].dt.year.astype('int16')
df['mes']         = df['fecha_local'].dt.month.astype('int8')
df['dia']         = df['fecha_local'].dt.day.astype('int8')
df['hora']        = df['fecha_local'].dt.hour.astype('int8')
dia_map = {0:'Lunes',1:'Martes',2:'Miércoles',3:'Jueves',4:'Viernes',5:'Sábado',6:'Domingo'}
df['dia_semana']  = df['fecha_local'].dt.dayofweek.map(dia_map).astype('category')
df['turno'] = pd.cut(df['hora'], bins=[-1,5,11,17,23], labels=['Madrugada','Mañana','Tarde','Noche']).astype('category')
df['fecha_local'] = df['fecha_local'].dt.tz_localize(None)
df['fecha_carga_date'] = pd.to_datetime(df['fecha_carga'], utc=True).dt.tz_convert('America/Cancun').dt.date
df = df.drop(columns=['fecha', 'fecha_carga'], errors='ignore')

# 3. Mapeos
tipo_map = {'JAM':'Tráfico','HAZARD':'Peligro','ACCIDENT':'Accidente','ROAD_CLOSED':'Vía Cerrada','POLICE':'Policía','CHIT_CHAT':'Chat'}
df['tipo_es'] = df['tipo'].map(tipo_map).astype('category')

subtipo_map = {
    'JAM_STAND_STILL_TRAFFIC':'Tráfico Detenido','JAM_HEAVY_TRAFFIC':'Tráfico Pesado',
    'JAM_MODERATE_TRAFFIC':'Tráfico Moderado','JAM_LIGHT_TRAFFIC':'Tráfico Ligero',
    'HAZARD_ON_ROAD':'Peligro en Vía','HAZARD_ON_ROAD_CAR_STOPPED':'Auto Detenido',
    'HAZARD_ON_ROAD_CONSTRUCTION':'Obras Viales','HAZARD_ON_ROAD_EMERGENCY_VEHICLE':'Vehículo de Emergencia',
    'HAZARD_ON_ROAD_ICE':'Hielo en Vía','HAZARD_ON_ROAD_LANE_CLOSED':'Carril Cerrado',
    'HAZARD_ON_ROAD_OBJECT':'Objeto en Vía','HAZARD_ON_ROAD_OIL':'Aceite en Vía',
    'HAZARD_ON_ROAD_POT_HOLE':'Bache','HAZARD_ON_ROAD_ROAD_KILL':'Animal Atropellado',
    'HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT':'Semáforo Descompuesto',
    'HAZARD_ON_SHOULDER':'Peligro en Acotamiento','HAZARD_ON_SHOULDER_ANIMALS':'Animales en Acotamiento',
    'HAZARD_ON_SHOULDER_CAR_STOPPED':'Auto Detenido (Acotamiento)','HAZARD_ON_SHOULDER_MISSING_SIGN':'Señal Faltante',
    'HAZARD_WEATHER':'Clima Adverso','HAZARD_WEATHER_FLOOD':'Inundación','HAZARD_WEATHER_FOG':'Neblina',
    'HAZARD_WEATHER_FREEZING_RAIN':'Lluvia Helada','HAZARD_WEATHER_HAIL':'Granizo',
    'HAZARD_WEATHER_HEAT_WAVE':'Ola de Calor','HAZARD_WEATHER_HEAVY_RAIN':'Lluvia Intensa',
    'HAZARD_WEATHER_HEAVY_SNOW':'Nevada Intensa','HAZARD_WEATHER_HURRICANE':'Huracán',
    'HAZARD_WEATHER_MONSOON':'Monzón','HAZARD_WEATHER_TORNADO':'Tornado',
    'ACCIDENT_MINOR':'Accidente Menor','ACCIDENT_MAJOR':'Accidente Mayor',
    'ROAD_CLOSED_HAZARD':'Vía Cerrada (Peligro)','ROAD_CLOSED_CONSTRUCTION':'Vía Cerrada (Obras)','ROAD_CLOSED_EVENT':'Vía Cerrada (Evento)',
    'POLICE_VISIBLE':'Policía Visible','POLICE_HIDING':'Policía Oculto','POLICE_WITH_MOBILE_CAMERA':'Radar Móvil',
}
df['subtipo_es'] = df['subtipo'].map(subtipo_map).astype('category')
camino_map = {1:'Calle local',2:'Calle local',3:'Calle local',4:'Boulevard',5:'Avenida',6:'Carretera mayor',
              7:'Carretera federal',8:'Carretera federal',9:'Vía libre de peaje',10:'Autopista',11:'Rampa',
              14:'Boulevard',15:'Camino privado',16:'Andador / Peatonal',17:'Estacionamiento',
              18:'Carretera internacional',19:'Carretera internacional',20:'Túnel',22:'Puente'}
df['tipo_camino_es'] = df['tipo_camino'].map(camino_map).astype('category')

for col in ['clasificacion_reporte', 'confianza', 'fiabilidad', 'tipo_camino']:
    if col in df.columns:
        df[col] = df[col].astype('int8')

df['municipio'] = 'Benito Juárez'

print("\n=== RESULTADO ===")
print(f"Columnas ({len(df.columns)}): {df.columns.tolist()}")
print(f"\nMuestra:")
print(df[['calle','tipo','tipo_es','subtipo_es','fecha_local','hora','turno','dia_semana','anio','mes','tipo_camino_es','municipio']].head(5).T)
print(f"\n¿Nulos en tipo_es?    {df['tipo_es'].isna().sum()}")
print(f"¿Nulos en subtipo_es? {df['subtipo_es'].isna().sum()} (esperado: {df['subtipo'].isna().sum()} sin subtype)")
print(f"Valores de turno:     {df['turno'].value_counts().to_dict()}")
