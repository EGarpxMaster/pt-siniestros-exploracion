import pandas as pd
df = pd.read_csv('data/semaforos_PT/alertas_historico.csv', nrows=100)
print('Cols:', df.columns)
if 'Location' in df.columns:
    print('Location head:')
    print(df['Location'].head(5))
    coords = df['Location'].astype(str).str.extract(r'(?i)Point\(([-.\d]+)\s+([-.\d]+)\)')
    print('Coords extracted:')
    print(coords.head(5))
