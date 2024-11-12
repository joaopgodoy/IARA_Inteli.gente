from . import *
from math import log

def equation(row):
    return ((log(row['idh-m']) - log(row['min'])) / (row['max'] - row['min']))

def df_transform(df):
    min_max_por_ano = df.groupby('ano')['idh-m'].agg(['min', 'max']).reset_index()

    # Mesclar os valores mínimos e máximos no DataFrame original
    df = df.merge(min_max_por_ano, on='ano', how='left')

    return df

with open(SOCIODEM_FILE, "r") as arquivo_json:
    lista_dados = json.load(arquivo_json)

process = processor.from_json(lista_dados['4001'], score=equation)

# Carrega, processa e salva o resultado
process.process_dataframe(process_function=df_transform, drop_columns=['min', 'max'])