import json
from math import log
from ..common.modules import processor, SOCIODEM_FILE

def equation(row):
    return ((log(row['valor']) - log(row['min'])) / (row['max'] - row['min']))

def df_transform(df):
    min_max_por_ano = df.groupby('ano')['valor'].agg(['min', 'max']).reset_index()

    # Mesclar os valores mínimos e máximos no DataFrame original
    df = df.merge(min_max_por_ano, on='ano', how='left')

with open(SOCIODEM_FILE, "r") as arquivo_json:
    lista_dados = json.load(arquivo_json)

process = processor.from_json(lista_dados['4001'], equation=equation)

# Carrega, processa e salva o resultado
process.process_dataframe(process_function=df_transform, drop_columns=['min', 'max'])