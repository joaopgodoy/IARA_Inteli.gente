import pandas as pd
from math import log
from ..modules.modules import CalculadoraIndicador

def formula(valor, min, max):
    res = ((log(valor) - log(min)) / (log(max) - log(min)))
    return res

def transforma_dataframe_idh_m(df):
    # Carregar o arquivo CSV que está na mesma pasta do script

    # Calcular o valor mínimo e máximo para cada ano
    min_max_por_ano = df.groupby('ano')['valor'].agg(['min', 'max']).reset_index()

    # Mesclar os valores mínimos e máximos no DataFrame original
    df = df.merge(min_max_por_ano, on='ano', how='left')

    # Aplicar a fórmula usando os mínimos e máximos específicos de cada ano
    df['valor_modificado'] = df.apply(lambda row: formula(row['valor'], row['min'], row['max']), axis=1)

    # Remover as colunas auxiliares 'min' e 'max' antes de salvar
    df = df.drop(columns=['min', 'max'])

    # Salvar o DataFrame modificado em um novo arquivo CSV
    df.to_csv('idh_m_transformado.csv', index=False)

calcula_idh = CalculadoraIndicador("idh_m",1,transforma_dataframe_idh_m,None)

df = pd.read_csv('idh_m.csv')

calcula_idh.funcao_calcula_valor(df)