from . import *
from math import log
import pandas as pd

def equation(row):
    return ((log(row['idh-m']) - log(row['min'])) / (row['max'] - row['min']))

def df_transform(df):
    min_max_por_ano = df.groupby('ano')['idh-m'].agg(['min', 'max']).reset_index()

    # Mesclar os valores mínimos e máximos no DataFrame original
    df = df.merge(min_max_por_ano, on='ano', how='left')

    return df

class SD_4001(processor):

    def __init__(self, data_list):
        super().__init__(data_list['4001'])

    def execute_processing(self, curr_df: pd.DataFrame = None):
        return self.process_dataframe(
            curr_df=curr_df,
            process_function=df_transform,
            drop_columns=['min', 'max']
        )