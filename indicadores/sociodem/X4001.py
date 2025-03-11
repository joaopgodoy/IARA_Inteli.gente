from . import *
from numpy import log
import pandas as pd

class SD_4001(processor):

    def __init__(self, data_list):
        super().__init__(data_list['4001'])

    def formula_calculo(self, row):
        return ((log(row['idh-m']) - log(row['min'])) / (log(row['max']) - log(row['min'])))
    
    def process_function(self, df: pd.DataFrame) -> pd.DataFrame:
        min_max_por_ano = df.groupby('ano')['idh-m'].agg(['min', 'max']).reset_index()

        df = df.merge(min_max_por_ano, on='ano', how='left')

        return df

    def execute_processing(self, curr_df: pd.DataFrame = None):
        return self.process_dataframe(
            curr_df=curr_df
        )