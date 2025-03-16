from . import *
from numpy import log
import pandas as pd

class SD_4001(processor):

    def __init__(self, data_list):
        super().__init__(data_list['4001'])

    def formula_calculo(self, row, **kwargs):
        return ((log(row[21]) - log(row['min'])) / (log(row['max']) - log(row['min'])))
    
    def process_function(self, df: pd.DataFrame) -> pd.DataFrame:
        min_max_por_ano = df.groupby('ano')[21].agg(['min', 'max'])

        df = df.join(min_max_por_ano, how='left')

        return df

    def execute_processing(self, df: pd.DataFrame = None, dados: dict = None):
        kwargs = {
            "columns": ['min', 'max']
        }

        return self.process_dataframe(
            df=df,
            dados=dados,
            **kwargs
        )