from . import *
from ..common.utils import weighted_sum
import pandas as pd

class SC_3007(processor):

    def __init__(self, data_list):
        super().__init__(data_list['3007'], score=weighted_sum)

    def execute_processing(self, curr_df: pd.DataFrame = None):
        return self.process_dataframe(curr_df=curr_df, weights=self.pesos)