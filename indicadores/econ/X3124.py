from . import *
from ..common.utils import weighted_sum
import pandas as pd

class EC_3124(processor):

    def __init__(self, data_list):
        super().__init__(data_list['3124'], score=weighted_sum)

    def execute_processing(self, curr_df: pd.DataFrame = None):
        return self.process_dataframe(curr_df=curr_df, weights=self.pesos)