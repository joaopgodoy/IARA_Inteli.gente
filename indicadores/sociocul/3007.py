from . import *
from ..common.functions import weighted_sum

class SC_3007(processor):

    def __init__(self, data_list):
        super().__init__(data_list['3007'], score=weighted_sum)

    def execute_processing(self):
        self.process_dataframe(weights=self.pesos)