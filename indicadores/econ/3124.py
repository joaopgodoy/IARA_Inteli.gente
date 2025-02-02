from . import *
from ..common.functions import weighted_sum

class EC_3124(processor):

    def __init__(self, data_list):
        super().__init__(data_list['3124'], score=weighted_sum)

    def execute_processing(self):
        self.process_dataframe(weights=self.pesos)