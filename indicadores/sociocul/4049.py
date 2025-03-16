from. import *

class SC_4049(processor):

    def __init__(self, data_list):
        super().__init__(data_list['4049'])

    def formula_calculo(self, row, **kwargs):
        return (row[12] / row[16]) * 100000