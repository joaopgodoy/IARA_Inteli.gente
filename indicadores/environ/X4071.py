from . import *

class EN_4071(processor):

    def __init__(self, data_list):
        super().__init__(data_list['4071'])

    def formula_calculo(self, row):
        try:
            return 100 * (row['AG018 - Volume de água tratada importado'] / row['AG006 -Volume de água produzido'])
        except ZeroDivisionError:
            return 0