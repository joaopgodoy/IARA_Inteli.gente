from . import *

def equation(row):
    try:
        return 100 * (row['AG018 - Volume de água tratada importado'] / row['AG006 -Volume de água produzido'])
    except ZeroDivisionError:
	    return 0
    
class EN_3028(processor):

    def __init__(self, data_list):
        super().__init__(data_list['3028'], score=equation)