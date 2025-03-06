from. import *

def equation(row):
    NVBP = float(row.get(13))
    NVPN = float(row.get(14))
    total = float(row.get(16))

    return ((NVBP / total) + (NVPN / total)) / 2

class SC_4021(processor):

    def __init__(self, data_list):
        super().__init__(data_list['4021'], score=equation)