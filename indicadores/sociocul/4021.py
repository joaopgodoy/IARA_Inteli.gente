from. import *

def equation(row):
    NVBP = row.get(13)
    NVPN = row.get(14)
    total = row.get(16)

    return ((NVBP / total) + (NVPN / total)) / 2

class SC_4021(processor):

    def __init__(self, data_list):
        super().__init__(data_list['4021'], score=equation)