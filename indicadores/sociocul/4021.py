from . import *
                
class SC_4021(processor):

    def __init__(self, data_list):
        super().__init__(data_list['4021'])

    def formula_calculo(self, row):
        NVBP = row.get(13)
        NVPN = row.get(14)
        total = row.get(16)

        return ((NVBP / total) + (NVPN / total)) / 2