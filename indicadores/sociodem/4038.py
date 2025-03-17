from . import *

class SD_4038(processor):

    def __init__(self, data_list):
        super().__init__(data_list['4038'])

    def formula_calculo(self, row):
        
            # 32: 'nível da hierarquia para as regiões de influência das cidades (var09)'
            # 33: 'classe denominação da hierarquia para as regiões de influência das cidades (var10)'
            return row[33]
       