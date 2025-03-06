from. import *

class SC_4069(processor):

    def __init__(self, data_list):
        super().__init__(data_list['4069'], score=lambda row: 'Sim' if row[self.coluna_origem] == True else 'Não')