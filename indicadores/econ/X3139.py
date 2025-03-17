from . import *

class EC_3139(processor):

    def __init__(self, data_list):
        super().__init__(data_list['3139'])

    def formula_calculo(self, row):
        try:
            # 38: 'domicílios particulares permanentes urbanos com existência de arborização de vias públicas'
            # Total_Domicílios ainda não está na VM
            return 100 * (row[38] / row['Total_Domicílios'])
        except ZeroDivisionError:
            return 0