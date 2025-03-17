from . import *

class EC_3139(processor):

    def __init__(self, data_list):
        super().__init__(data_list['3145'])

    def formula_calculo(self, row):
        try:
            # 37: 'domicílios particulares permanentes com existência de iluminação pública no entorno'
            # Total_Domicílios ainda não está na VM
            return 100 * (row[37] / row['Total_Domicílios'])
        except ZeroDivisionError:
            return 0