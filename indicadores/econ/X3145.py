from . import *

class EC_3139(processor):

    def __init__(self, data_list):
        super().__init__(data_list['3145'])

    def formula_calculo(self, row):
        try:
            return 100 * (row['Domicílios particulares permanentes urbanos com Existência de iluminação pública no entorno'] / row['Total_Domicílios'])
        except ZeroDivisionError:
            return 0