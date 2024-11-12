from . import *

def equation(row):
    try:
        return 100 * (row['AG018 - Volume de água tratada importado'] / row['AG006 -Volume de água produzido'])
    except ZeroDivisionError:
	    return 0

with open(ENVIRONMENT_FILE, "r") as json_file:
    data_list = json.load(json_file)

process = processor.from_json(data_list['4071'], score=equation)

# Carrega, processa e salva o resultado
process.process_dataframe()