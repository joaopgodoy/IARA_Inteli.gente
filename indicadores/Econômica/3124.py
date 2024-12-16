from . import *
from ..common.functions import weighted_sum

with open(ECON_FILE, "r") as json_file:
    data_list = json.load(json_file)

process = processor.from_json(data_list['3124'], score=weighted_sum)

# Carrega, processa e salva o resultado
process.process_dataframe(weights=process.pesos)