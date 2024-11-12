import json
from ..common.modules import processor, ECON_FILE

with open(ECON_FILE, "r") as json_file:
    data_list = json.load(json_file)

process = processor.from_json(data_list['3124'])

# Carrega, processa e salva o resultado
process.process_dataframe(weights=process.pesos)