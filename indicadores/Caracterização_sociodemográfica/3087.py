from . import *

with open(SOCIODEM_FILE, "r") as json_file:
    data_list = json.load(json_file)

process = processor.from_json(data_list['3087'])

# Carrega, processa e salva o resultado
process.process_dataframe()