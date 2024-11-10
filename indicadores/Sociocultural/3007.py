import json
from ..common.modules import processor, SOCIOCUL_FILE

with open(SOCIOCUL_FILE, "r") as arquivo_json:
    lista_dados = json.load(arquivo_json)

process = processor.from_json(lista_dados['3007'])

# Carrega, processa e salva o resultado
process.process_dataframe(weights=process.pesos)