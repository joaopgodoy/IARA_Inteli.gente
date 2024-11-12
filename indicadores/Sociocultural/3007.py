from . import *
from..common.functions import weighted_sum

with open(SOCIOCUL_FILE, "r") as arquivo_json:
    lista_dados = json.load(arquivo_json)

process = processor.from_json(lista_dados['3007'], score=weighted_sum)

# Carrega, processa e salva o resultado
process.process_dataframe(weights=process.pesos)