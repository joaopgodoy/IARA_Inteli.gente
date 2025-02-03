import sys
import importlib.util
import os
import json
import regex as re
from .common.processor import processor

def extract_dimension_from_path(caminho):
    padrao = r"indicadores/([\w]+)/(\d+)\.py"
    match = re.search(padrao, caminho)
    
    if match:
        categoria = match.group(1)  
        return categoria
    
    return None  

def carregar_modulo(data_list, caminho):
    caminho_absoluto = os.path.abspath(caminho)

    if not os.path.isfile(caminho_absoluto):
        print(f"Erro: Arquivo {caminho} não encontrado.")
        return None
    
    dimension = extract_dimension_from_path(caminho=caminho)
    json_obj = data_list[dimension]

    nome_modulo = caminho.replace("/", ".").replace(".py", "")

    spec = importlib.util.spec_from_file_location(nome_modulo, caminho_absoluto)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)

    for atributo in dir(modulo):
        objeto = getattr(modulo, atributo)
        if isinstance(objeto, type) and issubclass(objeto, processor) and objeto is not processor:
            return objeto(json_obj)

    return None

def executar_indicador(data_list, caminho, curr_df, columns):
    modulo = carregar_modulo(data_list, caminho)
    
    try:
        curr_df = modulo.execute_processing(curr_df=curr_df)
        columns.append(modulo.nome)

        return curr_df, columns

    except Exception as processingError:
        print(processingError)

        return None, None

if __name__ == "__main__":
    with open("indicadores/dimensions.json", "r") as json_file:
        data_list = json.load(json_file)

        initial_df, columns = None, []

        if len(sys.argv) < 2:
            padrao = re.compile(r"^\d{4}\.py$")

            for subdir, dirs, files in os.walk('indicadores/'):
                for file in files:
                    if padrao.match(file):
                        initial_df, columns = executar_indicador(
                            data_list=data_list,
                            caminho=f"{subdir}/{file}",
                            curr_df=initial_df,
                            columns=columns
                        )

        else:
            for idx, caminho in enumerate(sys.argv[1:]):
                initial_df, columns = executar_indicador(
                    data_list=data_list,
                    caminho=caminho,
                    curr_df=initial_df,
                    columns=columns
                )

        processor.save_csv(df=initial_df, columns=columns)