import pandas as pd
import os
from AbstractScrapper import EXTRACTED_FILES_DIR

FILES_FOLDER_PATH = os.path.join(os.getcwd(), EXTRACTED_FILES_DIR) 
CSV_FOLDER = "dados"

def outer_dir_loop(folder_path: str) -> None:
    inner_folder = os.listdir(folder_path)

    for folder in inner_folder:
        folder_correct_path = os.path.join(FILES_FOLDER_PATH, folder)
        if not os.path.isdir(folder_correct_path):
            continue

        if not data_dir_process(folder_correct_path):
            print(f"Processamento falhou em um dos folders: {folder_correct_path}")

        break #comentar para processar tudo


def data_dir_process(folder_path: str) -> bool:
    inner_folder = os.listdir(folder_path)

    if CSV_FOLDER not in inner_folder:
        return False
    
    data_folder = os.path.join(folder_path, CSV_FOLDER)
    data_files_list = os.listdir(data_folder)
    
    is_courses_file = lambda x: ".csv" in x
    filtered_list: list[str] = list(filter(is_courses_file, data_files_list))
    if len(filtered_list) > 1:
        print(f"Mais de um arquivo de microdados_ed_basica encontrado em {data_folder}")
        return False
    elif not filtered_list:
        print(f"Nenhum arquivo de microdados_ed_basica encontrado em {data_folder}")
        return False

    csv_file: str = filtered_list[0]
    full_csv_file_path: str = os.path.join(data_folder, csv_file)
    process_df(full_csv_file_path)

    return True


def process_df(csv_file_path: str) -> pd.DataFrame:
    RELEVANT_COLS: list[str] = [
        "IN_LABORATORIO_INFORMATICA", "IN_EQUIP_LOUSA_DIGITAL", "IN_EQUIP_MULTIMIDIA",
        "IN_DESKTOP_ALUNO", "IN_COMP_PORTATIL_ALUNO", "IN_TABLET_ALUNO", 
        "IN_INTERNET_APRENDIZAGEM", "NO_ENTIDADE"
    ]

    df: pd.DataFrame = pd.read_csv(csv_file_path, sep=";", encoding="latin-1", usecols=RELEVANT_COLS)
    print(df.info())
    print(df[RELEVANT_COLS].head())
    filter_df(df)
    return df


def filter_df(df: pd.DataFrame) -> pd.DataFrame:
    # Sem filtros, mantendo todas as linhas
    print(df.shape)
    print(df.head())
    return df


# Chamada da função principal
outer_dir_loop(FILES_FOLDER_PATH)
