import pandas as pd
import os

FILES_FOLDER_PATH = os.path.join(os.getcwd(), "extracted_files")
CSV_FOLDER = "dados"

def outer_dir_loop(folder_path: str) -> None:
    inner_folder = os.listdir(folder_path)

    for folder in inner_folder:
        folder_correct_path = os.path.join(FILES_FOLDER_PATH, folder)
        if not os.path.isdir(folder_correct_path):
            continue

        if not data_dir_process(folder_correct_path):
            print("Processamento falhou em um dos folders")

def data_dir_process(folder_path: str) -> bool:
    inner_folder = os.listdir(folder_path)

    if CSV_FOLDER not in inner_folder:
        return False
    
    data_folder = os.path.join(folder_path, CSV_FOLDER)
    data_files_list = os.listdir(data_folder)
    
    is_courses_file = lambda x: "CURSOS" in x
    filtered_list: list[str] = list(filter(is_courses_file, data_files_list))
    if len(filtered_list) > 1:
        return False
    
    csv_file: str = filtered_list[0]
    full_csv_file_path: str = os.path.join(data_folder, csv_file)
    process_df(full_csv_file_path)

    return True

def process_df(csv_file_path: str) -> pd.DataFrame:
    RELEVANT_COLS: list[str] = ["TP_GRAU_ACADEMICO", "TP_NIVEL_ACADEMICO", "TP_ORGANIZACAO_ACADEMICA",
                                "TP_CATEGORIA_ADMINISTRATIVA", "QT_VG_TOTAL", "CO_MUNICIPIO", "TP_MODALIDADE_ENSINO"]

    df: pd.DataFrame = pd.read_csv(csv_file_path, sep=";", encoding="latin-1", usecols=RELEVANT_COLS)
    print(df.info())
    print(df.head())  # Mostra as primeiras linhas do DataFrame para visualização
    filter_df(df)

def filter_df(df: pd.DataFrame) -> pd.DataFrame:
    DATA_COL: str = "QT_VG_TOTAL"
    
    cols_and_filter_vals: dict[str, list] = {
        "TP_GRAU_ACADEMICO": [1, 2, 3, 4],
        "TP_NIVEL_ACADEMICO": [1, 2],
        "TP_ORGANIZACAO_ACADEMICA": [1, 2, 3, 4, 5],
        "TP_CATEGORIA_ADMINISTRATIVA": [1, 2, 3, 4, 5, 7],
        "TP_MODALIDADE_ENSINO": [1]
    }

    df = df.dropna(axis="index", subset=["TP_GRAU_ACADEMICO", "TP_NIVEL_ACADEMICO", "TP_ORGANIZACAO_ACADEMICA", "TP_CATEGORIA_ADMINISTRATIVA"])

    for col in ["TP_GRAU_ACADEMICO", "TP_NIVEL_ACADEMICO", "TP_ORGANIZACAO_ACADEMICA", "TP_CATEGORIA_ADMINISTRATIVA"]:
        df[col] = df[col].astype("int")

    for key, val in cols_and_filter_vals.items():
        filtered_series = df[key].apply(lambda x: x in val)
        df = df[filtered_series]

    print(df.shape)

outer_dir_loop(FILES_FOLDER_PATH)
