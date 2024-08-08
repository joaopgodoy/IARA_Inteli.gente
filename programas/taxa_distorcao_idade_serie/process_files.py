import pandas as pd
import os

FILES_FOLDER_PATH = os.path.join(os.getcwd(), "extracted_files")  # Ajuste conforme necessário

def process_all_files_in_directory(folder_path: str) -> None:
    # Lista todas as subpastas no diretório
    subfolders = [f.path for f in os.scandir(folder_path) if f.is_dir()]
    print("Subpastas encontradas:", subfolders)

    for subfolder in subfolders:
        files_list = os.listdir(subfolder)
        print("Arquivos encontrados na subpasta", subfolder, ":", files_list)
        for file in files_list:
            if file.endswith(".xlsx") and "TDI_MUNICIPIOS" in file:
                file_correct_path = os.path.join(subfolder, file)
                print("Processando arquivo:", file_correct_path)
                if not data_file_process(file_correct_path):
                    print(f"Processamento falhou em um dos arquivos: {file_correct_path}")
                else:
                    print(f"Processamento bem-sucedido para o arquivo: {file_correct_path}")

def data_file_process(file_path: str) -> bool:
    try:
        process_df(file_path)
        return True
    except Exception as e:
        print(f"Erro ao processar o arquivo {file_path}: {e}")
        return False

def process_df(xlsx_file_path: str) -> pd.DataFrame:
    # Ler o arquivo XLSX a partir da linha 6 como cabeçalho e a partir da linha 9 para os dados
    df = pd.read_excel(xlsx_file_path, header=6, skiprows=[7, 8])

    # Exibe os nomes das colunas para verificação
    print(f"Colunas disponíveis no arquivo {xlsx_file_path}: {df.columns.tolist()}")

    # Usar a coluna 'Unnamed: 4' para os nomes dos municípios
    municipality_col = 'Unnamed: 4'
    total_col = 'Total'
    location_col = 'Unnamed: 5'  # Supondo que a coluna de localização seja 'Unnamed: 5'
    admin_dependency_col = 'Unnamed: 6'  # Supondo que a coluna de dependência administrativa seja 'Unnamed: 6'

    if municipality_col not in df.columns or total_col not in df.columns or location_col not in df.columns or admin_dependency_col not in df.columns:
        raise ValueError(f"Colunas necessárias ('{municipality_col}', '{total_col}', '{location_col}', '{admin_dependency_col}') não encontradas no arquivo {xlsx_file_path}")

    # Filtra as linhas onde a coluna 'Localização' contém o valor 'Total' e a coluna 'Dependência Administrativa' contém o valor 'Total'
    df_filtered = df[(df[location_col] == 'Total') & (df[admin_dependency_col] == 'Total')]

    filtered_df = df_filtered[[municipality_col, total_col]]
    print(filtered_df.head())  # Exibe uma amostra dos dados filtrados
    
    # Itera sobre as linhas e imprime os municípios e seus valores totais
    for index, row in filtered_df.iterrows():
        municipio = row[municipality_col]
        total_value = row[total_col]
        print(f"Município: {municipio}, Total: {total_value}")

    return filtered_df

# Chamada da função principal
process_all_files_in_directory(FILES_FOLDER_PATH)
