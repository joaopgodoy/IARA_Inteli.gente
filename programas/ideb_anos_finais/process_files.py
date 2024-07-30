import pandas as pd
import os
from AbstractScrapper import EXTRACTED_FILES_DIR

FILES_FOLDER_PATH = os.path.join(os.getcwd(), EXTRACTED_FILES_DIR)

def process_all_files_in_directory(folder_path: str) -> None:
    # Lista todos os arquivos no diretório e filtra apenas os arquivos .xlsx
    files_list = [f for f in os.listdir(folder_path) if f.endswith(".xlsx")]

    for file in files_list:
        file_correct_path = os.path.join(FILES_FOLDER_PATH, file)
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

    # Verifica a presença da coluna "Sigla da UF" e aplica os filtros adequados
    if 'Sigla da UF' in df.columns:
        # Filtra os dados do DF para "Pública"
        df_df = df[(df['Sigla da UF'] == 'DF') & (df['Rede'] == 'Pública')]
        # Filtra os dados dos outros estados para "Municipal"
        df_others = df[(df['Sigla da UF'] != 'DF') & (df['Rede'] == 'Municipal')]
        # Combina os dois dataframes
        df = pd.concat([df_df, df_others])
    else:
        # Se "Sigla da UF" não estiver presente, filtra por "Municipal"
        df = df[df['Rede'] == 'Municipal']

    # Filtra as colunas que contêm a palavra 'IDEB'
    relevant_cols = [col for col in df.columns if 'IDEB' in str(col)]
    
    # Usar a coluna 'Nome do Município' para os nomes dos municípios
    municipality_col = 'Nome do Município'
    if municipality_col not in df.columns:
        raise ValueError(f"Coluna do município '{municipality_col}' não encontrada no arquivo {xlsx_file_path}")

    relevant_cols.append(municipality_col)  # Inclui a coluna de município

    filtered_df = df[relevant_cols]
    print(filtered_df.head())  # Exibe uma amostra dos dados filtrados
    # Itera sobre as linhas e imprime os municípios e seus valores de IDEB
    for index, row in filtered_df.iterrows():
        municipio = row[municipality_col]
        ideb_values = row[relevant_cols[:-1]]  # Exclui a coluna de município dos valores de IDEB
        print(f"Município: {municipio}")
        print(ideb_values.to_string(index=False))
        print("\n")

    return filtered_df



# Chamada da função principal
process_all_files_in_directory(FILES_FOLDER_PATH)
