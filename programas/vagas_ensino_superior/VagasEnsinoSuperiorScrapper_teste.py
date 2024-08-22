import os
import re
import pandas as pd
from YearDataPoint import YearDataPoint
from AbstractScrapper import AbstractScrapper

class UnifiedScrapper(AbstractScrapper):
    
    def __init__(self, url: str, regex_pattern: str):
        self.url = url
        self.regex_pattern = regex_pattern
        self.files_folder_path = self._create_downloaded_files_dir()

    def extract_database(self)->list[YearDataPoint]:
        year_data_points = []

        # Modificação para acessar pastas já extraídas localmente
        inner_folder = os.listdir(self.files_folder_path)

        for folder in inner_folder:
            folder_correct_path = os.path.join(self.files_folder_path, folder)
            print(f"Processando pasta: {folder_correct_path}")
            if not os.path.isdir(folder_correct_path):
                print(f"Não é um diretório: {folder_correct_path}")
                continue

            year_data_point = self.data_dir_process(folder_correct_path)
            if year_data_point:
                year_data_points.append(year_data_point)
                print(f"Processamento concluído na pasta {folder} com sucesso.")
            else:
                print(f"Processamento falhou na pasta {folder}")
        
        print(f"Total de YearDataPoints processados: {len(year_data_points)}")
        return year_data_points

    def data_dir_process(self, folder_path: str) -> YearDataPoint:
        print(f"Entrando na pasta: {folder_path}")
        dados_folder = self.find_dados_folder(folder_path)
        
        if not dados_folder:
            print(f"A pasta 'dados' não foi encontrada em {folder_path}")
            return None

        data_files_list = os.listdir(dados_folder)
        print(f"Arquivos na pasta 'dados': {data_files_list}")
        
        is_courses_file = lambda x: "CURSOS" in x.upper()
        filtered_list = list(filter(is_courses_file, data_files_list))
        print(f"Arquivos filtrados (CURSOS): {filtered_list}")
        
        if len(filtered_list) != 1:
            print(f"Número inesperado de arquivos 'CURSOS' em {dados_folder}")
            return None
        
        csv_file = filtered_list[0]
        full_csv_file_path = os.path.join(dados_folder, csv_file)
        print(f"Processando arquivo: {full_csv_file_path}")
        
        df = self.process_df(full_csv_file_path)
        
        if df is not None:
            year = self.extract_year_from_path(folder_path)
            if year:
                return YearDataPoint(df=df, data_year=year)
            else:
                print(f"Não foi possível extrair o ano do caminho: {folder_path}")
        else:
            print(f"DataFrame é None após processamento do arquivo: {full_csv_file_path}")
        
        return None

    def find_dados_folder(self, base_path):
        for root, dirs, files in os.walk(base_path):
            for dir_name in dirs:
                if dir_name.lower() == "dados":
                    return os.path.join(root, dir_name)
        return None

    def process_df(self, csv_file_path: str) -> pd.DataFrame:
        RELEVANT_COLS = ["TP_GRAU_ACADEMICO", "TP_NIVEL_ACADEMICO", "TP_ORGANIZACAO_ACADEMICA",
                         "TP_CATEGORIA_ADMINISTRATIVA", "QT_VG_TOTAL", "CO_MUNICIPIO", "TP_MODALIDADE_ENSINO"]

        try:
            df = pd.read_csv(csv_file_path, sep=";", encoding="latin-1", usecols=RELEVANT_COLS)
            print(df.info())
            print(df.head())  # Mostra as primeiras linhas do DataFrame para visualização
            filtered_df = self.filter_df(df)
            return filtered_df
        except Exception as e:
            print(f"Erro ao processar o DataFrame: {e}")
            return None

    def filter_df(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_and_filter_vals = {
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

        print(f"Filtered DataFrame shape: {df.shape}")
        return df

    def extract_year_from_path(self, path: str) -> int:
        ano_match = re.search(r'\d{4}', path)
        return int(ano_match.group(0)) if ano_match else None


if __name__ == "__main__":
    url = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior"
    regex_pattern = r'https://download\.inep\.gov\.br/microdados/microdados_censo_da_educacao_superior_\d{4}\.zip'
    
    scrapper = UnifiedScrapper(url, regex_pattern)
    year_data_points = scrapper.extract_database()
    
    for data_point in year_data_points:
        print(f"Ano: {data_point.data_year}, DataFrame Shape: {data_point.df.shape}, Df info {data_point.df.info()}")
