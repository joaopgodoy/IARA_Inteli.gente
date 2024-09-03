import os
import re
import pandas as pd
from YearDataPoint import YearDataPoint
from AbstractScrapper import AbstractScrapper

EXTRACTED_FILES_DIR = "extracted_files"

class EquipamentosDeTecnologiaEscolasMunicipaisScrapper(AbstractScrapper):

    def __init__(self):
        super().__init__()
        self.files_folder_path = EXTRACTED_FILES_DIR

    def process_all_files_in_directory(self, folder_path: str) -> list[YearDataPoint]:
        year_data_points = []

        for root, dirs, files in os.walk(folder_path):
            if os.path.basename(root).lower() == "dados":
                for file in files:
                    if file.endswith(".csv"):
                        file_correct_path = os.path.join(root, file)
                        year_data_point = self.data_file_process(file_correct_path)
                        if year_data_point:
                            year_data_points.append(year_data_point)

        if not year_data_points:
            print("Nenhum arquivo processado. Verifique a estrutura de diretórios e arquivos.")
        return year_data_points

    def data_file_process(self, file_path: str) -> YearDataPoint:
        try:
            df = self.process_df(file_path)
            year = self.__extract_year_from_path(file_path)
            return YearDataPoint(df=df, data_year=year)
        except Exception as e:
            print(f"Erro ao processar o arquivo {file_path}: {e}")
            return None

    def process_df(self, csv_file_path: str) -> pd.DataFrame:
        # Defina as colunas relevantes
        RELEVANT_COLS: list[str] = [
            "IN_LABORATORIO_INFORMATICA", "IN_EQUIP_LOUSA_DIGITAL", "IN_EQUIP_MULTIMIDIA",
            "IN_DESKTOP_ALUNO", "IN_COMP_PORTATIL_ALUNO", "IN_TABLET_ALUNO", 
            "IN_INTERNET_APRENDIZAGEM", "NO_ENTIDADE"
        ]

        # Leitura do arquivo CSV, utilizando as colunas relevantes
        df = pd.read_csv(csv_file_path, sep=";", encoding="latin-1", usecols=RELEVANT_COLS)

        # Exibindo informações básicas para verificar se a leitura foi correta
        print(df.info())
        print(df[RELEVANT_COLS].head())

        # Chamando a função filter_df
        self.filter_df(df)
        
        return df

    def filter_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Retornando o DataFrame sem filtros
        return df

    def __extract_year_from_path(self, path: str) -> int:
        ano_match = re.search(r'\d{4}', path)
        if ano_match:
            return int(ano_match.group(0))
        else:
            print("Falha ao extrair o ano.")
            return None

    def extract_database(self) -> list[YearDataPoint]:
        return self.process_all_files_in_directory(self.files_folder_path)


if __name__ == "__main__":
    scrapper = EquipamentosDeTecnologiaEscolasMunicipaisScrapper()
    year_data_points = scrapper.extract_database()

    for data_point in year_data_points:
        data_point.df.to_csv(f"{data_point.data_year}.csv")
        print(f"Ano: {data_point.data_year}, DataFrame Shape: {data_point.df.info()}")
