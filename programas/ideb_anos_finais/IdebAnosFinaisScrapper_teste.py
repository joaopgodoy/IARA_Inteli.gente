import os, re, time, zipfile
import pandas as pd
from functools import reduce
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from ..modules.YearDataPoint import YearDataPoint
from ..modules.AbstractScrapper import AbstractScrapper

class IDEBScrapper(AbstractScrapper):
    
    def __init__(self, url, regex):
        super().__init__(url, regex)

    def __download_and_extract_zipfiles(self, urls: list[str]) -> None:
        # Caminho atual do diretório onde o script está sendo executado
        current_directory = os.getcwd()

        # Juntando o caminho atual com o subdiretório
        download_directory = os.path.join(current_directory, self.DOWNLOADED_FILES_PATH)

        # Verificando o caminho completo
        print(f"Caminho completo para o diretório de download: {download_directory}")

        # Criando o diretório se ele não existir
        if not os.path.exists(download_directory):
            os.makedirs(download_directory)

        print("Entrou em __download_and_extract_zipfiles")
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": download_directory,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        driver = webdriver.Chrome(options=chrome_options)

        for url in urls:
            driver.get(url)
            print(f"Acessando URL: {url}")
            time.sleep(15)  # Tempo para garantir que o download comece

            # Verifique o diretório até encontrar o arquivo ZIP
            downloaded_file = None
            max_wait_time = 300  # Máximo de 5 minutos para aguardar o download
            wait_time = 0

            while wait_time < max_wait_time:
                files = os.listdir(self.DOWNLOADED_FILES_PATH)
                for file in files:
                    if file.endswith(".zip"):
                        downloaded_file = file
                        break
                if downloaded_file:
                    print(f"Arquivo ZIP detectado: {downloaded_file}")
                    break

                time.sleep(10)
                wait_time += 10
                print(f"Aguardando... {wait_time} segundos passados.")

            if not downloaded_file:
                print(f"Tempo de espera excedido para o download de: {url}")
            else:
                # Extrair e remover o arquivo ZIP
                file_path = os.path.join(self.DOWNLOADED_FILES_PATH, downloaded_file)
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(self.DOWNLOADED_FILES_PATH)
                os.remove(file_path)
                print(f"Arquivo ZIP {downloaded_file} extraído e removido.")

        driver.quit()

    def process_all_files_in_directory(self, folder_path: str) -> list[YearDataPoint]:
        year_data_points = []

        # Percorrer todas as pastas extraídas no diretório
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith(".xlsx"):
                    file_correct_path = os.path.join(root, file)
                    year_data_point = self.data_file_process(file_correct_path)
                    if year_data_point:
                        year_data_points.append(year_data_point)
                        print(f"Processamento bem-sucedido para o arquivo: {file_correct_path}")
                    else:
                        print(f"Processamento falhou em um dos arquivos: {file_correct_path}")

        return year_data_points

    def process_df(self, xlsx_file_path: str) -> pd.DataFrame:
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

    def extract_database(self) -> list[YearDataPoint]:
        links = self._extract_links()[:2]
        self.__download_and_extract_zipfiles(links)
        return self.process_all_files_in_directory(self.DOWNLOADED_FILES_PATH)

if __name__ == "__main__":
    URL = "https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados"
    REGEX_PATTERN = r'https://download.inep.gov.br/ideb/resultados/divulgacao_anos_finais_municipios_\d{4}\.zip'

    scrapper = IDEBScrapper(URL, REGEX_PATTERN)
    year_data_points = scrapper.extract_database()

    for data_point in year_data_points:
        data_point.df.to_csv(f"{data_point.data_year}.csv")
        print(f"Ano: {data_point.data_year}, DataFrame Shape: {data_point.df.info()}")