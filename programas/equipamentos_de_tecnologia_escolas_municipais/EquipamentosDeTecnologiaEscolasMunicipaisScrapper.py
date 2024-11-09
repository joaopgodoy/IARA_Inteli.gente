import os, re, time, zipfile, logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from ..modules.YearDataPoint import YearDataPoint
from ..modules.AbstractScrapper import AbstractScrapper

class EquipamentosDeTecnologiaEscolasMunicipaisScrapper(AbstractScrapper):

    def __init__(self, url, regex):
        super().__init__(url, regex)

    def extract_database(self) -> list[YearDataPoint]:
        links = self._extract_links()
        self.download_zipfiles(links)
        self.extract_zipfiles()
        return self.process_all_files_in_directory(self.files_folder_path)
   
    def process_all_files_in_directory(self, folder_path: str) -> list[YearDataPoint]:
        year_data_points = []

        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith(".csv") and not file.lower().startswith("~$"):
                    file_correct_path = os.path.join(root, file)
                    year_data_point = self.data_file_process(file_correct_path)
                    if year_data_point:
                        year_data_points.append(year_data_point)
                        print(f"Processamento bem-sucedido para o arquivo: {file_correct_path}")
                    else:
                        print(f"Processamento falhou em um dos arquivos: {file_correct_path}")

    def data_file_process(self, file_path: str) -> YearDataPoint:
        try:
            df = self.process_df(file_path)
            year = self.__extract_year_from_path(file_path)
            return YearDataPoint(df=df, data_year=year)
        except Exception as e:
            print(f"Erro ao processar o arquivo {file_path}: {e}")
            return None
        
    def __extract_year_from_path(self, path: str) -> int:
        ano_match = re.search(r'\d{4}', path)
        if ano_match:
            return int(ano_match.group(0))
        else:
            logging.error("Falha ao extrair o ano.")
            return None

    def download_zipfiles(self, urls: list[str]) -> None:
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": os.path.abspath(self.files_folder_path),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        driver = webdriver.Chrome(options=chrome_options)

        for url in urls:
            logging.info(f"Acessando URL: {url}")
            driver.get(url)
            time.sleep(10)  # Tempo para garantir que o download comece

            max_wait_time = 300  # 5 minutos
            wait_time = 0
            poll_interval = 5

            while wait_time < max_wait_time:
                files = os.listdir(self.files_folder_path)
                downloading = any(file.endswith(".crdownload") for file in files)
                if not downloading:
                    logging.info(f"Download concluído para URL: {url}")
                    break

                time.sleep(poll_interval)
                wait_time += poll_interval
                logging.info(f"Aguardando... {wait_time} segundos passados.")

            if downloading:
                logging.error(f"Tempo de espera excedido para o download de: {url}")
        
        driver.quit()

    def extract_zipfiles(self) -> None:
        for file in os.listdir(self.files_folder_path):
            if file.endswith(".zip"):
                file_path = os.path.join(self.files_folder_path, file)
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(self.files_folder_path)
                os.remove(file_path)
                logging.info(f"Arquivo ZIP {file} extraído e removido.")

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

if __name__ == "__main__":
    URL = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar"
    REGEX_PATTERN = r'https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_\d{4}\.zip'

    scrapper = EquipamentosDeTecnologiaEscolasMunicipaisScrapper(URL, REGEX_PATTERN)
    year_data_points = scrapper.extract_database()

    for data_point in year_data_points:
        data_point.df.to_csv(f"{data_point.data_year}.csv")
        logging.info(f"Ano: {data_point.data_year}, DataFrame Shape: {data_point.df.info()}")
