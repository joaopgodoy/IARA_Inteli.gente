import os
import re
import time
import requests
import zipfile
import io
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from YearDataPoint import YearDataPoint
from AbstractScrapper import AbstractScrapper

class UnifiedScrapper(AbstractScrapper):

    def __init__(self, url: str, regex_pattern: str):
        self.url = url
        self.regex_pattern = regex_pattern
        self.files_folder_path = self._create_downloaded_files_dir()
        print(f"Initialized UnifiedScrapper with URL: {url} and regex pattern: {regex_pattern}")
        print(f"Files will be saved in: {self.files_folder_path}")

    def extrair_links(self):
        driver = webdriver.Chrome()
        driver.maximize_window()
        driver.get(self.url)

        # Esperar um pouco para garantir que a página carregue completamente
        driver.implicitly_wait(10)

        # Fechar o pop-up inicial clicando no meio da tela
        self.fechar_popup_inicial(driver)

        # Processar os anos desejados
        anos = range(2006, 2022+1)
        self.clicar_todos_os_anos(driver, anos)

        # Obter o conteúdo da página renderizada após clicar em todos os anos
        html_content = driver.page_source
        driver.quit()

        # Padrão regex para encontrar os links específicos
        pattern = re.compile(self.regex_pattern)
        links = pattern.findall(html_content)
        print(f"Links encontrados: {links}")

        return links

    def fechar_popup_inicial(self, driver):
        try:
            driver.implicitly_wait(5)
            window_size = driver.get_window_size()
            width = window_size['width']
            height = window_size['height']
            center_x = width / 2
            center_y = height / 2
            actions = ActionChains(driver)
            actions.move_by_offset(center_x, center_y).click().perform()
            print("Fechou o pop-up inicial clicando no meio da tela.")
        except Exception as e:
            print(f"Não foi possível fechar o pop-up inicial clicando no meio da tela: {e}")

    def clicar_na_seta(self, driver, direcao):
        if direcao == "esquerda":
            selector = "#content-core > div.govbr-tabs.swiper-container-initialized.swiper-container-horizontal.swiper-container-free-mode > div.button-prev > span"
        elif direcao == "direita":
            selector = "#content-core > div.govbr-tabs.swiper-container-initialized.swiper-container-horizontal.swiper-container-free-mode > div.button-next > span"

        try:
            seta_element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            seta_element.click()
            print(f"Clicou na seta {direcao} para mostrar anos anteriores.")
        except Exception as e:
            print(f"Não foi possível clicar na seta {direcao}: {e}")

    def clicar_todos_os_anos(self, driver, anos):
        for i, ano in enumerate(anos):
            while True:
                try:
                    print(f"Processando o ano {ano}...")
                    ano_element = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.LINK_TEXT, str(ano)))
                    )
                    driver.execute_script("arguments[0].scrollIntoView();", ano_element)
                    driver.execute_script("arguments[0].click();", ano_element)
                    time.sleep(2)
                    break
                except Exception as e:
                    print(f"Erro ao processar o ano {ano}: {e}")
                    if i > 0 and ano > anos[i - 1]:
                        self.clicar_na_seta(driver, "esquerda")
                    else:
                        self.clicar_na_seta(driver, "direita")

    def extract_database(self):
        year_data_points = []

        # Extração dos links das páginas
        links = self.extrair_links()

        if not os.path.isdir(self.files_folder_path):
            os.mkdir(self.files_folder_path)

        for link in links:
            print(f"Baixando e extraindo: {link}")
            r = requests.get(link, verify=False)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            extract_path = os.path.join(self.files_folder_path, self.extract_year_from_link(link))
            os.makedirs(extract_path, exist_ok=True)
            z.extractall(extract_path)

            year_data_point = self.data_dir_process(extract_path)
            if year_data_point:
                year_data_points.append(year_data_point)
                print(f"Processamento concluído na pasta {extract_path} com sucesso.")
            else:
                print(f"Processamento falhou na pasta {extract_path}")

        print(f"Total de YearDataPoints processados: {len(year_data_points)}")
        return year_data_points

    def extract_year_from_link(self, link: str) -> str:
        match = re.search(r'\d{4}', link)
        return match.group(0) if match else 'unknown_year'

    def data_dir_process(self, folder_path: str) -> YearDataPoint:
        print(f"Entrando na pasta: {folder_path}")
        subfolders = [f.path for f in os.scandir(folder_path) if f.is_dir()]
        print(f"Subpastas encontradas: {subfolders}")

        for subfolder in subfolders:
            files_list = os.listdir(subfolder)
            print(f"Arquivos encontrados na subpasta {subfolder}: {files_list}")
            for file in files_list:
                if file.endswith(".xlsx") and "TDI_MUNICIPIOS" in file:
                    file_correct_path = os.path.join(subfolder, file)
                    print(f"Processando arquivo: {file_correct_path}")
                    df = self.process_df(file_correct_path)
                    if df is not None:
                        year = self.extract_year_from_path(folder_path)
                        if year:
                            print(f"Criando YearDataPoint para o ano {year}")
                            return YearDataPoint(df=df, data_year=year)
                        else:
                            print(f"Falha ao extrair o ano do caminho: {folder_path}")
                    else:
                        print(f"Processamento falhou para o arquivo: {file_correct_path}")

        print(f"Não foram encontrados arquivos .xlsx relevantes em {folder_path}")
        return None

    def process_df(self, xlsx_file_path: str) -> pd.DataFrame:
        try:
            df = pd.read_excel(xlsx_file_path, header=6, skiprows=[7, 8])
            print(f"Colunas disponíveis no arquivo {xlsx_file_path}: {df.columns.tolist()}")

            municipality_col = 'Unnamed: 4'
            total_col = 'Total'
            location_col = 'Unnamed: 5'
            admin_dependency_col = 'Unnamed: 6'

            if municipality_col not in df.columns or total_col not in df.columns or location_col not in df.columns or admin_dependency_col not in df.columns:
                raise ValueError(f"Colunas necessárias ('{municipality_col}', '{total_col}', '{location_col}', '{admin_dependency_col}') não encontradas no arquivo {xlsx_file_path}")

            df_filtered = df[(df[location_col] == 'Total') & (df[admin_dependency_col] == 'Total')]
            filtered_df = df_filtered[[municipality_col, total_col]]
            print(f"Dados filtrados:\n{filtered_df.head()}")  # Exibe uma amostra dos dados filtrados

            return filtered_df
        except Exception as e:
            print(f"Erro ao processar o DataFrame: {e}")
            return None

    def extract_year_from_path(self, path: str) -> int:
        print(f"Extraindo ano do caminho: {path}")
        ano_match = re.search(r'\d{4}', path)
        if ano_match:
            year = int(ano_match.group(0))
            print(f"Ano extraído: {year}")
            return year
        else:
            print("Falha ao extrair o ano.")
            return None


if __name__ == "__main__":
    url = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/indicadores-educacionais/taxas-de-distorcao-idade-serie"
    regex_pattern = r'https://download\.inep\.gov\.br/informacoes_estatisticas/indicadores_educacionais/\d{4}/TDI_\d{4}_MUNICIPIOS.zip'

    scrapper = UnifiedScrapper(url, regex_pattern)
    year_data_points = scrapper.extract_database()

    for data_point in year_data_points:
        print(f"Ano: {data_point.data_year}, DataFrame Shape: {data_point.df.shape}")
