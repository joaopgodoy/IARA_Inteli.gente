import requests
import re
import os
import pandas as pd
import zipfile
import io
from AbstractScrapper import AbstractScrapper
from YearDataPoint import YearDataPoint
from DataEnums import BaseFileType

class UnifiedScrapper(AbstractScrapper):
    
    def __init__(self, url: str, regex_pattern: str):
        self.url = url
        self.regex_pattern = regex_pattern
        self.files_folder_path = self._create_downloaded_files_dir()

    def extrair_links_simples(self, url, regex_pattern):
        # Faz a requisição HTTP para obter o conteúdo da página
        response = requests.get(url)
        response.raise_for_status()  # Garante que a requisição foi bem-sucedida
        
        # Obtém o conteúdo da página como texto
        html = response.text
        
        # Define a expressão regular para encontrar os links desejados
        regex = re.compile(regex_pattern)
        
        # Encontra todos os links que correspondem à expressão regular
        links = regex.findall(html)
        
        return links

    def download_and_extract(self, link, extract_to):
        print(f"Baixando e extraindo: {link}")
        response = requests.get(link, verify=False)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(extract_to)
        print(f"Extração concluída para {link}")

    def find_single_subfolder(self, base_path):
        # Procura por uma única subpasta dentro de 'base_path'
        subfolders = [f.path for f in os.scandir(base_path) if f.is_dir()]
        if len(subfolders) == 1:
            return subfolders[0]
        return None

    def find_dados_folder(self, base_path):
        # Procura pela pasta "dados" ou "DADOS" dentro de 'base_path'
        for item in os.listdir(base_path):
            if item.lower() == "dados":
                return os.path.join(base_path, item)
        return None

    def extract_database(self):
        links = self.extrair_links_simples(self.url, self.regex_pattern)
        year_data_points = []

        for link in links:
            ano_match = re.search(r'\d{4}', link)
            if ano_match:
                ano = int(ano_match.group(0))
                extract_path = os.path.join(self.files_folder_path, f"year_{ano}")
                os.makedirs(extract_path, exist_ok=True)
                
                # Baixa e extrai o arquivo zip
                self.download_and_extract(link, extract_path)
                
                # Entra na única subpasta dentro da pasta "year_xxxx"
                subfolder_path = self.find_single_subfolder(extract_path)
                if subfolder_path:
                    # Procura pela pasta "dados" ou "DADOS" dentro da subpasta encontrada
                    dados_folder = self.find_dados_folder(subfolder_path)
                    if dados_folder:
                        extracted_files = os.listdir(dados_folder)
                        print(f"Arquivos extraídos na pasta 'dados' para o ano {ano}: {extracted_files}")
                        
                        # Carrega os dados extraídos em um DataFrame (assumindo que é CSV)
                        for filename in extracted_files:
                            if f"MICRODADOS" in filename.upper():
                                file_path = os.path.join(dados_folder, filename)
                                print(f"Lendo arquivo CSV: {file_path}")
                                try:
                                    df = pd.read_csv(file_path)
                                    year_data_points.append(YearDataPoint(df=df, data_year=ano))
                                    print(f"Arquivo {filename} adicionado à lista year_data_points")
                                except Exception as e:
                                    print(f"Erro ao ler o arquivo {filename}: {e}")
                            else:
                                print(f"Arquivo {filename} não é um CSV correspondente ao ano {ano}")
                    else:
                        print(f"A pasta 'dados' não foi encontrada em {subfolder_path}")
                else:
                    print(f"Nenhuma subpasta única encontrada em {extract_path}")
        
        print(f"Total de YearDataPoints adicionados: {len(year_data_points)}")
        return year_data_points

if __name__ == "__main__":
    url = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior"
    regex_pattern = r'https://download\.inep\.gov\.br/microdados/microdados_censo_da_educacao_superior_\d{4}\.zip'
    
    scrapper = UnifiedScrapper(url, regex_pattern)
    year_data_points = scrapper.extract_database()
    
    for data_point in year_data_points:
        print(f"Ano: {data_point.data_year}, DataFrame Shape: {data_point.df.shape}")
