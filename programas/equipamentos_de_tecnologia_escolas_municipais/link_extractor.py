from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests 
import re
import time



# FUNÇÃO NOVA E BEM MAIS SIMPLES QUE EXTRAI DIRETO DO HTML DA PÁGINA 
def extrair_links_simples(url, regex_pattern):
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

if __name__ == "__main__":
    url = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar"
    links_encontrados = extrair_links_simples(url)
    for link in links_encontrados:
        print(link)
