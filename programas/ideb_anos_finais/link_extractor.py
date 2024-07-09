import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

def extrair_links(url):
    # Configura o driver do Chrome com a configuração padrão
    driver = webdriver.Chrome()
    
    # Abre a URL fornecida
    driver.get(url)
    
    # Espera um tempo para garantir que a página carregue completamente
    time.sleep(5)

    # Clica no centro da tela para remover um pop-up
    try:
        actions = ActionChains(driver)
        actions.move_by_offset(driver.execute_script("return window.innerWidth / 2;"),
                               driver.execute_script("return window.innerHeight / 2;")).click().perform()
        time.sleep(2)
    except Exception as e:
        print(f"Erro ao clicar no centro da tela: {e}")

    # Clica no botão do ano de 2021
    try:
        botao_2021 = driver.find_element(By.LINK_TEXT, '2021')
        botao_2021.click()
        time.sleep(2)
    except Exception as e:
        print(f"Erro ao clicar no botão de 2021: {e}")
    
    # Clica no botão "anos anteriores"
    try:
        botao_anos_anteriores = driver.find_element(By.LINK_TEXT, 'Anos anteriores')
        botao_anos_anteriores.click()
        time.sleep(2)
    except Exception as e:
        print(f"Erro ao clicar no botão de anos anteriores: {e}")
    
    # Pega o HTML da página atualizada
    html = driver.page_source
    
    # Fecha o navegador
    driver.quit()
    
    # Usa expressões regulares para encontrar os links desejados
    regex = re.compile(r'https://download\.inep\.gov\.br/educacao_basica/portal_ideb/planilhas_para_download/\d{4}/divulgacao_anos_finais_municipios_\d{4}\.zip')
    links = regex.findall(html)
    
    return links

# Exemplo de uso
url = 'https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados'
links = extrair_links(url)
for link in links:
    print(link)
