from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import requests 
import re
import time


# ANTIGAS FUNÇÕES DE EXTRAÇÃO DOS LINKS POR INTERAÇÃO COM A PÁGINA
def fechar_popup_inicial(driver):
    try:
        # Aguardar um pouco para garantir que o pop-up está visível
        driver.implicitly_wait(5)
        
        # Obter o tamanho da janela
        window_size = driver.get_window_size()
        width = window_size['width']
        height = window_size['height']
        
        # Calcular o ponto central da janela
        center_x = width / 2
        center_y = height / 2
        
        # Usar ActionChains para mover para o centro e clicar
        actions = ActionChains(driver)
        actions.move_by_offset(center_x, center_y).click().perform()
        
        print("Fechou o pop-up inicial clicando no meio da tela.")
    except Exception as e:
        print(f"Não foi possível fechar o pop-up inicial clicando no meio da tela: {e}")


def clicar_na_seta(driver, direcao):
    if direcao == "esquerda":
        selector = "#content-core > div.govbr-tabs.swiper-container-initialized.swiper-container-horizontal.swiper-container-free-mode > div.button-prev > span"  # Substitua pelo seletor correto da seta esquerda
    elif direcao == "direita":
        selector = "#content-core > div.govbr-tabs.swiper-container-initialized.swiper-container-horizontal.swiper-container-free-mode > div.button-next > span"  # Substitua pelo seletor correto da seta direita
    
    try:
        seta_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
        )
        seta_element.click()
        print(f"Clicou na seta {direcao} para mostrar anos anteriores.")
    except Exception as e:
        print(f"Não foi possível clicar na seta {direcao}: {e}")

def clicar_todos_os_anos(driver, anos):
    for i, ano in enumerate(anos):
        while True:
            try:
                print(f"Processando o ano {ano}...")
                # Tentar encontrar o link do ano correspondente
                ano_element = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.LINK_TEXT, str(ano)))
                )
                print(f"Encontrado o elemento do ano {ano}")
                
                # Rolar até o elemento
                driver.execute_script("arguments[0].scrollIntoView();", ano_element)
                print(f"Rolando até o elemento do ano {ano}")
                
                # Tentar clicar no elemento usando JavaScript
                driver.execute_script("arguments[0].click();", ano_element)
                print(f"Clicando no elemento do ano {ano}")
                
                # Esperar que o novo conteúdo seja carregado
                time.sleep(2)  # Esperar mais tempo para garantir o carregamento completo
                print(f"Esperando o carregamento do conteúdo do ano {ano}")
                break  # Sair do loop se o clique for bem-sucedido
            except Exception as e:
                print(f"Erro ao processar o ano {ano}: {e}")
                if i > 0 and ano > anos[i - 1]:
                    print("Tentando clicar na seta esquerda para mostrar anos mais recentes...")
                    clicar_na_seta(driver, "esquerda")  # Tentar clicar na seta esquerda
                else:
                    print("Tentando clicar na seta direita para mostrar anos mais antigos...")
                    clicar_na_seta(driver, "direita")  # Tentar clicar na seta direita

def extrair_links(url):
    driver = webdriver.Chrome()
    driver.maximize_window()  # Maximizar a janela do navegador
    driver.get(url)
    
    # Esperar um pouco para garantir que a página carregue completamente
    driver.implicitly_wait(10)

    
    # Fechar o pop-up inicial clicando no meio da tela
    fechar_popup_inicial(driver)

    
    anos = range(2006, 2022+1)
    
    # Clicar em todos os anos para carregar os links
    clicar_todos_os_anos(driver, anos)
    
    # Obter o conteúdo da página renderizada após clicar em todos os anos
    html_content = driver.page_source
    
    driver.quit()
    
    # Padrão regex para encontrar os links específicos
    pattern = re.compile(r'https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/\d{4}\/TDI_\d{4}\_MUNICIPIOS.zip')
    links = pattern.findall(html_content)
    print(f"Links encontrados: {links}")
    
    
    # Verificar a quantidade de links encontrados
    print(f"Número de links encontrados: {len(links)}")
    
    return links



