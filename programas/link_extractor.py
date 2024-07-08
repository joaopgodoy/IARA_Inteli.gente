from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests 
import re
import time


# ANTIGAS FUNÇÕES DE EXTRAÇÃO DOS LINKS POR INTERAÇÃO COM A PÁGINA
def fechar_popup_inicial(driver):
    try:
        # Clicar no meio da tela para fechar o pop-up inicial
        driver.execute_script("document.elementFromPoint(window.innerWidth / 2, window.innerHeight / 2).click();")
        print("Fechou o pop-up inicial clicando no meio da tela.")
    except Exception as e:
        print(f"Não foi possível fechar o pop-up inicial clicando no meio da tela: {e}")

def clicar_na_seta(driver, direcao):
    if direcao == "esquerda":
        selector = ".navigation-arrow-left"  # Substitua pelo seletor correto da seta esquerda
    elif direcao == "direita":
        selector = ".navigation-arrow-right"  # Substitua pelo seletor correto da seta direita
    
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
    
    anos = range(2009, 2022+1)
    
    # Clicar em todos os anos para carregar os links
    clicar_todos_os_anos(driver, anos)
    
    # Obter o conteúdo da página renderizada após clicar em todos os anos
    html_content = driver.page_source
    
    driver.quit()
    
    # Padrão regex para encontrar os links específicos
    pattern = re.compile(r'https://download\.inep\.gov\.br/microdados/microdados_censo_da_educacao_superior_\d{4}\.zip')
    links = pattern.findall(html_content)
    print(f"Links encontrados: {links}")
    
    # Salvar o conteúdo HTML completo em um arquivo para verificação (opcional)
    with open("pagina_renderizada.html", "w", encoding='utf-8') as file:
        file.write(html_content)
    print("Conteúdo HTML renderizado salvo em 'pagina_renderizada.html'.")
    
    # Verificar a quantidade de links encontrados
    print(f"Número de links encontrados: {len(links)}")
    
    return links



# FUNÇÃO NOVA E BEM MAIS SIMPLES QUE EXTRAI DIRETO DO HTML DA PÁGINA 
def extrair_links_simples(url):
    # Faz a requisição HTTP para obter o conteúdo da página
    response = requests.get(url)
    response.raise_for_status()  # Garante que a requisição foi bem-sucedida
    
    # Obtém o conteúdo da página como texto
    html = response.text
    
    # Define a expressão regular para encontrar os links desejados
    regex = re.compile(r'https://download\.inep\.gov\.br/microdados/microdados_censo_da_educacao_superior_\d{4}\.zip')
    
    # Encontra todos os links que correspondem à expressão regular
    links = regex.findall(html)
    
    return links

if __name__ == "__main__":
    url = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior"
    links_encontrados = extrair_links_simples(url)
    for link in links_encontrados:
        print(link)
