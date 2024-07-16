from link_extractor import extrair_links_simples
import requests, zipfile, io, os
from AbstractScrapper import EXTRACTED_FILES_DIR

FILES_FOLDER_PATH = os.path.join(os.getcwd(), EXTRACTED_FILES_DIR) 

if __name__ == "__main__":
    
    url = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar"
    regex_pattern = r'https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_\d{4}\.zip'
    links_encontrados = extrair_links_simples(url, regex_pattern)
    
    if not os.path.isdir(FILES_FOLDER_PATH):
        os.mkdir(FILES_FOLDER_PATH)

    for link in links_encontrados:
        print(link)
        r = requests.get(link, verify=False)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall("/home/joao/Desktop/IARA_Intelli.gente/IARA_Inteli.gente/programas/equipamentos_de_tecnologia_escolas_municipais/extracted_files")