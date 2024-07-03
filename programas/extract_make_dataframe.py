from link_extractor import extrair_links
from AbstractScrapper import download_and_extract_zipfile
import requests, zipfile, io, os
from AbstractScrapper import EXTRACTED_FILES_DIR

FILES_FOLDER_PATH = os.path.join(os.getcwd(), EXTRACTED_FILES_DIR) 

if __name__ == "__main__":
    
    url = "https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/censo-da-educacao-superior/resultados"
    links_encontrados = extrair_links(url)
    
    if not os.path.isdir(FILES_FOLDER_PATH):
        os.mkdir(FILES_FOLDER_PATH)

    for link in links_encontrados:
        print(link)
        r = requests.get(link, verify=False)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall("/home/joao/Desktop/IARA_Intelli.gente/IARA_Inteli.gente/programas/extracted_files")