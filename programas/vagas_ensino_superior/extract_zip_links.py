from link_extractor import extrair_links_simples,extrair_links
import requests, zipfile, io, os

FILES_FOLDER_PATH = os.path.join(os.getcwd(), "extracted_files") 

if __name__ == "__main__":
    
    url = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior"
    regex_pattern = r'https://download\.inep\.gov\.br/microdados/microdados_censo_da_educacao_superior_\d{4}\.zip'
    links_encontrados = extrair_links_simples(url, regex_pattern)
    
    if not os.path.isdir(FILES_FOLDER_PATH):
        os.mkdir(FILES_FOLDER_PATH)

    for link in links_encontrados:
        print(link)
        r = requests.get(link)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall("/home/joao/Desktop/IARA_Intelli.gente/IARA_Inteli.gente/programas/vagas_ensino_superior/extracted_files")