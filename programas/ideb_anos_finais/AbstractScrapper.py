import os
import requests
import zipfile
from DataEnums import BaseFileType
import pandas as pd
import io


# Suprimir os avisos de solicitações HTTPS não verificadas

EXTRACTED_FILES_DIR = "extracted_files"

def download_and_extract_zipfile(file_url: str) -> str:
    """
    Dado um URL para baixar um arquivo zip, baixa esse arquivo zip e extrai seu conteúdo,
    retornando o caminho para o arquivo de dados extraído.
    Args:
       file_url (str): URL para baixar o arquivo .zip
    """
    # Caso o diretório para guardar os arquivos extraídos não exista, vamos criá-lo
    if not os.path.exists(EXTRACTED_FILES_DIR):
        os.makedirs(EXTRACTED_FILES_DIR)
    
    # Baixando o arquivo zip

    response = requests.get(file_url, verify=False)
    if response.status_code == 200:  # Request com sucesso
        zip_file_name = "zipfile.zip"
        zip_file_path = os.path.join(EXTRACTED_FILES_DIR, zip_file_name)
        
        with open(zip_file_path, "wb") as f:
            f.write(response.content)  # Escreve o arquivo zip no diretório de dados temporários
    else:
        raise RuntimeError(f"Falhou em baixar o arquivo .zip, status code da resposta: {response.status_code}")
    
    # Extraindo o arquivo zip
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(EXTRACTED_FILES_DIR)
    
    # No diretório de arquivos baixados e extraídos, vamos ter o .zip e o arquivo de dados extraído
    extracted_files = os.listdir(EXTRACTED_FILES_DIR)
    
    data_file_name = ""
    for file in extracted_files:
        if ".zip" not in file:  # Acha o arquivo de dados, que é o único sem ser .zip
            data_file_name = file
            break
    
    if not extracted_files or not data_file_name:  # Nenhum arquivo foi extraído ou nenhum arquivo de dados foi encontrado
        raise RuntimeError("Extração do arquivo zip no diretório temporário falhou")
    
    return os.path.join(EXTRACTED_FILES_DIR, data_file_name)  # Retorna o caminho para o arquivo extraído


def dataframe_from_link(file_url:str, file_type: BaseFileType, zipfile: bool = True)->pd.DataFrame:
   """
   Dado um link para um arquivo , se ele for zip primeiro extrai e dps carrega o arquivo tabular extraido, caso não seja apenas usas as 
   funções do pandas para carregar essa arquivo em um Dataframe
   Args:
      file_url (str): url para baixa o arquivo
      file_type (BaseFileType): tipo de arquivo a ser baixado
      zipfile (bool): diz se o link é pra um arquivo zip ou não
   Return:
      (Dataframe): um df da base extraida
   """
   
   if zipfile: #link  é pra um arquivo zip, vamos extrair ele primeiro
      file_path:str = download_and_extract_zipfile(file_url) #chama o método da mesma classe de extrair o zipfile
   else:
      file_path:str = file_url  #link n é pra um arquivo zip, o argumento pode ser passado para o pandas direto
   df:pd.DataFrame = None
   try:
      match (file_type): #match case no tipo de dado
         case BaseFileType.EXCEL:
            df = pd.read_excel(file_path)
         case BaseFileType.ODS:
            pass
         case BaseFileType.CSV:
            df = pd.read_csv(file_path)
   except Exception as e:
         raise RuntimeError(f"Não foi possível extrair o dataframe, tem certeza que o arquivo extraido do site não  é zip? Msg de erro? {e}")
   
   """
   TODO
   Colocar os casos para os outros tipos de arquivos, caso seja possível extrair dataframes deles
   """
   if df is None: #não foi possível extrair o DF
      raise RuntimeError("não foi possível criar um dataframe a partir do link")
   
   return df
  