import pandas as pd, regex as re, unicodedata
from . import DBconnection

INDICATOR_SCORE_NULL_VAL = -1
    
def get_datapoint_dim_table_info(data_point_name:str)-> dict | None:
   """
   Dado o nome de um dado, retorna a tabela de fato que ele pertence e os anos da série histórica desse dado.
   Caso esse dado não esteja mapeado na tabela dimensao_dados, retorna none

   Args:
      data_point_name (str): Nome do dado

   Return:
      (dict):
      {
            "topico": topico_do_dado,
            "dado_id": id_do_dado,
            "anos_serie_historica": lista_anos_serie_historica
      }
   """
   query = f"""--sql
   SELECT topico,dado_id,anos_serie_historica FROM dimensao_dado
   WHERE  LOWER(REPLACE(dimensao_dado.nome_dado, ' ', '')) = LOWER(REPLACE('{data_point_name}', ' ', ''));
   """
   result:list[tuple] = DBconnection.execute_query(query)
   if not result:
      return None

   return {
      "topico": result[0][0],
      "dado_id": result[0][1],
      "anos_serie_historica":result[0][2]
   }

def replace_city_codes_with_pk(city_codes:pd.Series)->pd.Series:
    query = """
    SELECT municipio_id,codigo_municipio FROM dimensao_municipio;
    """
    query_result = DBconnection.execute_query(query)
    city_code_to_pk:dict[int,int] = {city_code:city_pk for city_pk,city_code  in query_result} #dict cuja key é o codigo do munic e o valor é a pk da tabela de dimensao do municipio

    print(city_code_to_pk)

    return city_codes.map(city_code_to_pk)

def remove_non_en_chars(input_str:str)->str:
    normalized_text = unicodedata.normalize('NFKD', input_str)
    return normalized_text.encode('ascii', 'ignore').decode('ascii')

def parse_topic_table_name(data_topic:str,indicator_table = False)->str:
    """
    Transforma o nome de um tópico de um indicador em um nome de tabela aceitado pelo PG SQL e padronizado.
     
    Caso seja uma tabela fato de dados brutos, começa com "fato_topico"
    Caso seja uma tabela fato de indicadores, começa com "indicador_fato_topico"
    """
    str_:str = remove_non_en_chars(data_topic)
   
    str_ = str_.replace(" ", "_").replace("-", "_")
    str_ = str_.lower()
    str_ = str_.strip()
    
    # remove todos chars que não são letras, underscore ou números
    str_ = re.sub(r'[^a-zA-Z0-9_]', '', str_)
    
    #truncar para o tamanho max de um identificador do postgres

    if indicator_table:
        return f"indicador_fato_topico_{str_[:52]}"
    else:
        return f"fato_topico_{str_[:63]}"
    
def __normalize_text_for_indicators(indicator_name:str)->str:
   """
   Normaliza texto para esse caso específico de comparação de nomes de indicadores: deixa tudo lowercase,
   e tirar espaço e underline
   """
   return indicator_name.replace("_","") \
                        .replace(" ","") \
                        .lower()
    
def get_indicador_dim_table_info(indicator_name:str)->dict:
   """
   Retorna informação da tabela de dimensao_indicador correspondente à um certo indicador
   """
   parsed_name = __normalize_text_for_indicators(indicator_name)
   query = f"""--sql
   SELECT indicador_id,topico FROM dimensao_indicador
   WHERE  LOWER(REPLACE(dimensao_indicador.nome_indicador, ' ', '')) = '{parsed_name}'; 
   """

   result = DBconnection.execute_query(query)

   if len(result) == 0:
      raise IOError("Falha ao achar o ID do indicador {indicator_name} na tabela dimensao_indicador")
   
   return {
      "indicator_id": result[0][0],
      "topico": result[0][1]
   }
    
def insert_df_indicators_table(df:pd.DataFrame,has_indicator_score = False)->None:
    """
    Insere os dados de um indicador (no formato de DF) na tabela de fatos (indicador_fato) correspondente

    Args:
        df (pd.DataFrame): Dados do indicador. Tem que ter as colunas: (ano,codigo_municipio,valor,indicador,tipo_dado)
            e de forma opcional uma coluna com a nota do indicador

        has_indicator_score (bool): diz se o df dos dados do indicador tem a nota do indicador 

    Return:
        (None)
    """

    if df.shape[0] < 1:
        raise RuntimeError("Dataframe passado como argumento deve ter mais de uma linha")

    indicator_name:str = df["indicador"].iloc[0]
    indicator_info :dict = get_indicador_dim_table_info(indicator_name)
    indicator_id:int = int(indicator_info["indicator_id"])
    topic:str = indicator_info["topico"]
    table_name:str = parse_topic_table_name(topic,indicator_table=True)

    fact_table_cols =  (
        'municipio_id',
        'indicador_id',
        'ano',
        'tipo_dado',
        'valor',
        'nivel_maturidade'
    )

    df_rows:list[tuple] = []
    for row in df.itertuples(index=False):
        ano = (row.ano)
        codigo_municipio = (row.codigo_municipio)
        valor = str(row.valor)
        tipo_dado = row.tipo_dado

        if has_indicator_score: #tem nota do indicador de 1 a 7 
            nota_indicador = (row.nivel_maturidade)
            df_rows.append(
            (codigo_municipio,indicator_id,ano,tipo_dado,valor,nota_indicador)
            )
        else:
            df_rows.append(
            (codigo_municipio,indicator_id,ano,tipo_dado,valor,INDICATOR_SCORE_NULL_VAL)
            )

    DBconnection.insert_many_values(
       table_name=table_name,
       columns_tuple=fact_table_cols,
       values_list=df_rows,
       batch_size=2500
    )