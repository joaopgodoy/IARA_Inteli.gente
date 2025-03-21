import importlib.util, os, json, pandas as pd, regex as re
from psycopg2 import sql
from psycopg2.extensions import cursor
from .processor import processor
from .constants import dtypes_dict

def extract_dimension_from_path(path: str) -> str | None:
    pattern = r"indicadores/([\w]+)/(\d+)\.py"
    match = re.search(pattern, path)
    
    if not match:
        return None
    
    return match.group(1)   

def load_module(data_list: json, path: str) -> object | None:
    path_absoluto = os.path.abspath(path)

    if not os.path.isfile(path_absoluto):
        print(f"Erro: Arquivo {path} não encontrado.")
        return None
    
    dimension = extract_dimension_from_path(path=path)
    json_obj = data_list[dimension]

    nome_modulo = path.replace("/", ".").replace(".py", "")

    spec = importlib.util.spec_from_file_location(nome_modulo, path_absoluto)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)

    for attr in dir(modulo):
        obj = getattr(modulo, attr)
        if isinstance(obj, type) and issubclass(obj, processor) and obj is not processor:
            return obj(json_obj)

    return None

def execute_indicator(data_list: json, path: str, df: pd.DataFrame, indicator_datapoints) -> pd.DataFrame | None:
    try:
        modulo = load_module(data_list, path)

        current_dataframe = modulo.execute_processing(df, indicator_datapoints)

        return current_dataframe

    except Exception as processingError:
        print(processingError)

        return None

def get_data_indicator_junction(cursor: cursor) -> dict[int, list[int]]:
    cursor.execute(sql.SQL("""
        SELECT D.indicador_id, D.nome_indicador, ARRAY_AGG(J.dado_id)
        FROM juncao_dados_indicador AS J
        JOIN dimensao_indicador as D
        ON J.indicador_id = D.indicador_id
        GROUP BY D.indicador_id, D.nome_indicador;
    """))
    
    data = cursor.fetchall()

    data = [(data_point[0], (data_point[1], data_point[2])) for data_point in data]

    return dict(data) if data else None

def get_fact_table_names(cursor: cursor) -> list[str]:
    cursor.execute("""                              
        SELECT table_name
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name LIKE 'fato_topico%'
        ORDER BY table_name;
    """)
    
    table_names = cursor.fetchall()

    return table_names

def get_table_datapoints(table_name: str, cursor: cursor) -> tuple[pd.DataFrame, dict]:
    initial_df = pd.DataFrame()

    cursor.execute(sql.SQL("""
        SELECT dado_id, tipo_dado,
               ARRAY_AGG(municipio_id) AS municipios,
               ARRAY_AGG(ano) AS anos,
               ARRAY_AGG(valor) AS valores
        FROM {}
        GROUP BY dado_id, tipo_dado;
    """).format(sql.Identifier(table_name[0])))

    datapoints = cursor.fetchall()

    dfs, dtypes = [], {}

    if datapoints:
        for data_id, tipo_dado, municipio_id, ano, valor in datapoints:

            dtypes[data_id] = tipo_dado

            data_list = {
                'codigo_municipio': municipio_id,
                'ano': ano,
                data_id: valor
            }

            temp_df = pd.DataFrame(data_list).set_index(["codigo_municipio", "ano"])

            dfs.append(temp_df)

        initial_df = pd.concat(dfs, axis=1, join="outer")

    return initial_df, dtypes

def get_datapoints_from_database(cursor: cursor) -> tuple[pd.DataFrame, dict]:
    initial_df = pd.DataFrame()

    data_dict = get_data_indicator_junction(cursor=cursor)
    table_names = get_fact_table_names(cursor=cursor)

    dfs, dtypes = [], {}

    for table_name in table_names:
        rows, new_types = get_table_datapoints(table_name=table_name, cursor=cursor)

        if not rows.empty:
            dfs.append(rows)

            dtypes.update(new_types)

    if dfs:
        initial_df = pd.concat(dfs, axis=1, join="outer")

        for col, dtype in dtypes.items():
            if dtype == "int":
                initial_df[col] = initial_df[col].astype("Float64")

            initial_df[col] = initial_df[col].astype(dtypes_dict.get(dtype, dtype))

    return initial_df, data_dict

def save_csv(df: pd.DataFrame, nome: str) -> None:
        """Salva o dataframe final em um arquivo CSV."""
        final_columns = [
            "indicador",
            "tipo_dado",
            "valor",
            "nivel_maturidade"
        ]

        df.to_csv(nome + ".csv", columns=final_columns, index=True)

        print(f'File saved as {nome}.csv successfully.')

def weighted_sum(row, weights) -> int:
    total = 0

    for coluna, weight in weights.items():
        valor = row.get(coluna, 0)
        if pd.notna(valor) and valor not in ["Não sabe", "Não possui"]:
            total += valor * weight
            
    return total