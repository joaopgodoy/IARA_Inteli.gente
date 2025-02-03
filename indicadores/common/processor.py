import pandas as pd, os, json

class processor:

    def __init__(self, json_object: json, score = 'coluna') -> None:
        self.nome = json_object.get('nome', 'processed_data').strip().replace(' ', '_')
        self.dados = json_object.get('dados', 'dados')
        self.colunas_valor = json_object.get('colunas_valor', ["valor"])
        self.pesos = json_object.get('pesos')
        self.formula_calculo = score if score != 'coluna' else lambda row: row[json_object.get(score)]

    def __str__(self):
        return f"Dados: {self.dados}\nColunas valor: {self.colunas_valor}\nPesos: {self.pesos}\nFormula: {self.formula_calculo}"
    
    def execute_processing(self, curr_df: pd.DataFrame = None):
        return self.process_dataframe(curr_df)

    def load_csvs(self) -> pd.DataFrame:
        """Carrega e processa todos os CSVs na pasta, unindo-os em um único dataframe ou retorna o único .csv especificado."""
        if not os.path.isdir(self.dados):
            return pd.read_csv(self.dados)

        dataframes = []
        
        # Loop pelos arquivos CSV na pasta
        for arquivo in os.listdir(self.dados):
            if arquivo.endswith('.csv'):
                identificador = os.path.splitext(arquivo)[0]  # Nome único do indicador (excluindo extensão)
                
                # Carrega o CSV e mantém as colunas necessárias
                df = pd.read_csv(os.path.join(self.dados, arquivo), usecols=["ano", "codigo_municipio"] + self.colunas_valor)
                
                # Renomeia a coluna de valor com o identificador do arquivo
                df = df.rename(columns={self.colunas_valor[0]: identificador})
                
                # Adiciona o dataframe à lista
                dataframes.append(df)
        
        # Realiza o merge dos dataframes com base nas colunas-chave
        df_unificado = dataframes[0]
        for df in dataframes[1:]:
            df_unificado = pd.merge(df_unificado, df, on=["ano", "codigo_municipio"], how="outer")

        return df_unificado

    def get_processed_dataframe(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Aplica a fórmula de cálculo de pontuação para cada linha do dataframe."""
        df[self.nome] = df.apply(self.formula_calculo, axis=1, **kwargs).fillna(0)

        return df
    
    def process_dataframe(self, curr_df = None, process_function = None, drop_columns: list = None, **kwargs) -> None:
        drop_columns = [] if drop_columns is None else drop_columns
        curr_df = self.load_csvs() if curr_df is None else curr_df

        if process_function:
            curr_df = process_function(curr_df)

        return self.get_processed_dataframe(df=curr_df, **kwargs).drop(columns=drop_columns)
    
    @classmethod
    def save_csv(self, df: pd.DataFrame, columns: list = None) -> None:
        """Salva o dataframe final em um arquivo CSV."""
        columns = [] if columns is None else columns
        df.to_csv('processed_data.csv', columns=["ano", "codigo_municipio"] + columns, index=False)

        print('File saved as processed_data.csv successfully.')