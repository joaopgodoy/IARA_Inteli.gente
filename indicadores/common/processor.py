import pandas as pd, os, json

class processor:
    def __init__(self, pesos, dados, colunas_chave, colunas_valor, arquivo_saida, formula_calculo) -> None:
        """
        Inicializa o processador com:
        - dados: diretório onde os arquivos CSV estão localizados
        - colunas_chave: lista das colunas comuns para união dos CSVs (ex: ['ano', 'codigo_municipio'])
        - colunas_valor: lista das colunas a serem renomeadas em cada CSV (nome de cada indicador)
        - pesos: dicionário com os pesos de cada coluna de valor
        - formula_calculo: função que calcula a pontuação com base nos valores das colunas e pesos
        """
        self.dados = dados
        self.colunas_chave = colunas_chave
        self.colunas_valor = colunas_valor
        self.arquivo_saida = arquivo_saida
        self.pesos = pesos
        self.formula_calculo = formula_calculo

    def __str__(self):
        return f"Dados: {self.dados}\nColunas chave: {self.colunas_chave}\nColunas valor: {self.colunas_valor}\nPesos: {self.pesos}\nFormula: {self.formula_calculo}"

    @classmethod
    def from_json(cls, json_object: json, score = 'coluna') -> object:
        new_instance = cls(
            dados = json_object.get('dados', 'dados'),
            colunas_chave = json_object.get('colunas_chave', ["ano", "codigo_municipio"]),
            colunas_valor = json_object.get('colunas_valor', ["valor"]),
            arquivo_saida=json_object.get('arquivo_saida', 'processed_data.csv'),
            pesos = json_object.get('pesos'),
            formula_calculo = score if score != 'coluna' else lambda row: row[json_object.get(score)]
        )

        return new_instance
    
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
                df = pd.read_csv(os.path.join(self.dados, arquivo), usecols=self.colunas_chave + self.colunas_valor)
                
                # Renomeia a coluna de valor com o identificador do arquivo
                df = df.rename(columns={self.colunas_valor[0]: identificador})
                
                # Adiciona o dataframe à lista
                dataframes.append(df)
        
        # Realiza o merge dos dataframes com base nas colunas-chave
        df_unificado = dataframes[0]
        for df in dataframes[1:]:
            df_unificado = pd.merge(df_unificado, df, on=self.colunas_chave, how="outer")

        return df_unificado
            
    def save_csv(self, df: pd.DataFrame) -> None:
        """Salva o dataframe final em um arquivo CSV."""
        df = df[self.colunas_chave + ['score']]
        df.to_csv(self.arquivo_saida, index=False)

        print(f'Arquivo consolidado salvo como {self.arquivo_saida}')

    def get_processed_column(self, df: pd.DataFrame, **kwargs) -> pd.Series | pd.DataFrame:
        return df.apply(self.formula_calculo, axis=1, **kwargs).fillna(0)

    def get_processed_dataframe(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Aplica a fórmula de cálculo de pontuação para cada linha do dataframe."""
        df['score'] = self.get_processed_column(df, **kwargs)

        return df
    
    def process_dataframe(self, process_function = None, drop_columns: list = None, **kwargs) -> None:
        if drop_columns is None:
            drop_columns = []

        unified_df = self.load_csvs()

        if process_function:
            unified_df = process_function(unified_df)

        score_df = self.get_processed_dataframe(unified_df, **kwargs).drop(columns=drop_columns)
        self.save_csv(score_df)