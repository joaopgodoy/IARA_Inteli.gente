import pandas as pd, os, json

class processor:

    def __init__(self, json_object: json, score = None) -> None:
        self.nome = json_object.get('nome', 'processed_data').strip().replace(' ', '_')
        self.indicador_id = json_object.get('indicador_id', None)
        self.pesos = json_object.get('pesos', None)
        self.coluna_origem = json_object.get('coluna', None)
        self.formula_calculo = score if score else lambda row: row.iloc[-1]

    def __str__(self):
        return f"ID: {self.indicador_id}\nNome: {self.nome}"
    
    def execute_processing(self, curr_df: pd.DataFrame = None, dados = None):
        return self.process_dataframe(df=curr_df, dados=dados)

    def get_processed_dataframe(self, df: pd.DataFrame, dados, **kwargs) -> pd.DataFrame:
        """Aplica a fórmula de cálculo de pontuação para cada linha do dataframe."""

        colunas = [
            "valor",
            "municipio_id",
            "indicador_id",
            "tipo_dado",
            "ano",
            "nivel_maturidade"
        ]

        df_filtrado = df[['municipio_id', 'ano'] + dados.get(self.indicador_id)].dropna(how='any')

        df[colunas] = df_filtrado.apply(
            lambda row: pd.Series({
                "valor": (valor := self.formula_calculo(row)),
                "municipio_id": row.get("municipio_id"),
                "indicador_id": self.indicador_id,
                "tipo_dado": type(valor).__name__,
                "ano": row.get("ano"),
                "nivel_maturidade": -1
            }),
            axis=1
        )

        return df
    
    def process_dataframe(self, df, dados, process_function = None, **kwargs) -> None:
        if process_function:
            df = process_function(df)

        return self.get_processed_dataframe(df=df, dados=dados, **kwargs)