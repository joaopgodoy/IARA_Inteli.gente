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
            "indicador_id",
            "tipo_dado",
            "nivel_maturidade"
        ]

        df_filtrado = df[dados.get(self.indicador_id)].dropna()

        df_filtrado[colunas] = df_filtrado.apply(
            lambda row: pd.Series({
                "valor": (valor := self.formula_calculo(row)),
                "indicador_id": self.indicador_id,
                "tipo_dado": type(valor).__name__.strip("3264"),
                "nivel_maturidade": -1
            }),
            axis=1
        )

        return df_filtrado
    
    def process_dataframe(self, df, dados, process_function = None, **kwargs) -> None:
        if process_function:
            df = process_function(df)

        return self.get_processed_dataframe(df=df, dados=dados, **kwargs)