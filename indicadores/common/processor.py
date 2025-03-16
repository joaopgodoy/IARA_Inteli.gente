import pandas as pd, json

INDICATOR_NAME, INDICATOR_DATA = 0, 1

class processor:

    def __init__(self, json_object: json) -> None:
        self.nome = json_object.get('nome', 'processed_data').strip().replace(' ', '_')
        self.indicador_id = json_object.get('indicador_id', None)
        self.pesos = json_object.get('pesos', None)
        self.ranges = json_object.get('ranges', None)

    def __str__(self):
        return f"ID: {self.indicador_id}\nNome: {self.nome}"
    
    def formula_calculo(self, row, **kwargs):
        return row.iloc[-1]
    
    def ranges_maturidade(self, value):
        if self.ranges is not None:
            for level, (bottom_range, upper_range) in enumerate(self.ranges, start=1):
                if bottom_range <= value <= upper_range:
                    return level
            
        return -1
    
    def process_function(self, df: pd.DataFrame) -> pd.DataFrame | None:
        return df
    
    def execute_processing(self, df: pd.DataFrame = None, dados = None):
        return self.process_dataframe(df=df, dados=dados)

    def get_processed_dataframe(self, df: pd.DataFrame, dados, **kwargs) -> pd.DataFrame:
        """Aplica a fórmula de cálculo de pontuação para cada linha do dataframe."""

        df_filtrado = df[dados.get(self.indicador_id)[INDICATOR_DATA] + kwargs.get('columns', [])].dropna()

        df_filtrado["valor"] = df_filtrado.apply(lambda row: self.formula_calculo(row, **kwargs), axis=1)

        if pd.api.types.is_float_dtype(df_filtrado["valor"]):
            df_filtrado["valor"] = df_filtrado["valor"].round(3)

        df_filtrado["indicador"] = dados.get(self.indicador_id)[INDICATOR_NAME]
        df_filtrado["tipo_dado"] = type(df_filtrado['valor'].iloc[0]).__name__.strip("3264")
        df_filtrado["nivel_maturidade"] = df_filtrado["valor"].apply(self.ranges_maturidade)

        return df_filtrado
    
    def process_dataframe(self, df, dados, **kwargs) -> None:
        df = self.process_function(df)

        return self.get_processed_dataframe(df=df, dados=dados, **kwargs)