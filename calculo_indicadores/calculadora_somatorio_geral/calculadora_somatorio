import pandas as pd
import os

class ProcessadorDeIndicadores:
    def __init__(self, pasta_dados, colunas_chave, colunas_valor, pesos, formula_calculo):
        """
        Inicializa o processador com:
        - pasta_dados: diretório onde os arquivos CSV estão localizados
        - colunas_chave: lista das colunas comuns para união dos CSVs (ex: ['ano', 'codigo_municipio'])
        - colunas_valor: lista das colunas a serem renomeadas em cada CSV (nome de cada indicador)
        - pesos: dicionário com os pesos de cada coluna de valor
        - formula_calculo: função que calcula a pontuação com base nos valores das colunas e pesos
        """
        self.pasta_dados = pasta_dados
        self.colunas_chave = colunas_chave
        self.colunas_valor = colunas_valor
        self.pesos = pesos
        self.formula_calculo = formula_calculo
    
    def carregar_csvs(self):
        """Carrega e processa todos os CSVs na pasta, unindo-os em um único dataframe."""
        dataframes = []
        
        # Loop pelos arquivos CSV na pasta
        for arquivo in os.listdir(self.pasta_dados):
            if arquivo.endswith('.csv'):
                identificador = os.path.splitext(arquivo)[0]  # Nome único do indicador (excluindo extensão)
                
                # Carrega o CSV e mantém as colunas necessárias
                df = pd.read_csv(os.path.join(self.pasta_dados, arquivo), usecols=self.colunas_chave + self.colunas_valor)
                
                # Renomeia a coluna de valor com o identificador do arquivo
                df = df.rename(columns={self.colunas_valor[0]: identificador})
                
                # Adiciona o dataframe à lista
                dataframes.append(df)
        
        # Realiza o merge dos dataframes com base nas colunas-chave
        df_unificado = dataframes[0]
        for df in dataframes[1:]:
            df_unificado = pd.merge(df_unificado, df, on=self.colunas_chave, how="outer")
        
        return df_unificado
    
    def aplicar_formula(self, df):
        """Aplica a fórmula de cálculo de pontuação para cada linha do dataframe."""
        df['pontuacao'] = df.apply(lambda row: self.formula_calculo(row, self.pesos), axis=1)
        return df
    
    def salvar_csv(self, df, arquivo_saida):
        """Salva o dataframe final em um arquivo CSV."""
        df.to_csv(arquivo_saida, index=False)
        print(f'Arquivo consolidado salvo como {arquivo_saida}')

# Defina a fórmula de pontuação como uma função
def formula_exemplo(row, pesos):
    pontuacao = 0
    for coluna, peso in pesos.items():
        valor = row.get(coluna, 0)
        if pd.notna(valor) and valor != "Não sabe" and valor != "Não possui":
            pontuacao += valor * peso
    return pontuacao

# Configuração dos parâmetros
pasta_dados = 'dados'
colunas_chave = ['ano', 'codigo_municipio']
colunas_valor = ['valor']  # Nome da coluna de valor em cada CSV
pesos = {
    "Barco - MTRA181": 1,
    "Metrô - MTRA182": 3,
    "Mototáxi - MTRA183": 2,
    "Táxi - MTRA184": 2,
    "Trem - MTRA185": 3,
    "Van - MTRA186": 1,
    "Avião - MTRA187": 1,
    "Transporte coletivo por ônibus intramunicipal - MTRA19": 3,
    "Transporte coletivo por ônibus intermunicipal - MTRA23": 1
}
arquivo_saida = 'dados_unificados_com_pontuacao.csv'

# Criação e uso do processador
processador = ProcessadorDeIndicadores(
    pasta_dados=pasta_dados,
    colunas_chave=colunas_chave,
    colunas_valor=colunas_valor,
    pesos=pesos,
    formula_calculo=formula_exemplo
)

# Carrega, processa e salva o resultado
df_unificado = processador.carregar_csvs()
df_com_pontuacao = processador.aplicar_formula(df_unificado)
processador.salvar_csv(df_com_pontuacao, arquivo_saida)
