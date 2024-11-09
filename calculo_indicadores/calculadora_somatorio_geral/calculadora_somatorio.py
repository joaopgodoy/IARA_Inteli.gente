import pandas as pd
from ..modules.modules import ProcessadorDeIndicadores
from ..modules.modules import formula_exemplo

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
