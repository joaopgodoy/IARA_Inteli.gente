import pandas as pd
from ..modules.modules import ProcessadorDeIndicadores
from ..modules.modules import formula_exemplo

# Configuração dos parâmetros
pasta_dados = 'dados'
colunas_chave = ['ano', 'codigo_municipio']
colunas_valor = ['valor']  # Nome da coluna de valor em cada CSV
pesos = {
    "Cadastro de risco - MGRD187": 1,
    "Coordenação municipal de proteção e defesa civil - MGRD212": 1,
    "Mapeamentos de áreas de risco de enchentes ou inundações - MGRD201": 1,
    "Mecanismos de controle e fiscalização para evitar ocupação em áreas suscetíveis aos desastres - MGRD183": 1,
    "Sistema de alerta antecipado de desastre decorrente de escorregamento ou deslizamento de encosta - MGRD206": 1,
    "Sistema de alerta antecipado de desastres decorrente de enchentes, inundações ou enxurradas - MGRD186": 1,
    "Sistema de alerta antecipado de desastres de defesa civil - MGRD2213": 1
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