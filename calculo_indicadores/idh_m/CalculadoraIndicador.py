from typing import Callable
from dataclasses import dataclass
import pandas as pd


@dataclass
class CalculadoraIndicador:
    nome_indicador:str # nome na tabela de requisitos
    id_indicador:int # id do indicador no BD
    funcao_calcula_valor: Callable[[pd.DataFrame],pd.DataFrame]
    funcao_calcula_nota: Callable

