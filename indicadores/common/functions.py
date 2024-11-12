import pandas as pd

def weighted_sum(row, weights) -> int:
    total = 0

    for coluna, weight in weights.items():
        valor = row.get(coluna, 0)
        if pd.notna(valor) and valor not in ["Não sabe", "Não possui"]:
            total += valor * weight
            
    return total