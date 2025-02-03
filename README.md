# IARA_Inteli.gente
Projeto de iniciação científica de coleta e análise de dados para a plataforma inteli.gente

* Para processar todos os dados, a partir da pasta do repositório no terminal, digite:

```python
python3 -m indicadores.main
```

* Para processar indicadores selecionados, digite:

```python
python3 -m indicadores.main indicadores/dir/file.py [indicadores/dir/file.py] ...
```

Onde `dir` se refere a qualquer pasta do projeto, como `ecom` ou `environ`, e `file` se refere ao arquivo do indicador, como `3124.py`.

A seleção de indicadores aceita múltiplos argumentos no `argv`.

A saída será um arquivo .csv `processed_data` com as colunas de indentificação do município e do ano e com as colunas dos indicadores calculados.