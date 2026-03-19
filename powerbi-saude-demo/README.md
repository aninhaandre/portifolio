# Power BI (Demo) – Saúde

Este material é um **modelo funcional de dashboard** para você montar no Power BI rápido.

O repositório inclui:
- `data/healthcare_daily.csv` (dados sintéticos por dia e por setor)
- `DAX/measures.txt` (medidas prontas)
- `DAX/modeling-notes.md` (passos para importar e criar a estrutura no Power BI)

## Como montar no Power BI Desktop

1. Abra o **Power BI Desktop**.
2. Clique em **Obter dados** → **CSV** e selecione `data/healthcare_daily.csv`.
3. No Power Query:
   - Garanta que a coluna `date` está como **Data**.
   - Garanta que `department` é **Texto**.
   - Para colunas numéricas, mantenha como **Número**.
4. Volte para a tela principal e crie medidas DAX copiando de `DAX/measures.txt`.

## Páginas sugeridas (para ficar com cara de portfólio)

1. **Visão Geral**
   - Cartões: `Total Visits`, `Avg Wait (min)`, `Readmission Rate`
   - Gráfico de linhas: `Visits` por data
2. **Atendimentos por Setor**
   - Barras: `Visits` por `department`
3. **Indicadores Operacionais**
   - Gráfico: `Admissions` e `Discharges` por data
   - Gráfico: `Avg Length of Stay (days)` por data

## Observações

- Os dados são sintéticos (para fins de portfólio). Se você tiver dados reais, troque apenas o CSV mantendo a mesma modelagem.
- Se quiser, eu adapto o modelo para o tipo exato de dashboard que você montava no trabalho (ex.: UTI, internações, exames, SLA de atendimento, etc.).

