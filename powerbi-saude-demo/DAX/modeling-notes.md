# Modelagem (Power BI)

## 1) Importação

- Tabela: `healthcare_daily.csv`
- Colunas:
  - `date` (Data)
  - `department` (Texto)
  - `visits`, `admissions`, `discharges`, `readmissions` (Inteiros/Números)
  - `avg_wait_minutes`, `avg_length_stay_days` (Números)

## 2) Ajustes recomendados

1. Defina `date` como tipo **Data**.
2. Crie uma coluna de ano/mês (opcional, mas facilita segmentações).
   - `year = YEAR([date])`
   - `month = FORMAT([date], "YYYY-MM")`
3. Crie medidas conforme `DAX/measures.txt`.

## 3) Dicas de visual

- Cards/KPI: `Total Visits`, `Avg Wait (min)`, `Readmission Rate`
- Linha: `Visits` ao longo de `date`
- Barras: `Total Visits` por `department`

