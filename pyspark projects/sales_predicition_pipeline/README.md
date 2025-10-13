# 📈 Projeto PySpark - Previsão de Vendas

Este projeto implementa um **pipeline completo de previsão de vendas em PySpark**, usando regressão linear e features temporais (mês, dia da semana, feriado, média móvel).

---

## 🧱 Estrutura

projeto_previsao_vendas/
│
├── data/ # Dados de entrada
├── logs/ # Logs de execução
├── modelos/ # Modelos salvos
├── output/ # Saída (previsões)
└── src/ # Código-fonte

yaml
Copy code

---

## 🚀 Etapas do Pipeline

1. **Extração de Dados Históricos** – Leitura do arquivo CSV.
2. **Criação de Features Temporais** – Mês, dia da semana, fim de semana, média móvel e feriado.
3. **Treinamento** – Modelo de regressão linear (`LinearRegression` do PySpark ML).
4. **Avaliação** – RMSE e R².
5. **Armazenamento** – Modelo e previsões salvos em disco.

---

## 🧠 Exemplo de Execução

```bash
spark-submit src/pipeline_previsao_vendas.py
Saída esperada:

makefile
Copy code
RMSE: 25.30
R²: 0.87
✅ Pipeline executado com sucesso!
📊 Extensões Futuras
Substituir regressão linear por RandomForestRegressor

Adicionar variáveis de preço e promoções

Automatizar execução com Airflow

Autor: Sthefany Spina
Tecnologias: PySpark, MLlib, Parquet, Logging