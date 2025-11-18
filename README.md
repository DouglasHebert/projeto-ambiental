🌿 Sistema de Monitoramento Ambiental Industrial

Disciplina: Inteligência Artificial
Alunos: Douglas Hebert, Natanael Bezerra, Lucas de Souza Morais

📌 Descrição do Projeto

Este projeto implementa um Sistema de Monitoramento Ambiental Industrial, utilizando dados reais de autos de infração do IBAMA.

🔧 A solução inclui:
📥 Ingestão e Pré-processamento de Dados

🧽 Limpeza automática do dataset (84 colunas)

📑 Padronização e tratamento de tipos

💾 Geração de arquivos otimizados em Parquet

🤖 Modelos de Inteligência Artificial

Classificação — RandomForestClassifier

Regressão — RandomForestRegressor

Detecção de Anomalias — IsolationForest

📊 Dashboard Interativo em HTML

Gráficos com Plotly

Mapa com Leaflet

Indicadores e contadores

Lista de anomalias detectadas

🔎 Análises Exploratórias
🧱 Pipeline Modularizado (src/)
🎯 Objetivo do Projeto

Demonstrar, na prática, como aplicar IA em dados ambientais para:

✔️ Apoiar fiscalização
✔️ Identificar comportamentos anômalos
✔️ Gerar insights ambientais

📁 Estrutura do Repositório
projeto-ambiental/
│
├── data/
│   ├── raw/               # dados brutos (CSV do IBAMA)
│   └── processed/         # dados limpos (parquet)
│
├── models/                # modelos treinados (.joblib)
│
├── src/
│   ├── data_ingestion.py
│   ├── preprocessing.py
│   ├── inspect_parquet.py
│   ├── model.py
│   └── generate_dashboard.py
│
├── dashboard.html         # dashboard final
├── requirements.txt
├── .gitignore
└── README.md

🚀 Como Executar o Projeto
1️⃣ Instale as dependências
pip install -r requirements.txt

2️⃣ Adicione o CSV oficial do IBAMA

Coloque o arquivo em:
data/raw/auto_infracao_2024.csv

3️⃣ Execute o pré-processamento
python src/preprocessing.py

Gera arquivos em data/processed/:
clean_autuacoes.parquet
sample_for_dashboard.parquet

4️⃣ Treine os modelos
python src/model.py

Gera arquivos em models/:
preprocessor.joblib
rf_clf.joblib
rf_reg.joblib
iso_forest.joblib

5️⃣ Gere o Dashboard
python src/generate_dashboard.py

Saída:
dashboard.html
👉 Abra no navegador.

🤖 Modelos Utilizados

🔷 Random Forest Classifier
Usado para classificar infrações com base em:
tipo
gravidade
bioma
UF
histórico

🔶 Random Forest Regressor
Prediz valores de multa considerando:
bioma afetado
tipo de infração
estado
variáveis numéricas e categóricas

🟣 Isolation Forest
Detecta infrações fora do padrão — útil para identificar anomalias ambientais.

📊 Dashboard
O dashboard apresenta:
Gráficos Plotly
Mapa Leaflet
Indicadores
Lista de anomalias

Filtros por:
UF
Bioma
Ano

Arquivo final: dashboard.html

🧪 Tecnologias Utilizadas
- Python 3
- Pandas
- Scikit-Learn
- Numpy
- FastParquet
- Plotly
- Folium
- Joblib
