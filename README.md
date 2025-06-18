🛢️ Mercado Brasileiro de Combustíveis Líquidos  
📊 Fonte dos Dados  
Os dados utilizados neste projeto foram extraídos do Painel Dinâmico do Mercado Brasileiro de Combustíveis Líquidos, disponibilizado pela Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP).  

O painel fornece informações atualizadas periodicamente (dias 1º e 20 de cada mês) sobre a comercialização, distribuição e localização geográfica de agentes do setor de combustíveis líquidos no Brasil. A base de dados é pública e gratuita.  

🎯 Objetivo  
Este projeto tem como objetivo extrair, padronizar, limpar, transformar e analisar os dados da ANP utilizando PySpark, com foco em tratar grandes volumes de dados e gerar insights sobre o consumo e a distribuição de combustíveis líquidos no Brasil.  

🗂️ Estrutura do Projeto  
📁 Datalake (Armazenamento em camadas)  
landing/ — Dados brutos originais (.csv) extraídos da fonte oficial.  

bronze/ — Dados convertidos para Parquet com padronização inicial.  

silver/ — Dados limpos, tratados e normalizados.  

gold/ — Dados agregados e prontos para análise.  

⚙️ Workflow (Pipeline de Processamento)  
Organização modular dos scripts em PySpark, por etapas do processo:  

Extração dos arquivos  

Padronização de colunas   

Limpeza e tratamento de dados  

Análises e geração de indicadores  

🧪 Passo a Passo Técnico  
1. 🧾 Extração e Armazenamento Inicial (Landing Zone)  
Bibliotecas utilizadas:

requests — download automático dos arquivos.

zipfile — descompactação dos arquivos .zip.

os — manipulação de diretórios locais.

urllib3 — desativação de avisos SSL (problemas recorrentes no site da ANP).

Objetivo:
Baixar e extrair os dados originais, armazenando na pasta landing/.

2. 📦 Padronização e Conversão (Bronze Zone)
Bibliotecas utilizadas:

pyspark.sql.SparkSession — sessão Spark para processamentos distribuídos.

unicodedata — remoção de acentos e caracteres especiais.

os — iteração sobre arquivos locais.

Objetivo:
Padronizar os nomes das colunas e converter os arquivos CSV em Parquet, otimizando o desempenho para big data.

3. 🧹 Limpeza e Normalização (Silver Zone)
Bibliotecas utilizadas:

pyspark.sql.functions (to_date, when, col, etc.) — tratamento de nulos, datas e duplicidades.

pyspark.sql.SparkSession — continuidade do processamento.

Objetivo:
Remover linhas inválidas ou duplicadas e ajustar os tipos de dados, garantindo a integridade e consistência da base.

4. 📈 Análise e Indicadores (Gold Zone)
Bibliotecas utilizadas:

pyspark.sql.functions (regexp_replace, sum, col, etc.) — transformações e agregações.

pyspark.sql.SparkSession — execução distribuída.

Objetivo:
Gerar insights a partir da base tratada, como:

Tendência de consumo por tipo de produto e por ano;

Ranking de estados e municípios com maior volume de comercialização;

Identificação de polos regionais de distribuição e consumo;

Ranking dos produtos mais consumidos ao longo do tempo.


1. 📂 Estrutura de Diretórios
bash
Copy
Edit
workflow/  
│  
├── ingestion/  
│   └── bronze_loader.py  
│  
├── process/  
│   ├── silver_cleaner.py  
│   └── gold_transformer.py  
│  
├── utils/
│   ├── cleaner.py            # Funções reutilizáveis (como limpar_colunas)
│   └── paths.py              # Constantes com caminhos
