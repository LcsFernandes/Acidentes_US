### Projeto de ETL utilizando PySpark

Este projeto é uma implementação de ETL (Extract, Transform, Load) utilizando a biblioteca PySpark, que é uma interface Python para o framework de processamento distribuído Apache Spark. O objetivo deste projeto é extrair dados, transformá-los e carregá-los em um destino, tudo isso de forma distribuída e eficiente.

#### Estrutura do Projeto:

- **Data:** Esta pasta contém o arquivo de dados transformados no formato Parquet. (Acidentes.parquet)

- **Dependencies:**
  - **Logging.py:** Este arquivo contém uma classe Log4j que fornece métodos para log de diferentes níveis (error, warn, info). Ele é utilizado para configurar e gerenciar logs no ambiente Spark.
  - **Spark.py:** Aqui é definida a função start_spark, responsável por iniciar uma sessão Spark. Esta função configura a sessão Spark com parâmetros como nome da aplicação, mestre, pacotes JAR, arquivos, configurações Spark, etc. Além disso, ela configura o logger Spark utilizando a classe Log4j.

- **Job:**
  - **etl_job.py:** Este arquivo contém o código principal do processo de ETL. Aqui são definidas as etapas de extração, transformação e carregamento dos dados. É onde as operações de leitura, transformação e escrita são realizadas utilizando as funcionalidades fornecidas pelo PySpark.

- #### Test:

  - **test_etl_job.py:** Conjunto de testes unitários para validar o funcionamento da transformação de dados definida no arquivo etl_job.py
  - **log4j.properties:** Este arquivo contém as configurações do log4j, que é um framework de logging utilizado para registrar mensagens de log no ambiente Spark.

  - **testes_comando.ipynb:** Este arquivo Jupyter Notebook foi utilizado para testar o processo de ETL. Ele pode conter exemplos de como usar o job de ETL, carregar dados, executar transformações, etc.


#### Funcionamento do Projeto:
1. **Extração:** Os dados são extraídos de uma base de dados do [Kaggle](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents).

2. **Transformação:** Os dados extraídos são transformados conforme a lógica definida na função transform_data no arquivo etl_job.py. Isso pode incluir limpeza de dados, agregações, conversões de formato, entre outras operações.

3. **Carregamento:** Os dados transformados são carregados no diretório Data na raiz do projeto.

#### Testes:
- Os testes unitários definidos no arquivo test_etl_job.py garantem que a função transform_data está produzindo os resultados esperados, verificando se o número de colunas e linhas está correto após a transformação, bem como as médias de valores específicos.

#### Executando o Projeto:
- O projeto pode ser executado seguindo os seguintes passos:
 1. Configurar um ambiente com Apache Spark e PySpark.
 2. Executar o arquivo etl_job.py para realizar o processo de ETL.
 3. Executar os testes definidos em test_etl_job.py para verificar se a transformação está produzindo os resultados esperados.

#### Conclusão:
Este projeto fornece estrutura para realizar um processo de ETL utilizando PySpark. Ele pode ser expandido para lidar com volumes maiores de dados, integrar com fontes de dados adicionais e incorporar mais etapas de transformação conforme necessário para atender aos requisitos específicos do projeto.
