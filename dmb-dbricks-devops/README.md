# Introduction

O cliente apresentou a necessidade de ingerir dados do AzureDevOps para tomada de decisão estratégica e acompanhamento personalizado dos diversos projetos que estavam sendo realizados na companhia, no contexto inicial os dados eram retirados via csv do azure devops e inputados diretamente no powerBI, devido a escala dos projetos, esse passou a ser um processo moroso do qual eram necessárias 4 pessoas e entre 2 a 3 dias para consolidacao e disponibilizacao das informacoes. O requisito principal era que a solucao fosse desenvolvida na azure, permitindo processamentos diários e atualizacao dos dados no DW corporativo.

A arquitetura proposta para esse projeto pode ser observada abaixo:

![Reference](dmb-dbricks-devops/img/ArchP1.png)

Primeiramente criei uma arquitetura simplificada para leitura de dados da API utilizando databricks para ingestao e data factory para orquestracao e datadog para monitoramento do ambiente.

![Reference](dmb-dbricks-devops/img/datalake.png)

Para datalake utilizei a seguinte estratégia: 

- **Raw Layer**: In this layer, raw data is stored without alterations, allowing for source data tracking and maintaining data integrity. Original format of data (json).

- **Standardized Layer**: Data undergoes a transformation and cleaning process in this layer. It is standardized and typed to ensure high quality and ease of use. Parquet format.

- **Consume Layer**: Ready-to-use data is stored here, ready for querying and analysis, saving processing time whenever someone accesses the data. Parquet format.

Utilizei parquet para que pudesse ativar a feature de life cicle manager no datalake pois o dado teria um ciclo de vida de 90 dias, apos esse periodo poderia ser arquivado para diminuir custos operacionais no datalake.

### 1. Data Collection

- Utilizes highly effective Python scripts to extract detailed data from Azure DevOps APIs, including information from the following areas:
  - Work Items
  - Work Item History
  - Board Locations
  - Iterations
  - Processes
  - Projects
  - Teams
  - Users
  - Work Item Links
  - Work Item Revisions

Para esse case apresentarei apenas WorkItem pois toda coleta restante segue a mesma sequencia.

### 1. Organizacao do Projeto

O projeto está organizado da seguinte forma:

Access:
  Pasta contendo os arquivos:
    mount_ADLS.py - Este código monta um sistema de arquivos do Azure Data Lake Storage Gen2 no Databricks, usando OAuth para autenticação. As credenciais são obtidas de um Key Vault, permitindo acessar os dados no caminho especificado e montá-los no ponto `/mnt` para facilitar o acesso.

    Connections_Variable - Este código estabelece conexões com um banco de dados SQL Server e a API do Azure DevOps, configurando variáveis e funções para buscar e processar dados do DevOps, além de preparar caminhos no Data Lake e definir funções auxiliares para atualização de logs e controle de carga de dados, embora algumas funções não estejam em uso atualmente.
DevOps:
  raw
  standardized
  consume
  
  O repositorio devops contem todo o projeto que será executado e orquestrado no data factory.
  ![Reference](dmb-dbricks-devops/img/datafactory.png)

### 2. Conclusion

Ao final do projeto pode-se observouse os seguintes beneficios:

Reduction of data processing time from the API by up to 95%.

Data writing of tables using merge instead of drop table, ensuring that the table is always populated.

The POWERBI load process will be related only to database loading and will no longer be triggered by POWERBI itself.

Separation of subjects to allow different execution times for each subject.

Para mais informacoes, veja o projeto completo em: https://github.com/diegomendesbrasil/azure-dbricks-data-devops.git