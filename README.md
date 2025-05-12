# Ingestão de dados XML para S3 e athena

O objetivo do projeto é trazer dados em xml do banco SQL SERVER, salvar
na camada `raw` o xml. Depois converter o xml para arquivo `parquet` 
e por ultimo inserir em uma tabela `ICEBERG` no `Athena`.