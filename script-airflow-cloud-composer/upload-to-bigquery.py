from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import regexp_replace, when,length,to_date,upper,lower,col,split,explode,coalesce,concat_ws,concat,lit,broadcast,regexp_extract,month,year,to_date
from pyspark.sql.functions import broadcast,expr,udf
from pyspark.sql.types import *
from functools import reduce
import logging

# Definindo a sessão do Spark
spark = SparkSession.builder.appName("UploadBigQuery").getOrCreate()

# Definindo os schemas:
estabelecimentos = StructType([
        StructField("CNPJ_BASICO", StringType(), nullable=True),
        StructField("CNPJ_ORDEM", StringType(), nullable=True),
        StructField("CNPJ_DV", StringType(), nullable=True),
        StructField("MATRIZ_FILIAL", StringType(), nullable=True),
        StructField("NOME_FANTASIA", StringType(), nullable=True),
        StructField("SIT_CADASTRAL", IntegerType(), nullable=True),
        StructField("DT_SIT_CADASTRAL", StringType(), nullable=True),
        StructField("MOTIVO_CADASTRAL", StringType(), nullable=True),
        StructField("NOME_CIDADE_EXTERIOR", StringType(), nullable=True),
        StructField("PAIS", StringType(), nullable=True),
        StructField("DT_INICIO_ATIVIDADE", StringType(), nullable=True),
        StructField("CNAE_1", StringType(), nullable=True),
        StructField("CNAE_2", StringType(), nullable=True),
        StructField("TIPO_LOUGRADOURO", StringType(), nullable=True),
        StructField("LOGRADOURO", StringType(), nullable=True),
        StructField("NUMERO", IntegerType(), nullable=True),
        StructField("COMPLEMENTO", StringType(), nullable=True),
        StructField("BAIRRO", StringType(), nullable=True),
        StructField("CEP", IntegerType(), nullable=True),
        StructField("UF", StringType(), nullable=True),
        StructField("MUNICIPIO", StringType(), nullable=True),
        StructField("DDD1", StringType(), nullable=True),
        StructField("TEL1", StringType(), nullable=True),
        StructField("DDD2", StringType(), nullable=True),
        StructField("TEL2", StringType(), nullable=True),
        StructField("DDD_FAX", IntegerType(), nullable=True),
        StructField("FAX", IntegerType(), nullable=True),
        StructField("EMAIL", StringType(), nullable=True),
        StructField("SIT_ESPECIAL", StringType(), nullable=True),
        StructField("DT_SIT_ESPECIAL", StringType(), nullable=True)])

empresas = StructType([
        StructField("CNPJ", StringType(), nullable=True),
        StructField("NOME_EMPRESA", StringType(), nullable=True),
        StructField("COD_NAT_JURICA", StringType(), nullable=True),
        StructField("QUALIF_RESPONVAVEL", StringType(), nullable=True),
        StructField("CAP_SOCIAL", StringType(), nullable=True),
        StructField("PORTE", StringType(), nullable=True),
        StructField("ENTE_FEDERATIVO", StringType(), nullable=True)])

municipios = StructType([
        StructField("ID_MUNICPIO", StringType(), nullable=True),
        StructField("MUNICIPIO", StringType(), nullable=True)])

cnaes = StructType([
        StructField("COD_CNAE", StringType(), nullable=True),
        StructField("CNAE", StringType(), nullable=True)])
    
paises = StructType([
        StructField("COD_PAIS", StringType(), nullable=True),
        StructField("NM_PAIS", StringType(), nullable=True)])
    
qualificacoes = StructType([
        StructField("COD_QUALIFICACAO", StringType(), nullable=True),
        StructField("NM_QUALIFICACAO", StringType(), nullable=True)])

socios = StructType([
        StructField("CNPJ_BASICO", StringType(), nullable=True),
        StructField("IDENTIFICADOR_SOCIO", IntegerType(), nullable=True),
        StructField("NOME_SOCIO_RAZAO_SOCIAL", StringType(), nullable=True),
        StructField("CNPJ_CPF_SOCIO", StringType(), nullable=True),
        StructField("QUALIFICAÇAO_SOCIO", StringType(), nullable=True),
        StructField("DATA_ENTRADA_SOCIEDADE", StringType(), nullable=True),
        StructField("PAIS", StringType(), nullable=True),
        StructField("REPRESENTANTE_LEGAL", StringType(), nullable=True),
        StructField("NOME_REPRESENTANTE", StringType(), nullable=True),
        StructField("QUALIFICACAO_REPRESENTANTE_LEGAL", StringType(), nullable=True),
        StructField("FAIXA_ETARIA", StringType(), nullable=True)])

simples = StructType([
        StructField("CNPJ_BASICO", StringType(), nullable=True),
        StructField("OPCAO_PELO_SIMPLES", StringType(), nullable=True),
        StructField("DATA_OPCAO_PELO_SIMPLES", StringType(), nullable=True),
        StructField("DATA_EXCLUSAO_SIMPLES", StringType(), nullable=True),
        StructField("OPÇAO_PELO_MEI", StringType(), nullable=True),
        StructField("DATA_OPCAO_PELO_MEI", StringType(), nullable=True),
        StructField("DATA_EXCLUSAO_MEI", StringType(), nullable=True)])

naturezas = StructType([
        StructField("COD_NAT_JURICA", StringType(), nullable=True),
        StructField("NAT_JURICA", StringType(), nullable=True)])
    
motivos = StructType([
        StructField("COD_MOTIVO", StringType(), nullable=True),
        StructField("NM_MOTIVO", StringType(), nullable=True)])
    
# Definindo os schemas:
schemas = {"Estabelecimentos": estabelecimentos,
            "Empresas": empresas,
            "Municipios": municipios,
            "Cnaes": cnaes,
            "Socios": socios,
            "Simples": simples,
            "Naturezas": naturezas,
            "Qualificacoes": qualificacoes,
            "Motivos": motivos,
            "Paises": paises}

# Mapeamento dos encodings
encodings = {
    'Empresas': 'ascii',
    'Naturezas': 'ISO-8859-1',
    'Qualificacoes': 'ISO-8859-1',
    'Estabelecimentos': 'ascii',
    'Paises': 'ISO-8859-1',
    'Municipios': 'ascii',
    'Cnaes': 'ISO-8859-1',
    'Motivos': 'ascii',
    'Simples': 'ascii'
}

def read_data(spark, schema_name, base_path, encoding):
    file_pattern = f"{base_path}/*"
    
    # Utilizando o Spark para ler os arquivos do GCS
    df = (spark.read.format("csv")
          .option("sep", ";")
          .option("header", "false")
          .option('quote', '"')
          .option("escape", '"')
          .option("encoding", encoding)
          .schema(schemas[schema_name])
          .load(file_pattern))
    
    return df


def save_to_bigquery(df, dataset_name, table_name, bucket_name):
    """
    Salva o DataFrame no BigQuery.
    
    Args:
    df: DataFrame do Spark a ser salvo.
    dataset_name: Nome do dataset no BigQuery.
    table_name: Nome da tabela no BigQuery.
    bucket_name: Nome do bucket do GCS usado para armazenamento temporário.
    """
    full_table_name = f"{dataset_name}.{table_name}"
    temp_gcs_bucket = f"{bucket_name}/temp_bq"
    
    (df.write.format('bigquery')
     .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
     .option('table', full_table_name)
     .option("temporaryGcsBucket", temp_gcs_bucket)
     .mode("overwrite")
     .save())
    

datasets = ['Municipios', 'Cnaes', 'Naturezas', 'Simples', 'Qualificacoes', 'Paises', 'Motivos', 'Simples', 'Socios', 'Empresas', 'Estabelecimentos']

for dataset in datasets:
    logging.info(f"upload do dataset {datasets} iniciado")
    base_path = f"gs://projeto-dados-receita-federal/{dataset}/arquivo_deszipado"
    logging.info(f"Carregamento dos dados de {datasets}")
    df = read_data(spark, dataset, base_path, encodings[dataset])
    logging.info(f"Enviando para o Bigquery")
    save_to_bigquery(df, 'receitafederal', dataset.lower(), 'projeto-dados-receita-federal')
    logging.info(f"upload do dataset {datasets} finalizado")

spark.stop()
