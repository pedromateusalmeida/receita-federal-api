
# Pacotestes
import logging
from google.cloud import storage
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import gcsfs
import zipfile
from io import BytesIO
from pyspark.sql import SparkSession
import io

import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import regexp_replace, when,length,to_date,upper,lower,col,split,explode,coalesce,concat_ws,concat,lit,broadcast,regexp_extract,month,year,to_date
from pyspark.sql.functions import broadcast,expr,udf
from pyspark.sql.types import *
from functools import reduce

class ReceitaCNPJApiGCP:
    """
    Classe para interagir com a API de Dados Abertos da Receita Federal e salvar diretamente no Google Cloud Storage (GCS).

    Esta classe facilita a obtenção de dados relacionados a CNPJs e o armazenamento desses dados no GCS, implementando funcionalidades
    para verificar a atualização dos dados, realizar tentativas de download com intervalos de espera e executar downloads de forma
    assíncrona utilizando ThreadPoolExecutor.

    Atributos:
        BASE_URL (str): URL base para a API de Dados Abertos da Receita Federal.
        bucket_name (str): Nome do bucket no Google Cloud Storage onde os arquivos serão salvos.
        data_update (bool): Determina se a função deve verificar se os dados foram atualizados nos últimos 30 dias.
        max_attempts (int): Número máximo de tentativas de download.
        wait_time (int): Tempo de espera entre tentativas em segundos.
        logger (Logger): Logger para registrar atividades e erros.
        storage_client (Client): Cliente do Google Cloud Storage para interação com o GCS.

    Métodos:
        __init__(self, bucket_name, data_update=True, max_attempts=15, wait_time=150):
            Inicializa a classe com configurações específicas para interação com a API e o GCS.

        get_last_update_date(self):
            Verifica e retorna a última data de atualização dos dados na API da Receita Federal.

        should_update_data(self):
            Avalia se os dados necessitam ser atualizados com base na última data de atualização.

        lista_urls_receita(self, *prefixes):
            Gera uma lista de URLs para download com base nos prefixos de arquivo especificados.

        download_and_upload_to_gcs(self, url, destination_blob_name):
            Realiza o download de um arquivo da URL especificada e o salva diretamente no bucket do GCS.

        download_files_concurrently(self, urls):
            Utiliza ThreadPoolExecutor para baixar arquivos de uma lista de URLs fornecida de forma assíncrona.
    """

    BASE_URL = "https://dadosabertos.rfb.gov.br/CNPJ"
    FILE_PREFIXES = ['Estabelecimentos', 'Municipios', 'Simples', 'Empresas', 'Cnaes', 'Socios', 'Naturezas', 'Qualificacoes', 'Paises', 'Motivos']

    def __init__(self, bucket_name, data_update=True, max_attempts=15, wait_time=180):
        """
        Inicializa a classe com configurações para interação com a API da Receita Federal e o Google Cloud Storage.

        Parâmetros:
            bucket_name (str): Nome do bucket no GCS onde os arquivos serão salvos.
            data_update (bool): Se True, verifica se os dados foram atualizados nos últimos 30 dias antes de baixar.
            max_attempts (int): Número máximo de tentativas de download para cada arquivo.
            wait_time (int): Tempo de espera (em segundos) entre as tentativas de download.
        """
        self.bucket_name = bucket_name
        self.data_update = data_update
        self.max_attempts = max_attempts
        self.wait_time = wait_time
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        self.storage_client = storage.Client()

    def get_last_update_date(self):
        """
        Consulta a API da Receita Federal para determinar a última data de atualização dos dados disponíveis.

        Retorna:
            datetime.date: A última data de atualização dos dados, ou None se não puder ser determinada.
        """
        response = requests.get(self.BASE_URL)
        if response.status_code == 200:
            last_update_date = datetime.today().date()  # Placeholder para a data real
            return last_update_date
        else:
            self.logger.error("Não foi possível verificar a data da última atualização.")
            return None

    def should_update_data(self):
        """
        Determina se é necessário atualizar os dados com base na última data de atualização.

        Retorna:
            bool: True se os dados precisam ser atualizados, False caso contrário.
        """
        if not self.data_update:
            return True
        last_update_date = self.get_last_update_date()
        if last_update_date and (datetime.today().date() - last_update_date <= timedelta(days=30)):
            return True
        else:
            return False

    def lista_urls_receita(self, *prefixes):
        """
        Gera uma lista de URLs para download com base nos prefixos fornecidos. As URLs são 
        determinadas com base nos prefixos de arquivo conhecidos. Se nenhum prefixo for 
        fornecido, o método gerará URLs para todos os tipos de arquivos conhecidos.

        Args:
            *prefixes (str): Prefixos de arquivos para os quais as URLs serão geradas.

        Returns:
            list: Lista de URLs completas para os arquivos correspondentes aos prefixos.
        """
        urls = []

        # Usa todos os prefixos de arquivo conhecidos se nenhum prefixo específico for fornecido
        if not prefixes:
            prefixes = self.FILE_PREFIXES

        for prefix in prefixes:
            # Para prefixos específicos que sabemos terem apenas um arquivo associado
            if prefix in ['Municipios', 'Cnaes', 'Naturezas', 'Simples', 'Qualificacoes', 'Paises', 'Motivos']:
                urls.append(f"{self.BASE_URL}/{prefix}.zip")
            # Para prefixos que potencialmente têm múltiplos arquivos (identificados por índices de 0 a 9)
            elif prefix in ['Estabelecimentos', 'Empresas', 'Socios']:
                for i in range(10):  # Ajuste o range se necessário
                    urls.append(f"{self.BASE_URL}/{prefix}{i}.zip")
            else:
                self.logger.warning(f"Prefixo '{prefix}' não reconhecido. Verifique a lista de prefixos válidos.")

        return urls

    def download_and_upload_to_gcs(self, url, destination_blob_name):
        """
        Baixa um arquivo da URL especificada e o salva diretamente em um bucket do GCS.

        Parâmetros:
            url (str): URL do arquivo para download.
            destination_blob_name (str): Localização (caminho/nome) no bucket do GCS onde o arquivo será salvo.
        """
        for attempt in range(self.max_attempts):
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                bucket = self.storage_client.bucket(self.bucket_name)
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_string(response.content)
                self.logger.info(f"Arquivo {url} baixado e salvo em {destination_blob_name} com sucesso.")
                return
            except requests.RequestException as e:
                self.logger.error(f"Tentativa {attempt + 1} de download falhou: {e}")
                if attempt < self.max_attempts - 1:
                    time.sleep(self.wait_time)
                else:
                    self.logger.error(f"Erro ao salvar o arquivo {destination_blob_name}: {e}")

    def download_files_concurrently(self, urls, prefix):
        """
        Baixa arquivos de forma assíncrona a partir de uma lista de URLs e os salva no GCS.

        Parâmetros:
            urls (list[str]): Lista de URLs dos arquivos a serem baixados.
            prefix (str): Prefixo do diretório para salvar os arquivos no bucket.
        """
        if self.should_update_data():
            with ThreadPoolExecutor(max_workers=5) as executor:
                # Certifique-se de usar prefix como um único diretório, não a primeira letra
                future_to_url = {executor.submit(self.download_and_upload_to_gcs, url, f'{prefix}/' + url.split('/')[-1]): url for url in urls}
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"Erro ao processar {url}: {e}")
        else:
            self.logger.info("Dados não necessitam atualização.")


if __name__ == "__main__":
    bucket_name = "projeto-dados-receita-federal"
    api = ReceitaCNPJApiGCP(bucket_name=bucket_name, data_update=True, max_attempts=15, wait_time=180)
    prefix = prefix = ['Municipios', 'Cnaes', 'Naturezas', 'Simples', 'Qualificacoes', 'Paises', 'Motivos']
    for i in prefix:
        urls = api.lista_urls_receita(f'{i}')
        api.download_files_concurrently(urls, f'{i}')

# Exemplo de uso
if __name__ == "__main__":
    bucket_name = "projeto-dados-receita-federal"
    api = ReceitaCNPJApiGCP(bucket_name=bucket_name, data_update=True, max_attempts=15, wait_time=180)
    prefix = prefix = ['Socios']
    for i in prefix:
        urls = api.lista_urls_receita(f'{i}')
        api.download_files_concurrently(urls, f'{i}')

# Exemplo de uso
if __name__ == "__main__":
    bucket_name = "projeto-dados-receita-federal"
    api = ReceitaCNPJApiGCP(bucket_name=bucket_name, data_update=True, max_attempts=15, wait_time=180)
    prefix = prefix = ['Empresas']
    for i in prefix:
        urls = api.lista_urls_receita(f'{i}')
        api.download_files_concurrently(urls, f'{i}')


# Exemplo de uso
if __name__ == "__main__":
    bucket_name = "projeto-dados-receita-federal"
    api = ReceitaCNPJApiGCP(bucket_name=bucket_name, data_update=True, max_attempts=15, wait_time=180)
    prefix = prefix = ['Estabelecimentos']
    for i in prefix:
        urls = api.lista_urls_receita(f'{i}')
        api.download_files_concurrently(urls, f'{i}')


def extract_file_by_condition_from_zip_in_gcs(bucket_name, zip_file_path, output_path, condition_func=None):
    """
    Extrai um arquivo baseado em uma condição específica de um arquivo zip armazenado no GCS.
    """
    # Inicializa o sistema de arquivos GCS
    fs = gcsfs.GCSFileSystem()

    # Caminho completo do arquivo zip no GCS
    full_zip_file_path = f"{bucket_name}/{zip_file_path}"
    
    # Lê o arquivo zip do GCS
    with fs.open(full_zip_file_path, 'rb') as f:
        with zipfile.ZipFile(BytesIO(f.read()), 'r') as z:
            # Lista todos os arquivos no arquivo zip e aplica a condição, se fornecida
            file_list = z.namelist()
            for file_name in file_list:
                if condition_func is None or condition_func(file_name):
                    # Extrai o arquivo para a memória
                    with z.open(file_name) as specific_file:
                        data = specific_file.read()
                        
                        # Define o nome do arquivo de saída (ajuste conforme necessário)
                        output_file_name = file_name.split('/')[-1]

                        # Salva o arquivo extraído no GCS
                        with fs.open(f"{output_path}/{output_file_name}", 'wb') as f_out:
                            f_out.write(data)
                    print(f"File {file_name} extracted and saved to {output_path}")
                    break  # Remova ou ajuste conforme a necessidade de extrair mais arquivos
            else:
                print("No files found matching the condition.")

def extract_files_by_prefixes_from_zips_in_gcs(bucket_name, prefixes, base_output_path, condition_func=None):
    """
    Extrai arquivos baseados em uma condição específica de vários arquivos zip armazenados no GCS,
    cada um correspondendo a um prefixo na lista fornecida.
    """
    for prefix in prefixes:
        zip_file_path = f'{prefix}/{prefix}.zip'
        # Constrói o caminho de saída diretamente usando o prefixo
        output_path = f"{base_output_path}/{prefix}/arquivo_deszipado"
        try:
            extract_file_by_condition_from_zip_in_gcs(
                bucket_name=bucket_name,
                zip_file_path=zip_file_path,
                output_path=output_path,
                condition_func=condition_func
            )
            print(f"Completed extraction for prefix: {prefix}")
        except Exception as e:
            print(f"Error extracting files for prefix {prefix}: {e}")


# Configuração dos prefixos e chamada das funções
prefixes = ['Municipios', 'Cnaes', 'Naturezas', 'Qualificacoes', 'Paises', 'Motivos','Simples']
bucket_name = 'projeto-dados-receita-federal'
base_output_path = 'projeto-dados-receita-federal'  # Caminho base ajustado

# Executa a extração
extract_files_by_prefixes_from_zips_in_gcs(
    bucket_name=bucket_name,
    prefixes=prefixes,
    base_output_path=base_output_path,
    condition_func=None  # Pode ser substituído por uma função específica se necessário
)


# Deszipando de forma distribuida 
# Função para extrair arquivos de um zip e salvar no GCS
def extract_and_save_file(zip_bytes_io, file_name, gcs_output_path):
    with zipfile.ZipFile(zip_bytes_io) as z:
        with z.open(file_name) as specific_file:
            data = specific_file.read()
            # Escrever no GCS usando gcsfs
            fs = gcsfs.GCSFileSystem()
            with fs.open(gcs_output_path, 'wb') as f_out:
                f_out.write(data)
            print(f"File {file_name} extracted and saved to {gcs_output_path}")

# Inicializa a sessão do Spark
spark = SparkSession.builder.appName("ExtractZipFiles").getOrCreate()

# Definição de variáveis
bucket_name = 'projeto-dados-receita-federal'
prefixes = ['Empresas']

# RDD para processar arquivos
for prefix in prefixes:
    for i in range(10):
        zip_file_path = f'gs://{bucket_name}/{prefix}/{prefix}{i}.zip'
        output_path = f'gs://{bucket_name}/{prefix}/arquivo_deszipado/'
        
        # Lê o arquivo zip como bytes
        rdd = spark.sparkContext.binaryFiles(zip_file_path)
        
        # Processa cada arquivo no zip
        def process_zip_file(file_data):
            zip_bytes_io = io.BytesIO(file_data[1])
            with zipfile.ZipFile(zip_bytes_io, 'r') as z:
                file_list = z.namelist()
                for file_name in file_list:
                    extract_and_save_file(zip_bytes_io, file_name, output_path + file_name)
        
        # Aplica a função a cada arquivo no RDD
        rdd.foreach(process_zip_file)



# Definição de variáveis
bucket_name = 'projeto-dados-receita-federal'
prefixes = ['Socios']

# RDD para processar arquivos
for prefix in prefixes:
    for i in range(10):
        zip_file_path = f'gs://{bucket_name}/{prefix}/{prefix}{i}.zip'
        output_path = f'gs://{bucket_name}/{prefix}/arquivo_deszipado/'
        
        # Lê o arquivo zip como bytes
        rdd = spark.sparkContext.binaryFiles(zip_file_path)
        
        # Processa cada arquivo no zip
        def process_zip_file(file_data):
            zip_bytes_io = io.BytesIO(file_data[1])
            with zipfile.ZipFile(zip_bytes_io, 'r') as z:
                file_list = z.namelist()
                for file_name in file_list:
                    extract_and_save_file(zip_bytes_io, file_name, output_path + file_name)
        
        # Aplica a função a cada arquivo no RDD
        rdd.foreach(process_zip_file)


# Deszipando de forma distribuida (porém um arquivo de cada vez, se não sobrecarregar o worker dado tamanho dos arquivos em estabelecimentos)
        


# Definindo a sessão do Spark
spark = SparkSession.builder.appName("ExtractZipFiles").getOrCreate()

# Função para processar cada arquivo zip
def process_zip_file(zip_file_path):
    import io
    import zipfile
    import gcsfs
    
    # Gera o caminho de saída com base no nome do arquivo ZIP
    output_path = 'gs://projeto-dados-receita-federal/Estabelecimentos/arquivo_deszipado/'
    
    # Cria uma instância do sistema de arquivos do GCS
    fs = gcsfs.GCSFileSystem()
    
    # Lê o arquivo zip como bytes usando gcsfs diretamente
    with fs.open(zip_file_path, 'rb') as f_zip:
        zip_bytes_io = io.BytesIO(f_zip.read())
        
        with zipfile.ZipFile(zip_bytes_io, 'r') as z:
            file_list = z.namelist()
            for file_name in file_list:
                with z.open(file_name) as specific_file, fs.open(output_path + file_name, 'wb') as f_out:
                    # Lê e escreve em partes para evitar o uso excessivo de memória
                    for data in iter(lambda: specific_file.read(4096), b''):
                        f_out.write(data)
                print(f"File {file_name} extracted and saved to {output_path + file_name}")

# Variáveis para o nome do bucket e o prefixo dos arquivos
bucket_name = 'projeto-dados-receita-federal'
prefix = 'Estabelecimentos'

# Lista de caminhos dos arquivos ZIP
zip_files_paths = [f'gs://projeto-dados-receita-federal/Estabelecimentos/{prefix}{i}.zip' for i in range(10)]

# Distribui o processamento dos arquivos ZIP
rdd = spark.sparkContext.parallelize(zip_files_paths)
rdd.foreach(process_zip_file)


# ------------------------------------------------------------------------------------------------------------------- #



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

## Estabelecimentos

# Caminho base dos arquivos CSV no GCS
base_path = "gs://projeto-dados-receita-federal/Estabelecimentos/arquivo_deszipado"

# Lendo os dados dos estabelecimentos e criando um DataFrame Spark
df_estabelecimentos = read_data(spark, "estabelecimentos", base_path, encodings['Estabelecimentos'])

df_estabelecimentos.write.format('bigquery') \
.option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
.option('table', 'receitafederal.'+str('estabelecimentos')) \
.option("temporaryGcsBucket","projeto-dados-receita-federal/temp_bq") \
.mode("overwrite") \
.save()

## Empresas

# Caminho base dos arquivos CSV no GCS
base_path = "gs://projeto-dados-receita-federal/Empresas/arquivo_deszipado"

# Lendo os dados dos estabelecimentos e criando um DataFrame Spark
df_empresas = read_data(spark, "Empresas", base_path, encodings['Empresas'])

df_empresas.write.format('bigquery') \
.option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
.option('table', 'receitafederal.'+str('Empresas')) \
.option("temporaryGcsBucket","projeto-dados-receita-federal/temp_bq") \
.mode("overwrite") \
.save()

## Naturezas

# Caminho base dos arquivos CSV no GCS
base_path = "gs://projeto-dados-receita-federal/Naturezas/arquivo_deszipado"

# Lendo os dados dos estabelecimentos e criando um DataFrame Spark
df_naturezas = read_data(spark, "Naturezas", base_path, encodings['Naturezas'])

df_naturezas.write.format('bigquery') \
.option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
.option('table', 'receitafederal.'+str('naturezas')) \
.option("temporaryGcsBucket","projeto-dados-receita-federal/temp_bq") \
.mode("overwrite") \
.save()


## Qualificacoes

# Caminho base dos arquivos CSV no GCS
base_path = "gs://projeto-dados-receita-federal/Qualificacoes/arquivo_deszipado"

# Lendo os dados dos estabelecimentos e criando um DataFrame Spark
df_qualificacoes = read_data(spark, "Qualificacoes", base_path, encodings['Qualificacoes'])

df_qualificacoes.write.format('bigquery') \
.option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
.option('table', 'receitafederal.'+str('qualificacoes')) \
.option("temporaryGcsBucket","projeto-dados-receita-federal/temp_bq") \
.mode("overwrite") \
.save()

## Paises

# Caminho base dos arquivos CSV no GCS
base_path = "gs://projeto-dados-receita-federal/Paises/arquivo_deszipado"

# Lendo os dados dos estabelecimentos e criando um DataFrame Spark
df_paises = read_data(spark, "Paises", base_path, encodings['Paises'])

df_paises.write.format('bigquery') \
.option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
.option('table', 'receitafederal.'+str('paises')) \
.option("temporaryGcsBucket","projeto-dados-receita-federal/temp_bq") \
.mode("overwrite") \
.save()

## Municipios

# Caminho base dos arquivos CSV no GCS
base_path = "gs://projeto-dados-receita-federal/Municipios/arquivo_deszipado"

# Lendo os dados dos estabelecimentos e criando um DataFrame Spark
df_municipios = read_data(spark, "Municipios", base_path, encodings['Municipios'])

df_municipios.write.format('bigquery') \
.option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
.option('table', 'receitafederal.'+str('municipios')) \
.option("temporaryGcsBucket","projeto-dados-receita-federal/temp_bq") \
.mode("overwrite") \
.save()

## Cnaes

# Caminho base dos arquivos CSV no GCS
base_path = "gs://projeto-dados-receita-federal/Cnaes/arquivo_deszipado"

# Lendo os dados dos estabelecimentos e criando um DataFrame Spark
df_cnaes = read_data(spark, "Cnaes", base_path, encodings['Cnaes'])

df_cnaes.write.format('bigquery') \
.option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
.option('table', 'receitafederal.'+str('cnaes')) \
.option("temporaryGcsBucket","projeto-dados-receita-federal/temp_bq") \
.mode("overwrite") \
.save()

## Motivos
# Caminho base dos arquivos CSV no GCS
base_path = "gs://projeto-dados-receita-federal/Motivos/arquivo_deszipado"

# Lendo os dados dos estabelecimentos e criando um DataFrame Spark
df_motivos = read_data(spark, "Motivos", base_path, encodings['Motivos'])

df_motivos.write.format('bigquery') \
.option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
.option('table', 'receitafederal.'+str('motivos')) \
.option("temporaryGcsBucket","projeto-dados-receita-federal/temp_bq") \
.mode("overwrite") \
.save()

## Simples

# Caminho base dos arquivos CSV no GCS
base_path = "gs://projeto-dados-receita-federal/Simples/arquivo_deszipado"

# Lendo os dados dos estabelecimentos e criando um DataFrame Spark
df_motivos = read_data(spark, "Simples", base_path, encodings['Simples'])


df_motivos.write.format('bigquery') \
.option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
.option('table', 'receitafederal.'+str('simples')) \
.option("temporaryGcsBucket","projeto-dados-receita-federal/temp_bq") \
.mode("overwrite") \
.save()

