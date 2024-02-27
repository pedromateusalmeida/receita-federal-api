from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import gcsfs
import zipfile
from io import BytesIO
from pyspark.sql import SparkSession
import logging
import io

# Definindo a sessão do Spark
spark = SparkSession.builder.appName("ExtractZipFiles").getOrCreate()

# Função para processar cada arquivo zip
def process_zip_file(zip_file_path):

    
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

logging.info("Unzip dos dados de Estabelecimentos no storage concluído")

spark.stop()