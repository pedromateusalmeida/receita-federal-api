
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import gcsfs
import zipfile
from io import BytesIO
from pyspark.sql import SparkSession
import logging

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
                    break  
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

logging.info("Unzip tabelas pequenas no storage concluído")