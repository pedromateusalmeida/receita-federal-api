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

    def __init__(self, bucket_name, data_update=True, max_attempts=15, wait_time=150):
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


logging.info("Downloads iniciado")
# Exemplo de uso
if __name__ == "__main__":
    bucket_name = "projeto-dados-receita-federal"
    api = ReceitaCNPJApiGCP(bucket_name=bucket_name, data_update=True, max_attempts=10, wait_time=240)
    prefix = prefix = ['Municipios', 'Cnaes', 'Naturezas', 'Qualificacoes', 'Paises', 'Motivos','Simples','Socios','Empresas','Estabelecimentos']
    for i in prefix:
        logging.info(f"Download do dataset {prefix} iniciado")
        urls = api.lista_urls_receita(f'{i}')
        api.download_files_concurrently(urls, f'{i}')
        logging.info(f"Download do dataset {prefix} finalizado")

logging.info("Downloads Finalizados")