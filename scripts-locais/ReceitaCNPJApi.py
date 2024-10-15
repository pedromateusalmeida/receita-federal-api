import os
import logging
import requests
import zipfile
import shutil
import time
from datetime import datetime
from collections import Counter
from bs4 import BeautifulSoup

class ReceitaCNPJApi:
    """
    Classe ReceitaCNPJApi:

    Esta classe é responsável por interagir com a API de Dados Abertos da Receita Federal, facilitando 
    a obtenção de informações relacionadas a CNPJs, como empresas, sócios, municípios, entre outros. 
    A classe inclui funcionalidades para baixar, descompactar e salvar dados, além de utilitários 
    para manipular e consultar URLs.
    """

    # Prefixos de arquivos que podem ser baixados da API.
    FILE_PREFIXES = [
        'Estabelecimentos', 'Municipios', 'Simples', 'Empresas', 
        'Cnaes', 'Socios', 'Naturezas', 'Qualificacoes', 'Paises', 'Motivos'
    ]

    # Número máximo de tentativas ao fazer uma solicitação para a API.
    MAX_ATTEMPTS = 15

    # Tempo de espera entre tentativas de solicitações (em segundos).
    WAIT_TIME = 180

    # URL base da API de dados abertos da Receita Federal, sem incluir o ano/mês.
    BASE_URL = "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj"

    def __init__(self, base_output_path: str = "./output", save_base_path: str = "./temp"):
        """
        Inicializador da classe. Configura o logger para registrar atividades e erros e define os caminhos base 
        para salvar os arquivos descompactados e baixados.

        Args:
            base_output_path (str, optional): Caminho base para salvar os arquivos descompactados. Padrão para "./output".
            save_base_path (str, optional): Caminho base para salvar os arquivos baixados. Padrão para "./temp".
        """
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

        self.BASE_OUTPUT_PATH = base_output_path
        self.SAVE_BASE_PATH = save_base_path

    @staticmethod
    def get_current_date():
        """
        Retorna a data atual.

        Returns:
            date: Data atual.
        """
        return datetime.today().date()

    def get_output_path_for_prefix(self, prefix):
        """
        Gera o caminho completo de saída para armazenar os dados com base em um prefixo fornecido.

        Args:
            prefix (str): Prefixo do arquivo para o qual o caminho de saída será gerado.

        Returns:
            str: Caminho completo de saída para o prefixo especificado.
        """
        return os.path.join(self.BASE_OUTPUT_PATH, prefix, f"{prefix}.csv")

    def lista_urls_receita(self, *prefixes, year: int = None, month: int = None):
        """
        Gera uma lista de URLs para download com base nos prefixos fornecidos e no período especificado. 
        Se nenhum prefixo for fornecido, o método gerará URLs para todos os tipos de arquivos conhecidos.

        Args:
            *prefixes (str): Prefixos de arquivos para os quais as URLs serão geradas.
            year (int, optional): Ano de atualização dos dados (ex: 2024). Se não fornecido, usa o ano atual.
            month (int, optional): Mês de atualização dos dados (ex: 9 para setembro). Se não fornecido, usa o mês atual.

        Returns:
            list: Lista de URLs completas para os arquivos correspondentes aos prefixos.
        """
        # Determinar o ano e o mês a serem usados
        if year is None or month is None:
            current_date = self.get_current_date()
            year = year or current_date.year
            month = month or current_date.month

        # Validar os parâmetros de ano e mês
        if not (1 <= month <= 12):
            raise ValueError("O mês deve estar entre 1 e 12.")
        if year < 0:
            raise ValueError("O ano deve ser um valor positivo.")

        # Construir a URL completa com base no ano e mês
        period = f"{year:04d}-{month:02d}"
        full_base_url = f"{self.BASE_URL}/{period}"

        self.logger.info(f"Gerando URLs para o período: {period}")

        urls = []

        # Se nenhum prefixo for fornecido, usar todos os prefixos conhecidos
        if not prefixes:
            prefixes = self.FILE_PREFIXES

        for prefix in prefixes:
            if prefix in ['Municipios', 'Cnaes', 'Naturezas', 'Simples', 'Qualificacoes', 'Paises', 'Motivos']:
                urls.append(f"{full_base_url}/{prefix}.zip")
            elif prefix in self.FILE_PREFIXES:
                urls.extend([f"{full_base_url}/{prefix}{i}.zip" for i in range(10)])
            else:
                self.logger.warning(f"Prefixo '{prefix}' não reconhecido!")

        self.logger.info(f"{len(urls)} URLs geradas.")
        return urls

    def fetch_data(self, url, log_accumulator=None, max_attempts=15, wait_time=180):
        """
        Baixa um arquivo da URL especificada e o salva no caminho de saída especificado. Em caso de 
        falha na tentativa de download, tentará novamente até o número máximo de tentativas ser atingido.

        Args:
            url (str): URL de onde o arquivo será baixado.
            log_accumulator (list, optional): Um acumulador para armazenar mensagens de log. Padrão para None.
            max_attempts (int, optional): Número máximo de tentativas de download. Padrão para 15.
            wait_time (int, optional): Tempo de espera entre tentativas em segundos. Padrão para 180.

        Raises:
            Exception: Se o número máximo de tentativas for atingido sem sucesso.

        Returns:
            str: Caminho completo do arquivo baixado.
        """
        os.makedirs(self.SAVE_BASE_PATH, exist_ok=True)

        file_name = url.split('/')[-1]
        file_path = os.path.join(self.SAVE_BASE_PATH, file_name)

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36"
            )
        }

        for attempt in range(1, max_attempts + 1):
            try:
                self.logger.info(f"Baixando {url} (Tentativa {attempt}/{max_attempts})")
                response = requests.get(url, headers=headers)
                response.raise_for_status()

                with open(file_path, 'wb') as file:
                    file.write(response.content)
                self.logger.info(f"Download concluído: {file_path}")
                return file_path

            except requests.RequestException as e:
                msg = f"Tentativa {attempt} de {max_attempts} falhou. Erro: {e}"
                self.logger.error(msg)
                if log_accumulator is not None:
                    log_accumulator.append([msg, "ERRO NO REQUEST! RETRY SENDO FEITO"])

                if attempt == max_attempts:
                    final_msg = "Número máximo de tentativas atingido. Download falhou."
                    self.logger.error(final_msg)
                    if log_accumulator is not None:
                        log_accumulator.append([final_msg])
                    raise Exception(final_msg) from e

                self.logger.info(f"Aguardando {wait_time} segundos antes da próxima tentativa...")
                time.sleep(wait_time)

    def download_and_unzip(self, url, output_base_path=None, log_accumulator=None):
        """
        Baixa um arquivo zip da URL fornecida, descompacta e salva no caminho especificado.

        Args:
            url (str): URL de onde o arquivo zip será baixado.
            output_base_path (str, optional): Caminho base onde os dados descompactados serão armazenados. 
                                              Se não for fornecido, usa o caminho definido no inicializador.
            log_accumulator (list, optional): Um acumulador para armazenar mensagens de log. Padrão para None.

        Returns:
            tuple: Um tuple contendo a URL e uma mensagem indicando "Success" ou a razão da falha.
        """
        if output_base_path is None:
            output_base_path = self.BASE_OUTPUT_PATH

        try:
            zip_file_path = self.fetch_data(url, log_accumulator)
            self.unzip_files(zip_file_path, output_base_path, log_accumulator)
            self.logger.info(f"Arquivo {url} foi descompactado com sucesso para {output_base_path}")
            return (url, "Success")
        except Exception as e:
            error_msg = f"Falha ao baixar e descompactar {url}. Erro: {e}"
            self.logger.error(error_msg)
            return (url, str(e))

    def unzip_files(self, zip_file_path, output_base_path, log_accumulator=None):
        """
        Descompacta o arquivo fornecido e salva no caminho especificado. Substitui arquivos existentes.

        Args:
            zip_file_path (str): Caminho completo do arquivo zip que precisa ser descompactado.
            output_base_path (str): Caminho base onde o arquivo descompactado deve ser salvo.
            log_accumulator (list, optional): Uma lista que pode ser fornecida para acumular mensagens de log, 
                                             útil para rastrear erros ou informações. Se não for fornecido, 
                                             apenas os logs padrão serão usados.
        """
        zip_file_name = os.path.basename(zip_file_path)
        prefix = next((p for p in self.FILE_PREFIXES if zip_file_name.startswith(p)), None)

        if not prefix:
            msg = f"O arquivo {zip_file_path} não corresponde aos padrões esperados."
            self.logger.error(msg)
            if log_accumulator is not None:
                log_accumulator.append([msg])
            return

        output_path = os.path.join(output_base_path, prefix)
        os.makedirs(output_path, exist_ok=True)

        try:
            self.logger.info(f"Descompactando {zip_file_path} para {output_path}")
            with zipfile.ZipFile(zip_file_path, "r") as z:
                file_inside_zip = z.namelist()[0]
                number_in_zip = ''.join(filter(str.isdigit, zip_file_name))
                final_file_path = os.path.join(output_path, f"{prefix}{number_in_zip}.csv")

                if os.path.exists(final_file_path):
                    os.remove(final_file_path)
                    self.logger.info(f"Arquivo existente {final_file_path} removido.")

                with z.open(file_inside_zip) as zf, open(final_file_path, 'wb') as f_out:
                    shutil.copyfileobj(zf, f_out)
            self.logger.info(f"Arquivo descompactado com sucesso: {final_file_path}")
        except zipfile.BadZipFile as e:
            msg = f"Erro ao descompactar {zip_file_path}: {e}"
            self.logger.error(msg)
            if log_accumulator is not None:
                log_accumulator.append([msg])
            raise
        finally:
            if os.path.exists(zip_file_path):
                os.remove(zip_file_path)
                self.logger.info(f"Arquivo zip {zip_file_path} removido após descompactação.")
