import logging
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
from collections import Counter
import zipfile
import shutil
from concurrent.futures import ThreadPoolExecutor
import time


class ReceitaCNPJApi:
    """
    Classe ReceitaCNPJApi:

    Esta classe é responsável por interagir com a API de Dados Abertos da Receita Federal, facilitando 
    a obtenção de informações relacionadas a CNPJs, como empresas, sócios, municípios, entre outros. 
    A classe inclui funcionalidades para baixar, descompactar e salvar dados, além de utilitários 
    para manipular e consultar URLs.
    """

    # URL base da API de dados abertos da Receita Federal.
    BASE_URL = "https://dadosabertos.rfb.gov.br/CNPJ"

    # Prefixos de arquivos que podem ser baixados da API.
    FILE_PREFIXES = ['Estabelecimentos', 'Municipios', 'Simples', 'Empresas', 'Cnaes', 'Socios', 'Naturezas','Qualificacoes','Paises','Motivos']

    # Número máximo de tentativas ao fazer uma solicitação para a API.
    MAX_ATTEMPTS = 15

    # Tempo de espera entre tentativas de solicitações (em segundos).
    WAIT_TIME = 180

    def __init__(self):
        """
        Inicializador da classe. Configura o logger para registrar atividades e erros.
        """
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    @staticmethod
    def get_last_month():
        """
        Retorna a data atual.
        """
        return (datetime.today()).date()

    def get_output_path_for_prefix(self, prefix):
        """
        Gera o caminho completo de saída para armazenar os dados com base em um prefixo fornecido.

        Args:
        prefix (str): Prefixo do arquivo para o qual o caminho de saída será gerado.

        Returns:
        str: Caminho completo de saída para o prefixo especificado.
        """
        return os.path.join(self.BASE_OUTPUT_PATH, prefix, f"{prefix}.csv")

    def get_most_common_date(self):
        """
        Consulta a API para obter a data mais comum em que os arquivos foram atualizados.
        """

        # Etapa 1: Tentar consultar a API e obter uma resposta.
        try:
            response = requests.get(self.BASE_URL)
            response.raise_for_status()

            # Etapa 2: Usar a biblioteca BeautifulSoup para analisar a resposta HTML.
            soup = BeautifulSoup(response.text, 'html.parser')

            # Etapa 3: Selecionar todos os elementos de data da resposta HTML.
            # Assumindo que a data é o terceiro elemento filho da tag 'tr'.
            date_elements = soup.select('tr > td:nth-child(3)')

            # Etapa 4: Transformar cada elemento de data em um objeto datetime.
            dates = [datetime.strptime(elem.get_text().strip(), '%Y-%m-%d %H:%M') for elem in date_elements if elem.get_text().strip() != '']

            # Etapa 5: Contar as ocorrências de cada data e determinar a data mais comum.
            most_common_date, _ = Counter([date.date() for date in dates]).most_common(1)[0]

            # Etapa 6: Retornar a data mais comum.
            return datetime.combine(most_common_date, datetime.min.time()).date()
        except requests.RequestException as e:
            # Etapa 7: Em caso de qualquer erro na consulta da API, registrar o erro e retornar None.
            self.logger.error(f"Failed to get most common date. Error: {e}")
            return None

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
        # Etapa 1: Inicializar a lista vazia para armazenar as URLs.
        urls = []

        # Etapa 2: Se nenhum prefixo for fornecido como argumento, use todos os prefixos de arquivo conhecidos.
        if not prefixes:
            prefixes = self.FILE_PREFIXES

        # Etapa 3: Iterar sobre cada prefixo fornecido.
        for prefix in prefixes:
            # Etapa 3.1: Se o prefixo estiver na lista de prefixos especificados, 
            # adicione a URL correspondente à lista de URLs.
            if prefix in ['Municipios', 'Cnaes', 'Naturezas', 'Simples','Qualificacoes','Paises','Motivos']:
                urls.append(f"{self.BASE_URL}/{prefix}.zip")

            # Etapa 3.2: Se o prefixo estiver na lista de prefixos de arquivo conhecidos, 
            # gere URLs para cada arquivo (de 0 a 9) e adicione-as à lista de URLs.
            elif prefix in self.FILE_PREFIXES:
                urls.extend([f"{self.BASE_URL}/{prefix}{i}.zip" for i in range(10)])

            # Etapa 3.3: Se o prefixo fornecido não for reconhecido, 
            # imprima uma mensagem informando que o prefixo não é reconhecido.
            else:
                print(f"Prefixo '{prefix}' não reconhecido!")

        # Etapa 4: Retorne a lista completa de URLs geradas.
        return urls

    def fetch_data(self, url, save_path, log_accumulator=None, max_attempts=15, wait_time=180):
        """
        Baixa um arquivo da URL especificada e o salva no caminho de saída especificado. Em caso de 
        falha na tentativa de download, tentará novamente até o número máximo de tentativas ser atingido.

        Args:
        url (str): URL de onde o arquivo será baixado.
        save_path (str): Caminho onde o arquivo baixado será salvo.
        log_accumulator (list, optional): Um acumulador para armazenar mensagens de log. Padrão para None.
        max_attempts (int, optional): Número máximo de tentativas de download. Padrão para 15.
        wait_time (int, optional): Tempo de espera entre tentativas em segundos. Padrão para 150.

        Raises:
        Exception: Se o número máximo de tentativas for atingido sem sucesso.

        Returns:
        str: Caminho completo do arquivo baixado.
        """

        # Etapa 1: Criar o diretório no caminho de salvamento, caso ele não exista.
        os.makedirs(save_path, exist_ok=True)

        # Etapa 2: Derivar o nome do arquivo da URL fornecida.
        file_name = url.split('/')[-1]
        file_path = os.path.join(save_path, file_name)

        # Etapa 3: Definir os cabeçalhos da solicitação.
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

        # Etapa 4: Iniciar tentativas de download do arquivo.
        for attempt in range(max_attempts):
            try:
                # Etapa 4.1: Fazer uma solicitação GET para a URL.
                response = requests.get(url, headers=headers)
                response.raise_for_status()

                # Etapa 4.2: Escrever o conteúdo da resposta no arquivo.
                with open(file_path, 'wb') as file:
                    file.write(response.content)
                return file_path

            except requests.RequestException as e:
                # Etapa 4.3: Registrar falha na tentativa de download.
                msg = f"Tentativa {attempt + 1} de {max_attempts} falhou. Erro: {e}"
                if log_accumulator:
                    log_accumulator.append([msg, "ERRO NO REQUEST! RETRY SENDO FEITO"])

                # Etapa 4.4: Se for a última tentativa, registrar o erro persistente e lançar uma exceção.
                if attempt == max_attempts - 1:
                    if log_accumulator:
                        log_accumulator.append(["ERRO PERSISTENTE AO TENTAR BAIXAR O ARQUIVO"])
                    raise

                # Etapa 4.5: Aguardar o tempo especificado antes de tentar novamente.
                time.sleep(wait_time)

                
    def download_and_unzip(self, url, save_base_path="./temp", output_base_path="./output", headers=None, log_accumulator=None, data_update=True):
        """
        Baixa um arquivo zip da URL fornecida, descompacta e salva no caminho especificado. Se os 
        dados da Receita Federal não foram atualizados nos últimos 30 dias e o parâmetro data_update 
        estiver ativado, uma mensagem de log será gerada.

        Args:
        url (str): URL de onde o arquivo zip será baixado.
        save_base_path (str, optional): Caminho base onde o arquivo baixado será salvo. Padrão para "./temp".
        output_base_path (str, optional): Caminho base onde os dados descompactados serão armazenados. Padrão para "./output".
        headers (dict, optional): Cabeçalhos HTTP para serem usados no pedido. Padrão para None.
        log_accumulator (list, optional): Uma acumulador para armazenar mensagens de log. Padrão para None.
        data_update (bool, optional): Determina se a função deve verificar se os dados foram atualizados nos últimos 30 dias. Padrão para True.

        Returns:
        tuple: Um tuple contendo a URL e uma mensagem indicando "Success" ou a razão da falha.
        """

        # Etapa 1: Identificar o nome do arquivo e seu prefixo a partir da URL fornecida.
        file_name = url.split('/')[-1]
        prefix = next((p for p in self.FILE_PREFIXES if file_name.startswith(p)), None)
        if not prefix:
            return (url, "Failed to determine prefix")

        # Etapa 2: Configurar o caminho de salvamento e garantir que o diretório de salvamento exista.
        save_path = os.path.join(save_base_path)
        os.makedirs(save_path, exist_ok=True)

        # Etapa 3: Obter a data mais comum de atualização dos arquivos da API e verificar se é válida.
        data_atualizacao = self.get_most_common_date()
        if data_atualizacao is None:
            return (url, "Failed to determine most common date")

        # Etapa 4: Obter a data atual.
        data_atual = self.get_last_month()

        # Etapa 5: Verificar se os dados da Receita Federal foram atualizados nos últimos 30 dias.
        if not data_update or (data_update and (data_atual - data_atualizacao).days <= 30):
            try:
                # Etapa 5.1: Tentar baixar o arquivo zip da URL fornecida.
                zip_file_path = self.fetch_data(url, save_path, headers)

                # Etapa 5.2: Configurar o caminho de saída e garantir que o diretório de saída exista.
                output_path = os.path.join(output_base_path)
                os.makedirs(output_path, exist_ok=True)

                # Etapa 5.3: Tentar descompactar o arquivo baixado.
                self.unzip_files(zip_file_path, output_path)

                # Etapa 5.4: Registrar a conclusão bem-sucedida da descompactação.
                self.logger.info(f"File {url} has been unzipped successfully to {output_path}")
                return (url, "Success")
            except Exception as e:
                # Etapa 5.5: Em caso de qualquer erro, registrar o erro e retornar a mensagem.
                self.logger.error(f"Failed to download and unzip {url}. Error: {e}")
                return (url, str(e))
        else:
            # Etapa 6: Se os dados não foram atualizados nos últimos 30 dias e data_update está ativado, registrar uma mensagem.
            log_msg = "Os dados da receita federal não foram atualizados nos últimos 30 dias"
            if log_accumulator:
                log_accumulator.add([log_msg])
            self.logger.info(log_msg)
            return (url, log_msg)

    def unzip_files(self, zip_file_path, output_base_path, log_accumulator=None):
        """
        Descompacta o arquivo fornecido e salva no caminho especificado. Substitui arquivos existentes.

        Args:
        - zip_file_path (str): Caminho completo do arquivo zip que precisa ser descompactado.
        - output_base_path (str): Caminho base onde o arquivo descompactado deve ser salvo.
        - log_accumulator (list, optional): Uma lista que pode ser fornecida para acumular mensagens de log, útil para rastrear erros ou informações. Se não for fornecido, apenas os logs padrão serão usados.

        """

        # Etapa 1: Extraia o nome do arquivo zip da rota fornecida.
        zip_file_name = os.path.basename(zip_file_path)

        # Etapa 2: Determine o prefixo do arquivo com base nos prefixos conhecidos.
        prefix = next((p for p in self.FILE_PREFIXES if zip_file_name.startswith(p)), None)
        if not prefix:
            msg = f"File {zip_file_path} does not match expected patterns."

            # Etapa 2.1: Se o prefixo não for encontrado, adicione uma mensagem de erro ao acumulador de log (se fornecido) e retorne.
            if log_accumulator:
                log_accumulator.append(msg)
            self.logger.error(msg)
            return

        # Etapa 3: Configurar o caminho de saída e garantir que o diretório de saída exista.
        output_path = os.path.join(output_base_path, prefix)
        os.makedirs(output_path, exist_ok=True)

        # Etapa 4: Abrir o arquivo zip.
        self.logger.info(f"Trying to unzip {zip_file_path}")
        with zipfile.ZipFile(zip_file_path, "r") as z:

            # Etapa 4.1: Pegue o nome do primeiro arquivo dentro do arquivo zip.
            file_inside_zip = z.namelist()[0]

            # Etapa 4.2: Extraia a parte numérica do nome do arquivo zip para nomear corretamente o arquivo csv resultante.
            number_in_zip = ''.join(filter(str.isdigit, zip_file_name))
            final_file_path = os.path.join(output_path, f"{prefix}{number_in_zip}.csv")

            # Etapa 4.3: Se o arquivo csv já existir no caminho de destino, exclua-o para garantir que o novo arquivo não seja sobreposto.
            if os.path.exists(final_file_path):
                os.remove(final_file_path)

            # Etapa 4.4: Descompacte o conteúdo do arquivo zip diretamente para o caminho de saída desejado.
            with z.open(file_inside_zip) as zf, open(final_file_path, 'wb') as f_out:
                shutil.copyfileobj(zf, f_out)

        # Etapa 5: Exclua o arquivo zip original após a extração.
        os.remove(zip_file_path)