from functools import reduce
import os
import chardet
import logging
import glob
import secrets
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import regexp_replace, when,length,to_date,upper,lower,col,split,explode,coalesce,concat_ws,concat,lit,broadcast,regexp_extract,month,year,to_date
from pyspark.sql.functions import broadcast,expr,udf
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

   
def geocode_address(address):
    """
    Geocodifica um endereço, convertendo-o em coordenadas de latitude e longitude.

    Parâmetros:
        address (str): Endereço a ser geocodificado.

    Retorna:
        tuple: Um par contendo a latitude e a longitude do endereço fornecido. 
                Se o endereço não puder ser geocodificado, retorna (None, None).

    Exemplo:
        lat, lon = geocode_address("1600 Amphitheatre Parkway, Mountain View, CA")

    Notas:
        - Usa o serviço Nominatim para a geocodificação.
        - Incorpora um limitador de taxa para garantir que não excedamos os limites de requisições por segundo 
            do serviço.
    """
    geolocator = Nominatim(user_agent="CNPJ_GEOLOCATION")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    location = geocode(address)
    if location:
        return (location.latitude, location.longitude)
    else:
        return (None, None)


class ReceitaLT:
    """
    A classe `ReceitaLT` facilita a manipulação e análise de dados da Receita Federal do Brasil.

    Atributos:
        spark (SparkSession): Sessão Spark para manipulação de dataframes.
        logger (Logger): Logger para capturar e exibir logs.
        
    Atributos estáticos:
        - estabelecimentos: Schema para dados de estabelecimentos.
        - empresas: Schema para dados das empresas.
        - municipios: Schema para municípios.
        - cnaes: Schema para CNAEs.
        - paises: Schema para países.
        - qualificacoes: Schema para qualificações.
        - socios: Schema para sócios.
        - simples: Schema para opções do Simples Nacional.
        - naturezas: Schema para naturezas jurídicas.
        - motivos: Schema para motivos de situações cadastrais.
        - dic_provedor: Dicionário para correção de nomes de provedores de email.
        
    Métodos:
        detect_encoding(file_pattern_or_path, num_bytes=10000): Detecta a codificação do arquivo ou arquivos fornecidos.

    Uso:
        1. Instancie a classe com uma sessão Spark.
        2. Utilize os schemas estáticos para leitura de arquivos.
        3. Use o método `detect_encoding` para determinar a codificação de arquivos antes de lê-los.
        
    Exemplo:
        from pyspark.sql import SparkSession
        
        spark_session = SparkSession.builder.appName("MyApp").getOrCreate()
        receita_helper = ReceitaLT(spark_session)
        encodings = receita_helper.detect_encoding("path/to/datafile.csv")
        df = spark_session.read.csv("path/to/datafile.csv", schema=ReceitaLT.empresas, encoding=encodings["path/to/datafile.csv"])
    """
    def __init__(self, spark: SparkSession):
        """
        Inicializa a classe ReceitaLT.
        
        Parâmetros:
        spark (SparkSession): Uma sessão Spark ativa.
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO) 
        
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
    
    dic_provedor = {'0UTLOOK': 'OUTLOOK', '123GMAIL': 'GMAIL', '12GMAIL': 'GMAIL', '19GMAIL': 'GMAIL', '1HOTMAIL': 'HOTMAIL', 
                    '2010HOTMAIL': 'HOTMAIL', '20GMAIL': 'GMAIL', '23GMAIL': 'GMAIL', '2GMAIL': 'GMAIL', '2HOTMAIL': 'HOTMAIL', 
                    '30GMAIL': 'GMAIL', '7GMAIL': 'GMAIL', 'ADV': 'ADV', 'AGMAIL': 'GMAIL', 'AHOO': 'YAHOO', 'AIL': 'AOL', 'ALUNO': 'ALUNO', 
                    'AOL': 'AOL', 'AUTLOOK': 'OUTLOOK', 'BB': 'BB', 'BOL': 'BOL', 'BOLL': 'BOL', 'BOOL': 'BOL', 'BRTURBO': 'OI', 
                    'CAIXA': 'CAIXA', 'CLICK21': 'CLICK21', 'CLOUD': 'ICLOUD', 'CRECI': 'CRECI', 'EDU': 'EDU', 'EMAIL': 'EMAIL', 
                    'FACEBOOK': 'FACEBOOK', 'FMAIL': 'GMAIL', 'G': 'GMAIL', 'G-MAIL': 'GMAIL', 'GAIL': 'GMAIL', 'GAMAIL': 'GMAIL', 
                    'GAMEIL': 'GMAIL', 'GAMIAL': 'GMAIL', 'GAMIL': 'GMAIL', 'GEMAIL': 'GMAIL', 'GGMAIL': 'GMAIL', 'GHMAIL': 'GMAIL', 
                    'GHOTMAIL': 'HOTMAIL', 'GIMAIL': 'GMAIL', 'GLOBO': 'GLOBO', 'GLOBOMAIL': 'LWMAIL', 'GMA': 'GMAIL', 'GMAAIL': 'GMAIL', 
                    'GMAI': 'GMAIL', 'GMAIAL': 'GMAIL', 'GMAII': 'GMAIL', 'GMAIIL': 'GMAIL', 'GMAIK': 'GMAIL', 'GMAIL': 'GMAIL', 
                    'GMAILC': 'GMAIL', 'GMAILGMAIL': 'GMAIL', 'GMAILL': 'GMAIL', 'GMAILMAIL': 'GMAIL', 'GMAILO': 'GMAIL', 'GMAIM': 'GMAIL', 
                    'GMAIO': 'GMAIL', 'GMAIOL': 'GMAIL', 'GMAIS': 'GMAIL', 'GMAISL': 'GMAIL', 'GMAIUL': 'GMAIL', 'GMAL': 'GMAIL', 
                    'GMALI': 'GMAIL', 'GMAOL': 'GMAIL', 'GMAQIL': 'GMAIL', 'GMASIL': 'GMAIL', 'GMAUIL': 'GMAIL', 'GMAUL': 'GMAIL',
                    'GMEIL': 'GMAIL', 'GMIAL': 'GMAIL', 'GMIL': 'GMAIL', 'GML': 'GMAIL', 'GMMAIL': 'GMAIL', 'GMNAIL': 'GMAIL', 
                    'GMQIL': 'GMAIL', 'GMSIL': 'GMAIL', 'GNAIL': 'GMAIL', 'GNMAIL': 'GMAIL', 'GOMAIL': 'GMAIL', 'GOOGLEMAIL': 'GMAIL',
                    'GOTMAIL': 'HOTMAIL', 'GTMAIL': 'GMAIL', 'H0TMAIL': 'HOTMAIL', 'HAHOO': 'YAHOO', 'HATMAIL': 'HOTMAIL', 'HAYOO': 'YAHOO', 
                    'HGMAIL': 'GMAIL', 'HHOTMAIL': 'HOTMAIL', 'HIOTMAIL': 'HOTMAIL', 'HITMAIL': 'HOTMAIL', 'HJOTMAIL': 'HOTMAIL', 
                    'HMAIL': 'HOTMAIL', 'HOITMAIL': 'HOTMAIL', 'HOLMAIL': 'HOTMAIL', 'HOLTMAIL': 'HOTMAIL', 'HOMAIL': 'HOTMAIL', 
                    'HOMTAIL': 'HOTMAIL', 'HOMTIAL': 'HOTMAIL', 'HOMTMAIL': 'HOTMAIL', 'HOOTMAIL': 'HOTMAIL', 'HOPTMAIL': 'HOTMAIL', 
                    'HORMAIL': 'HOTMAIL', 'HORTMAIL': 'HOTMAIL', 'HOT': 'HOTMAIL', 'HOTAIL': 'HOTMAIL', 'HOTAMAIL': 'HOTMAIL', 
                    'HOTAMIL': 'HOTMAIL', 'HOTEMAIL': 'HOTMAIL', 'HOTGMAIL': 'HOTMAIL', 'HOTIMAIL': 'HOTMAIL', 'HOTIMAL': 'HOTMAIL', 
                    'HOTLMAIL': 'HOTMAIL', 'HOTLOOK': 'OUTLOOK', 'HOTMA': 'HOTMAIL', 'HOTMAAIL': 'HOTMAIL', 'HOTMAI': 'HOTMAIL', 
                    'HOTMAIAL': 'HOTMAIL', 'HOTMAII': 'HOTMAIL', 'HOTMAIIL': 'HOTMAIL', 'HOTMAIL': 'HOTMAIL', 'HOTMAILC': 'HOTMAIL', 
                    'HOTMAILL': 'HOTMAIL', 'HOTMAILO': 'HOTMAIL', 'HOTMAIM': 'HOTMAIL', 'HOTMAIO': 'HOTMAIL', 'HOTMAIOL': 'HOTMAIL', 
                    'HOTMAIUL': 'HOTMAIL', 'HOTMAL': 'HOTMAIL', 'HOTMALI': 'HOTMAIL', 'HOTMAMIL': 'HOTMAIL', 'HOTMAOL': 'HOTMAIL', 
                    'HOTMAQIL': 'HOTMAIL', 'HOTMASIL': 'HOTMAIL', 'HOTMAUIL': 'HOTMAIL', 'HOTMAUL': 'HOTMAIL', 'HOTMEIL': 'HOTMAIL', 
                    'HOTMIAIL': 'HOTMAIL', 'HOTMIAL': 'HOTMAIL', 'HOTMIL': 'HOTMAIL', 'HOTMMAIL': 'HOTMAIL', 'HOTMNAIL': 'HOTMAIL',
                    'HOTMQIL': 'HOTMAIL', 'HOTMSIL': 'HOTMAIL', 'HOTNAIL': 'HOTMAIL', 'HOTOMAIL': 'HOTMAIL', 'HOTRMAIL': 'HOTMAIL', 
                    'HOTTMAIL': 'HOTMAIL', 'HOTYMAIL': 'HOTMAIL', 'HOUTLOOK': 'OUTLOOK', 'HOYMAIL': 'HOTMAIL', 'HPTMAIL': 'HOTMAIL', 
                    'HTMAIL': 'HOTMAIL', 'HTOMAIL': 'HOTMAIL', 'HYAHOO': 'YAHOO', 'IAHOO': 'YAHOO', 'IBEST': 'IBEST', 'ICLAUD': 'ICLOUD', 
                    'ICLOD': 'ICLOUD', 'ICLOID': 'ICLOUD', 'ICLOOD': 'ICLOUD', 'ICLOU': 'ICLOUD', 'ICLOUD': 'ICLOUD', 'ICLOUDE': 'ICLOUD', 
                    'ICLOULD': 'ICLOUD', 'ICLOUND': 'ICLOUD', 'ICLUD': 'ICLOUD', 'ICLUOD': 'ICLOUD', 'ICOUD': 'ICLOUD', 'ICOULD': 'ICLOUD', 
                    'ID': 'IG', 'IG': 'IG', 'IGMAIL': 'GMAIL', 'IGUI': 'IG', 'IMAIL': 'GMAIL', 'INCLOUD': 'ICLOUD', 'ITELEFONICA': 'ITELEFONICA',
                    'JMAIL': 'GMAIL', 'JOTMAIL': 'HOTMAIL', 'LIVE': 'LIVE', 'LWMAIL': 'LWMAIL', 'MAIL': 'MAIL', 'ME': 'ME', 'MSM': 'MSN', 
                    'MSN': 'MSN', 'NETSITE': 'NETSITE', 'OI': 'OI', 'OIMAIL': 'HOTMAIL', 'OITLOOK': 'OUTLOOK', 'OLTLOOK': 'OUTLOOK', 
                    'OOUTLOOK': 'OUTLOOK', 'OTLOOK': 'OUTLOOK', 'OTMAIL': 'HOTMAIL', 'OUL': 'UOL', 'OULOOK': 'OUTLOOK', 'OULTLOOK': 'OUTLOOK',
                    'OULTOOK': 'OUTLOOK', 'OUTILOOK': 'OUTLOOK', 'OUTIOOK': 'OUTLOOK', 'OUTLLOK': 'OUTLOOK', 'OUTLLOOK': 'OUTLOOK', 
                    'OUTLOCK': 'OUTLOOK', 'OUTLOK': 'OUTLOOK', 'OUTLOKK': 'OUTLOOK', 'OUTLOOCK': 'OUTLOOK', 'OUTLOOK': 'OUTLOOK', 
                    'OUTLOOKL': 'OUTLOOK', 'OUTLOOL': 'OUTLOOK', 'OUTLOOOK': 'OUTLOOK', 'OUTLUK': 'OUTLOOK', 'OUTOLOOK': 'OUTLOOK',
                    'OUTOOK': 'OUTLOOK', 'OUTOOLK': 'OUTLOOK', 'OUTTLOOK': 'OUTLOOK', 'OUTULOOK': 'OUTLOOK', 'POP': 'POP',
                    'PROTON': 'PROTONMAIL', 'PROTONMAIL': 'PROTONMAIL', 'PUTLOOK': 'OUTLOOK', 'R7': 'R7', 'ROCKETMAIL': 'ROCKETMAIL', 
                    'ROCKTMAIL': 'ROCKETMAIL', 'ROTMAIL': 'HOTMAIL', 'SERCOMTEL': 'SERCOMTEL', 'SETELAGOASGML': 'GMAIL', 
                    'SUPERIG': 'SUPERIG', 'TAHOO': 'YAHOO', 'TERRA': 'TERRA', 'TERRRA': 'TERRA', 'TMAIL': 'GMAIL', 
                    'TVGLOBO': 'GLOBO', 'UAHOO': 'YAHOO', 'UAI': 'UAI', 'UFV': 'UFV', 'UNESP': 'UNESP', 'UNOCHAPECO': 'UNOCHAPECO', 
                    'UO': 'UOL', 'UOL': 'UOL', 'UOTLOOK': 'OUTLOOK', 'UPF': 'UPF', 'USP': 'USP', 'UTLOOK': 'OUTLOOK', 'VELOXMAIL': 'VELOXMAIL',
                    'WINDOWSLIVE': 'WINDOWSLIVE', 'YAAHOO': 'YAHOO', 'YAGOO': 'YAHOO', 'YAHAOO': 'YAHOO', 'YAHHO': 'YAHOO', 'YAHHOO': 'YAHOO', 
                    'YAHO': 'YAHOO', 'YAHOO': 'YAHOO', 'YAHOOCOM': 'YAHOO', 'YAHOOL': 'YAHOO', 'YAHOOO': 'YAHOO', 'YAHOOU': 'YAHOO', 
                    'YANHOO': 'YAHOO', 'YAOO': 'YAHOO', 'YAOOL': 'YAHOO', 'YAROO': 'YAHOO', 'YHAOO': 'YAHOO', 'YHOO': 'YAHOO', 'YMAIL': 'YMAIL', 
                    'YOHOO': 'YAHOO', 'YOPMAIL': 'HOTMAIL', 'ZIPMAIL': 'ZIPMAIL', '_HOTMAIL': 'HOTMAIL',     'GMAUL': 'GMAIL','GMALE': 'GMAIL', 
                    'GMAILE': 'GMAIL', 'GMILE': 'GMAIL', 'HOTMEL': 'HOTMAIL', 'HOTMELL': 'HOTMAIL','HOTMEAL': 'HOTMAIL', 'OUTLOKES': 'OUTLOOK', 
                    'OTLOOKS': 'OUTLOOK', 'YAHU': 'YAHOO', 'YOHU': 'YAHOO', 'YAHUO': 'YAHOO', 'YAHEE': 'YAHOO', 'UOLL': 'UOL',
                    'UOOL': 'UOL', 'UULL': 'UOL', 'ICLODUE': 'ICLOUD', 'ICLAWD': 'ICLOUD', 'ROCKEDMAIL': 'ROCKETMAIL', 'ROKETMAIL': 'ROCKETMAIL',
                    'OUTLOKE': 'OUTLOOK', 'OUTLOOCKE': 'OUTLOOK', 'YAAHO': 'YAHOO', 'YAHOOE': 'YAHOO', 'YAHUE': 'YAHOO', 'HOTMILE': 'HOTMAIL', 'HOTMELE': 'HOTMAIL',
                    'FACEBOKE': 'FACEBOOK', 'FACBOOK': 'FACEBOOK', 'FCEBOOK': 'FACEBOOK', 'BOLL': 'BOL', 'BOLLE': 'BOL', 'BULE': 'BOL', 'GLOBOE': 'GLOBO',
                    'GLOBU': 'GLOBO', 'GMILE': 'GMAIL', 'MSNE': 'MSN', 'MSNN': 'MSN', 'ICLOOUD': 'ICLOUD', 'OUTLUKE': 'OUTLOOK', 'OUTLLOKE': 'OUTLOOK',
                    'GMAUL': 'GMAIL','GMALE': 'GMAIL', 'GMAILE': 'GMAIL', 'GMILE': 'GMAIL', 'HOTMEL': 'HOTMAIL', 'HOTMELL': 'HOTMAIL',
                    'HOTMEAL': 'HOTMAIL', 'OUTLOKES': 'OUTLOOK', 'OTLOOKS': 'OUTLOOK', 'YAHU': 'YAHOO', 'YOHU': 'YAHOO', 'YAHUO': 'YAHOO', 
                    'YAHEE': 'YAHOO', 'UOLL': 'UOL', 'UOOL': 'UOL', 'UULL': 'UOL', 'ICLODUE': 'ICLOUD', 'ICLAWD': 'ICLOUD',  'ROCKEDMAIL': 'ROCKETMAIL',
                    'ROKETMAIL': 'ROCKETMAIL', 'OUTLOKE': 'OUTLOOK', 'OUTLOOCKE': 'OUTLOOK', 'YAAHO': 'YAHOO', 'YAHOOE': 'YAHOO', 'YAHUE': 'YAHOO', 
                    'HOTMILE': 'HOTMAIL', 'HOTMELE': 'HOTMAIL', 'FACEBOKE': 'FACEBOOK', 'FACBOOK': 'FACEBOOK', 'FCEBOOK': 'FACEBOOK', 'BOLL': 'BOL',
                    'BOLLE': 'BOL', 'BULE': 'BOL', 'GLOBOE': 'GLOBO',  'GLOBU': 'GLOBO', 'GMILE': 'GMAIL', 'MSNE': 'MSN', 'MSNN': 'MSN', 'ICLOOUD': 'ICLOUD',
                    'OUTLUKE': 'OUTLOOK', 'OUTLLOKE': 'OUTLOOK', 'PROTONMIAL': 'PROTONMAIL',  'PROTONMALE': 'PROTONMAIL', 'PROTOMAIL': 'PROTONMAIL', 
                    'OULOOKCOM': 'OUTLOOK', 'YAHCOM': 'YAHOO',  'YAHOCOM': 'YAHOO','GAMILCOM': 'GMAIL', 'GMALCOM': 'GMAIL',  'HOTMALCOM': 'HOTMAIL',  
                    'HOTMILCOM': 'HOTMAIL', 'HOTMELCOM': 'HOTMAIL', 'ROCKMAIL': 'ROCKETMAIL', 'ROKMAIL': 'ROCKETMAIL', 'TERA': 'TERRA', 'TEERA': 'TERRA', 
                    'FACBOOKCOM': 'FACEBOOK', 'FACEBOOKCOM': 'FACEBOOK', 'ICLOWD': 'ICLOUD', 'ICLOUND': 'ICLOUD', 'UOOLCOM': 'UOL', 'UOLLCOM': 'UOL', 
                    'UOLCOMBR': 'UOL','LIVECOM': 'LIVE', 'LIVECOMBR': 'LIVE', 'GMAICOM': 'GMAIL',  'GMAILCOMBR': 'GMAIL',  'YAHOOBR': 'YAHOO', 
                    'YAHOOOCOMBR': 'YAHOO', 'YAHOOOCOM': 'YAHOO', 'ZIPMAILE': 'ZIPMAIL', 'ZIPMAILL': 'ZIPMAIL',  'IBESTT': 'IBEST', 'IBESTE': 'IBEST'}

    
    @staticmethod
    def detect_encoding(file_pattern_or_path, num_bytes=10000):
        """
        Detecta a codificação do arquivo ou arquivos fornecidos.
        
        Parâmetros:
            file_pattern_or_path (str): Caminho ou padrão do arquivo para detecção.
            num_bytes (int, opcional): Número de bytes para ler para a detecção. Padrão é 10000.
        
        Retorna:
            dict: Dicionário com caminho do arquivo como chave e codificação detectada como valor.
        """
        files = glob.glob(file_pattern_or_path)
        encodings = {}
        for file_path in files:
            with open(file_path, 'rb') as f:
                rawdata = f.read(num_bytes)
                encodings[file_path] = chardet.detect(rawdata)["encoding"]
        return encodings


    def read_data(self, schema_name, base_path=None):
        """
        Lê dados de vários arquivos CSV de acordo com o esquema e caminho base fornecidos, consolidando-os 
        em um único DataFrame do Spark.

        Parâmetros:
            schema_name (str): Nome do esquema a ser usado para a leitura dos arquivos.
                               Deve ser uma das chaves do dicionário `schemas`.

            base_path (str, opcional): Caminho base dos arquivos CSV.
                                       Se não for fornecido, ele tentará buscar da variável de ambiente 'BASE_PATH'.
                                       Caso não encontre, o padrão "./output" será utilizado.

        Retorna:
            DataFrame: DataFrame do Spark contendo os dados consolidados dos arquivos CSV.

        Exceções:
            Pode lançar uma exceção se o arquivo não estiver presente no caminho especificado ou
            se houver problemas de codificação ao ler o arquivo.

        Exemplo:
            receita_helper = ReceitaLT(spark_session)
            df = receita_helper.read_data("estabelecimentos", "/path/to/csv/files")

        Notas:
            - A função primeiro detecta a codificação dos arquivos antes de lê-los para garantir que 
              eles sejam lidos corretamente.
            - A função lida com múltiplos arquivos CSV e os une em um único DataFrame.
            - O formato de arquivo assumido é CSV com delimitador ";", sem cabeçalho e com aspas para delimitar campos.
        """
        schemas = {
            "estabelecimentos": self.estabelecimentos,
            "empresas": self.empresas,
            "municipios": self.municipios,
            "cnaes": self.cnaes,
            "socios": self.socios,
            "simples": self.simples,
            "naturezas": self.naturezas,
            "qualificacoes": self.qualificacoes,
            "motivos": self.motivos,
            "paises": self.paises}

        # Se o base_path não for fornecido, pegar da variável de ambiente ou usar um padrão.
        if not base_path:
            base_path = os.environ.get('BASE_PATH', "./output")

        if schema_name in ['estabelecimentos', 'empresas', 'socios']:
            file_location_pattern = os.path.join(base_path, schema_name.capitalize(), '*.csv')
        else:
            file_location_pattern = os.path.join(base_path, schema_name.capitalize(), f"{schema_name.capitalize()}.csv")

        # Detectar codificações
        encodings = self.detect_encoding(file_location_pattern)
        self.logger.info(f"Detected encodings: {encodings}")

        # Agora, vamos ler cada arquivo com sua codificação correta e armazenar em uma lista de DataFrames
        dfs = []
        for file_location, encoding in encodings.items():
            df = (self.spark.read.format("csv")
                  .option("sep", ";")
                  .option("header", "false")
                  .option('quote', '"')
                  .option("escape", '"')
                  .option("encoding", encoding)
                  .schema(schemas[schema_name])
                  .load(file_location))
            dfs.append(df)

        # Unir todos os DataFrames em um único DataFrame
        if dfs:
            final_df = reduce(lambda a, b: a.union(b), dfs)
        else:
            final_df = self.spark.createDataFrame([], schemas[schema_name])

        return final_df
    

    # Define the UDF
    schema = StructType([
        StructField("latitude", FloatType(), nullable=True),
        StructField("longitude", FloatType(), nullable=True)
    ])
    
    @udf(schema)
    def geocode_udf(address):
        """
        UDF do Spark para geocodificar um endereço dentro de um DataFrame.

        Parâmetros:
            address (str): Endereço a ser geocodificado.

        Retorna:
            dict: Dicionário contendo a 'latitude' e a 'longitude' do endereço fornecido.
                  Se o endereço não puder ser geocodificado, os valores serão None.

        Exemplo:
            df.withColumn("location", geocode_udf(df["address"]))

        Notas:
            - Esta UDF encapsula a função `geocode_address`.
            - Retorna um tipo de dado complexo (Struct) com dois campos: 'latitude' e 'longitude'.
        """
        global geocode_address
        lat, lon = geocode_address(address)
        return {"latitude": lat, "longitude": lon}
    
    def process_estabelecimentos(self, df):
        
        """
        Processa e enriquece o DataFrame de estabelecimentos com informações adicionais e transformações.

        Parâmetros:
            df (DataFrame): DataFrame inicial contendo informações de estabelecimentos.

        Retorna:
            DataFrame: DataFrame processado e enriquecido com novas colunas e informações.

        Descrição:
            - Lê dataframes adicionais relacionados a países, municípios, cnaes e motivos.
            - Realiza renomeações de colunas para facilitar junções.
            - Enriquece o dataframe com informações de motivos, cnaes, municípios e países.
            - Processa colunas de e-mail, separando provedores e corrigindo valores.
            - Converte colunas de data de string para formato de data.
            - Deriva colunas de ano e mês a partir de datas.
            - Processa e deriva novas colunas com base em mapeamentos para situação cadastral e tipo de estabelecimento.
            - Valida endereços de e-mail usando expressões regulares.
            - Combina informações de endereço para formar uma coluna completa de endereço.
            - Utiliza a função de geocodificação para obter coordenadas com base no endereço e, em caso de falha, com base no CEP.
            - Realiza correções na coluna de provedor de e-mail usando um dicionário de mapeamento.

        Notas:
            - Esta função faz uso intensivo das operações de DataFrame do PySpark.
            - Dependências: A função depende de outras funções e UDFs, como 'geocode_udf', bem como de variáveis de instância, como 'dic_provedor'.
        """
        
        df_pais = self.read_data(schema_name='paises')
        df_mun = self.read_data(schema_name='municipios')
        df_cnaes = self.read_data(schema_name='cnaes')
        df_motivos = self.read_data(schema_name='motivos')
        
        df = df.withColumnRenamed("CNAE_1", "COD_CNAE")
        df = df.withColumnRenamed("MUNICIPIO", "ID_MUNICPIO")
        df = df.withColumnRenamed("PAIS", "COD_PAIS")
        df = df.withColumnRenamed("MOTIVO_CADASTRAL", "COD_MOTIVO") 
        

        df = df.join(broadcast(df_motivos), "COD_MOTIVO", "left").drop(df.COD_MOTIVO)
        df = df.join(broadcast(df_cnaes), "COD_CNAE", "left").drop(df.COD_CNAE)
        df = df.join(broadcast(df_mun), "ID_MUNICPIO", "left").drop(df.ID_MUNICPIO)
        df = df.join(broadcast(df_pais), "COD_PAIS", "left").drop(df.COD_PAIS)
        
        dic_provedor = self.dic_provedor
        # Tratamento da coluna provedor
        df = df.withColumn("PROVEDOR",  regexp_extract("EMAIL", "(?<=@)[^.]+(?=\\.)", 0))
        
        # Colocando em caixa alta o provedor
        df = df.withColumn("PROVEDOR", upper(col("PROVEDOR")))
        
        # Colocando em caixa baixa o email
        df = df.withColumn("EMAIL", lower(col("EMAIL")))

        # Convertendo colunas de data
        df = df.withColumn("DT_SIT_CADASTRAL", to_date(col('DT_SIT_CADASTRAL'), "yyyyMMdd"))
        df = df.withColumn("DT_INICIO_ATIVIDADE", to_date(col('DT_INICIO_ATIVIDADE'), "yyyyMMdd"))
        df = df.withColumn("DT_SIT_ESPECIAL", to_date(col('DT_SIT_ESPECIAL'), "yyyyMMdd"))
        
        df = df.withColumn( "ano_cadastro", year('DT_INICIO_ATIVIDADE'))
        df = df.withColumn( "mes_cadastro", month('DT_INICIO_ATIVIDADE'))
        df = df.withColumn( "ano_sit_cadastral", year('DT_SIT_CADASTRAL'))
        df = df.withColumn( "mes_sit_cadastral", month('DT_SIT_CADASTRAL'))
        
        # Defina o dicionário de mapeamento
        mapping = {1: 'NULA',2: 'ATIVA',3: 'SUSPENSA',4: 'INAPTA',8: 'BAIXADA'}
        
        # Use a função 'when' para criar a nova coluna 'NM_SIT_CADASTRAL'
        df = df.withColumn("NM_SIT_CADASTRAL",
                           when(df["SIT_CADASTRAL"].isin(list(mapping.keys())), df["SIT_CADASTRAL"]).otherwise(None))
        
        # Substitua os valores na nova coluna com base no dicionário de mapeamento
        for key, value in mapping.items():
            df = df.withColumn("NM_SIT_CADASTRAL", when(df["SIT_CADASTRAL"] == key, value).otherwise(df["NM_SIT_CADASTRAL"]))
            
        # Use uma expressão regular para validar os endereços de e-mail
        email_pattern = r'^\S+@\S+\.\S+$'  # Padrão simples de endereço de e-mail
        
        # Use a função 'regexp_extract' para extrair endereços de e-mail válidos
        df = df.withColumn("valid_email", regexp_extract(col("EMAIL"), email_pattern, 0))
        
        # Defina o dicionário de mapeamento
        mapping = {1: 'MATRIZ',2: 'FILIAL'}
        
        # Use a função 'when' para criar a nova coluna 'NM_MATRIZ_FILIAL'
        df = df.withColumn("NM_MATRIZ_FILIAL",
                           when(df["MATRIZ_FILIAL"].isin(list(mapping.keys())), df["MATRIZ_FILIAL"]).otherwise(None))
        
        # Substitua os valores na nova coluna com base no dicionário de mapeamento
        for key, value in mapping.items():
            df = df.withColumn("NM_MATRIZ_FILIAL", when(df["MATRIZ_FILIAL"] == key, value).otherwise(df["NM_MATRIZ_FILIAL"]))
            
        # Criando a nova coluna "ENDERECO_COMPLETO"
        df = df.withColumn("ENDERECO_COMPLETO",
                           concat_ws(", ",
                                     concat(df["TIPO_LOUGRADOURO"], lit(" "), df["LOGRADOURO"]),
                                     "NUMERO",concat_ws(" - ", "MUNICIPIO", "UF")))
        
        # Adicione a lógica de geocodificação aqui
        df = df.withColumn("COORDENADAS", ReceitaLT.geocode_udf(df["ENDERECO_COMPLETO"]))
        
        df = df.withColumn("COORDENADAS",
                           when((col("COORDENADAS.latitude").isNull()) & (col("COORDENADAS.longitude").isNull()),
                                ReceitaLT.geocode_udf(df["CEP"])).otherwise(col("COORDENADAS")))
        
        # Correção da coluna provedor
        df = df.replace(dic_provedor, subset=['PROVEDOR'])

        # Transformação das keys e values do dicionário em lowercase
        dic_prov_lower = {k.lower(): str(v).lower() for k, v in dic_provedor.items()}

        # Correção dos provedores na coluna EMAIL
        replace_expr = reduce(
            lambda a, b: regexp_replace(a, rf"\b{b[0]}\b", b[1]),
            dic_prov_lower.items(),
            col("valid_email"))

        df = df.withColumn("valid_email", replace_expr)
        df = df.withColumnRenamed("valid_email", "VALILD_EMAIL")
        
        return df
    
    def process_empresas(self, df):
        """
        Processa e enriquece o DataFrame de empresas com informações adicionais e transformações.

        Parâmetros:
            df (DataFrame): DataFrame inicial contendo informações de empresas.

        Retorna:
            DataFrame: DataFrame processado e enriquecido com novas colunas e informações.

        Descrição:
            - Lê dataframes adicionais relacionados a naturezas jurídicas e qualificações.
            - Realiza renomeação de colunas para facilitar junções.
            - Enriquece o dataframe com informações de naturezas jurídicas e qualificações.
            - Processa a coluna 'NOME_EMPRESA' para extrair informações potenciais de CPF.
            - Deriva uma nova coluna baseada no porte da empresa, usando um mapeamento predefinido.
            - Determina a probabilidade de um valor ser um CPF válido com base em seu comprimento.
            - Criptografa possíveis valores de CPF usando AES e os armazena em uma nova coluna 'CPF_CRIPTOGRAFADO', enquanto remove a coluna original 'CPF'.

        Notas:
            - Esta função faz uso intensivo das operações de DataFrame do PySpark.
            - O valor de criptografia (secret_key) é gerado dinamicamente a cada chamada da função. Portanto, cada execução resultará em valores de 'CPF_CRIPTOGRAFADO' diferentes para os mesmos CPFs.
            - O método AES usado aqui é 'ECB', que não é considerado seguro para muitos casos de uso devido à falta de vetor de inicialização (IV). A utilização deste modo deve ser revista se a segurança for uma preocupação.
        """
        df_nat = self.read_data(schema_name='naturezas')
        df_qual = self.read_data(schema_name='qualificacoes')
        
        df = df.withColumnRenamed("QUALIF_RESPONVAVEL", "COD_QUALIFICACAO")
        
        df = df.join(broadcast(df_nat), "COD_NAT_JURICA", "left").drop(df.COD_NAT_JURICA)
        df = df.join(broadcast(df_qual), "COD_QUALIFICACAO", "left").drop(df.COD_QUALIFICACAO)
               
        df = df.withColumn("CPF", regexp_replace("NOME_EMPRESA", "[^0-9]", ""))
        df = df.withColumn("CPF", when(col("CPF") == "", None).otherwise(col("CPF")))
        df = df.withColumn('CPF_LEN', length('CPF'))
        # Defina o dicionário de mapeamento
        mapping = {0: 'NÃO INFORMADO',1: 'MICRO EMPRESA',3: ' EMPRESA DE PEQUENO PORTE',5: 'DEMAIS',8: 'BAIXADA'}
        # Use a função 'when' para criar a nova coluna 'NM_SIT_CADASTRAL'
        df = df.withColumn("NM_PORTE",
                           when(df["PORTE"].isin(list(mapping.keys())), df["PORTE"]).otherwise(None))
        
        # Substitua os valores na nova coluna com base no dicionário de mapeamento
        for key, value in mapping.items():
            df = df.withColumn("NM_PORTE", when(df["PORTE"] == key, value).otherwise(df["NM_PORTE"]))

        df = df.withColumn("PROBABILIDADE_DE_SER_CPF", when(df["CPF_LEN"] == 11, "SIM").otherwise("NAO"))
        
        secret_key = secrets.token_urlsafe(24)
        df = df.withColumn("CPF_CRIPTOGRAFADO", expr(f"base64(aes_encrypt(CPF, '{secret_key}', 'ECB', 'PKCS'))")).drop(df.CPF)
        
        return df
    
    
    def process_simples(self, df):
        """
        Processa o DataFrame relacionado ao regime tributário SIMPLES das empresas.

        Parâmetros:
            df (DataFrame): DataFrame inicial contendo informações relacionadas ao regime tributário SIMPLES.

        Retorna:
            DataFrame: DataFrame processado com colunas de data convertidas e apenas as colunas relevantes selecionadas.

        Descrição:
            - Converte colunas que representam datas do formato "yyyyMMdd" para o tipo data.
            - Seleciona apenas as colunas relevantes para o contexto, que são: 'CNPJ_BASICO', 'OPÇAO_PELO_MEI', 'DT_OPCAO_MEI', 'DT_EXCLUSAO_MEI', 'OPCAO_PELO_SIMPLES', 'DT_OPCAO_SIMPLES', e 'DT_EXCLUSAO_SIMPLES'.

        Notas:
            - Esta função assume que as colunas de data estão no formato "yyyyMMdd" e realiza a conversão para o tipo data.
            - As colunas de datas que são processadas incluem: DATA_OPCAO_PELO_SIMPLES, DATA_EXCLUSAO_SIMPLES, DATA_EXCLUSAO_MEI e DATA_OPCAO_PELO_MEI.
        """
        df = df.withColumn("DT_OPCAO_SIMPLES", to_date,(col('DATA_OPCAO_PELO_SIMPLES'), "yyyyMMdd"))
        df = df.withColumn("DT_EXCLUSAO_SIMPLES", to_date(col('DATA_EXCLUSAO_SIMPLES'), "yyyyMMdd"))
        df = df.withColumn("DT_EXCLUSAO_MEI", to_date(col('DATA_EXCLUSAO_MEI'), "yyyyMMdd"))
        df = df.withColumn("DT_OPCAO_MEI", to_date(col('DATA_OPCAO_PELO_MEI'), "yyyyMMdd"))
        df = df.select('CNPJ_BASICO','OPÇAO_PELO_MEI','DT_OPCAO_MEI','DT_EXCLUSAO_MEI','OPCAO_PELO_SIMPLES','DT_OPCAO_SIMPLES','DT_EXCLUSAO_SIMPLES')
        
        return df
    
    
    def save_data(self, df, path, num_partitions=1, file_format="parquet"):
        """
        Save the DataFrame to the specified path.
        
        :param df: DataFrame to be saved
        :param path: Destination path
        :param num_partitions: Number of partitions for saving data (default is 1)
        :param file_format: File format to save the data (default is "parquet")
        """
        
        # Repartitioning the DataFrame based on user input
        df = df.repartition(num_partitions)
        
        # Saving the DataFrame to the specified path and format
        df.write.mode('overwrite').format(file_format).save(path)
        
    @staticmethod
    def download_nomes(save_base_path="./output/nomes"):        
        """
        Baixa e extrai o arquivo nomes.csv.gz do dataset genero-nomes no Brasil.io.

        Parâmetros:
            save_base_path (str, opcional): Caminho base onde o arquivo será salvo. O padrão é './output/nomes'.

        Descrição:
            - Cria o diretório de salvamento se ele não existir.
            - Baixa o arquivo nomes.csv.gz da URL especificada.
            - Extrai o conteúdo do arquivo .gz.
            - Remove o arquivo .gz original, mantendo apenas o arquivo CSV extraído.

        Notas:
            - Esta função usa a biblioteca `requests` para baixar o arquivo.
            - A função verifica se a resposta do servidor é 200 (sucesso) antes de baixar o arquivo.
            - O arquivo .gz é extraído usando a biblioteca `gzip`.
        """
        # Certifique-se de que o diretório de salvamento exista
        os.makedirs(save_base_path, exist_ok=True)
        url = "https://data.brasil.io/dataset/genero-nomes/nomes.csv.gz"
        # Derive o nome do arquivo da URL
        file_name = os.path.basename(url)
        file_path = os.path.join(save_base_path, file_name)
        extracted_file_path = os.path.join(save_base_path, file_name[:-3])  # remove .gz

        # Baixe o arquivo
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=128):
                    file.write(chunk)
        else:
            print(f"Failed to download {url}. Status code: {response.status_code}")
            return

        # Extraia o arquivo
        with gzip.open(file_path, 'rb') as f_in:
            with open(extracted_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Apague o arquivo .gz
        os.remove(file_path)
        
    def process_mei(self, df, save_base_path="./output/nomes", file_name="nomes.csv"):
        """
        Processa um DataFrame referente a MEIs, realiza joins com dados adicionais de naturezas jurídicas,
        qualificações, e um conjunto de dados de nomes para extração e categorização de primeiro nome.

        Parâmetros:
            df (pyspark.sql.DataFrame): DataFrame inicial contendo dados sobre MEIs.
            save_base_path (str, opcional): Caminho onde o arquivo com dados de nomes foi extraído. Padrão é './output/nomes'.
            file_name (str, opcional): Nome do arquivo CSV contendo dados de nomes a ser lido. Padrão é 'nomes.csv'.

        Retorna:
            pyspark.sql.DataFrame: DataFrame processado após todas as transformações e joins.

        Descrição:
            1. Realiza join com DataFrames de 'naturezas' e 'qualificações'.
            2. Extração e manipulação de dados de CPF.
            3. Utiliza um dicionário para mapear e criar a coluna "NM_PORTE".
            4. Criptografa a coluna de CPF.
            5. Realiza filtragens baseado na probabilidade do nome ser um CPF válido.
            6. Lê um conjunto de dados de nomes e realiza o explode na coluna 'alternative_names'.
            7. Extrai o primeiro nome da coluna 'NOME_EMPRESA'.
            8. Realiza o join com o conjunto de dados de nomes para categorizar o primeiro nome.
            9. Retorna um DataFrame contendo informações relevantes após todas as transformações.
        """

        df_nat = self.read_data(schema_name='naturezas')
        df_qual = self.read_data(schema_name='qualificacoes')
        
        df = df.withColumnRenamed("QUALIF_RESPONVAVEL", "COD_QUALIFICACAO")
        
        df = df.join(broadcast(df_nat), "COD_NAT_JURICA", "left").drop(df.COD_NAT_JURICA)
        df = df.join(broadcast(df_qual), "COD_QUALIFICACAO", "left").drop(df.COD_QUALIFICACAO)
               
        df = df.withColumn("CPF", regexp_replace("NOME_EMPRESA", "[^0-9]", ""))
        df = df.withColumn("CPF", when(col("CPF") == "", None).otherwise(col("CPF")))
        df = df.withColumn('CPF_LEN', length('CPF'))
        # Defina o dicionário de mapeamento
        mapping = {0: 'NÃO INFORMADO',1: 'MICRO EMPRESA',3: ' EMPRESA DE PEQUENO PORTE',5: 'DEMAIS',8: 'BAIXADA'}
        # Use a função 'when' para criar a nova coluna 'NM_SIT_CADASTRAL'
        df = df.withColumn("NM_PORTE",
                           when(df["PORTE"].isin(list(mapping.keys())), df["PORTE"]).otherwise(None))
        
        # Substitua os valores na nova coluna com base no dicionário de mapeamento
        for key, value in mapping.items():
            df = df.withColumn("NM_PORTE", when(df["PORTE"] == key, value).otherwise(df["NM_PORTE"]))

        df = df.withColumn("PROBABILIDADE_DE_SER_CPF", when(df["CPF_LEN"] == 11, "SIM").otherwise("NAO"))
        
        secret_key = secrets.token_urlsafe(24)
        df = df.withColumn("CPF_CRIPTOGRAFADO", expr(f"base64(aes_encrypt(CPF, '{secret_key}', 'ECB', 'PKCS'))")).drop(df.CPF)
        
        # Caminho completo do arquivo
        file_path = os.path.join(save_base_path, file_name)
        
        # Filtrar df_processed baseado na coluna PROBABILIDADE_DE_SER_CPF
        df_filter = df.filter(col('PROBABILIDADE_DE_SER_CPF') == 'SIM').dropDuplicates(subset=['CPF_CRIPTOGRAFADO', 'NOME_EMPRESA'])

        # Ler o arquivo CSV
        df = self.spark.read.csv(file_path, header=True, inferSchema=True)

        # Explodir a coluna alternative_names para múltiplas linhas
        df_expanded = df.withColumn("alternative_names", explode(split(coalesce(col("alternative_names"), col("first_name")), "\\|")))

        # Selecionar as colunas desejadas
        df_result = df_expanded.select("alternative_names", "group_name", "ratio", "classification").dropDuplicates(subset=['alternative_names'])

        # Extrair o primeiro nome da coluna NOME_EMPRESA
        df_filter = df_filter.withColumn("PRIMEIRO_NOME", split(col("NOME_EMPRESA"), " ")[0])
        
        # Fazer o join entre df_filter e df_result
        joined_df = df_filter.join(df_result, df_filter.PRIMEIRO_NOME == df_result.alternative_names, "left").dropDuplicates()
        
        joined_df = joined_df.select('CNPJ','NOME_EMPRESA','CAP_SOCIAL','NM_PORTE','NAT_JURICA','ENTE_FEDERATIVO','NM_QUALIFICACAO','CPF_CRIPTOGRAFADO','CPF_LEN',
                    'PROBABILIDADE_DE_SER_CPF','PRIMEIRO_NOME',col('group_name').alias('GRUPO_NOME'), 
                    col('ratio').alias('PROBABILIDADE_CLASSIFICACAO'), col('classification').alias('CLASSIFICACAO')).dropDuplicates()
        
        return joined_df
    
    def process_socios(self, df, save_base_path="./output/nomes", file_name="nomes.csv"):
        """
        Processa um DataFrame referente a sócios, realiza joins com dados adicionais de países, qualificações, 
        e um conjunto de dados de nomes para extração e categorização de primeiro nome.

        Parâmetros:
            df (pyspark.sql.DataFrame): DataFrame inicial contendo dados sobre sócios.
            save_base_path (str, opcional): Caminho onde o arquivo com dados de nomes foi extraído. Padrão é './output/nomes'.
            file_name (str, opcional): Nome do arquivo CSV contendo dados de nomes a ser lido. Padrão é 'nomes.csv'.

        Retorna:
            pyspark.sql.DataFrame: DataFrame processado após todas as transformações e joins.

        Descrição:
            1. Realiza join com DataFrames de 'países'.
            2. Usa mapeamentos para criar colunas "NM_FAIXA_ETARIA" e "NM_IDENTIFICADOR_SOCIO".
            3. Renomeia e realiza join com DataFrame de qualificações para obter descrições das qualificações.
            4. Converte coluna de data "DATA_ENTRADA_SOCIEDADE" para o formato desejado.
            5. Lê e processa um conjunto de dados de nomes, explodindo e selecionando colunas relevantes.
            6. Extração do primeiro nome da coluna 'NOME_SOCIO_RAZAO_SOCIAL'.
            7. Realiza o join entre o DataFrame processado e o conjunto de dados de nomes para categorizar o primeiro nome.
            8. Retorna um DataFrame contendo informações relevantes após todas as transformações.
        """
        
        df_pais = self.read_data(schema_name='paises')
        df = df.withColumnRenamed("PAIS", "COD_PAIS")
        df = df.join(broadcast(df_pais), "COD_PAIS", "left").drop(df.COD_PAIS)
        
        # Mapeamento de códigos para faixas etárias.
        mapping = {
            1: '0 a 12 anos',
            2: '13 a 20 anos',
            3: '21 a 30 anos',
            4: '31 a 40 anos',
            5: '41 a 50 anos',
            6: '51 a 60 anos',
            7: '61 a 70 anos',
            8: '71 a 80 anos',
            9: 'maiores de 80 anos',
            0: 'NA'
        }
        
                
        # Mapeamento de códigos para faixas etárias.
        id_socio = {
            1: 'PESSOA JURIDICA',
            2: 'PESSOA FISICA',
            3: 'ESTRANGEIRO'}
        
        df_qual = self.read_data(schema_name='qualificacoes')
        
        
        # Use a função 'when' para criar a nova coluna 'NM_SIT_CADASTRAL'
        df = df.withColumn("NM_FAIXA_ETARIA",
                           when(df["FAIXA_ETARIA"].isin(list(mapping.keys())), df["FAIXA_ETARIA"]).otherwise(None))
        
        # Substitua os valores na nova coluna com base no dicionário de mapeamento
        for key, value in mapping.items():
            df = df.withColumn("NM_FAIXA_ETARIA", when(df["FAIXA_ETARIA"] == key, value).otherwise(df["NM_FAIXA_ETARIA"]))
            

        # Use a função 'when' para criar a nova coluna 'NM_SIT_CADASTRAL'
        df = df.withColumn("NM_IDENTIFICADOR_SOCIO",
                           when(df["IDENTIFICADOR_SOCIO"].isin(list(id_socio.keys())), df["IDENTIFICADOR_SOCIO"]).otherwise(None))
        
        # Substitua os valores na nova coluna com base no dicionário de mapeamento
        for key, value in id_socio.items():
            df = df.withColumn("NM_IDENTIFICADOR_SOCIO", when(df["IDENTIFICADOR_SOCIO"] == key, value).otherwise(df["NM_IDENTIFICADOR_SOCIO"]))


        df_qual = self.read_data(schema_name='qualificacoes')
        # Renomeação e join com df_qual para obter descrições de qualificações.
        df = df.withColumnRenamed("QUALIFICACAO_REPRESENTANTE_LEGAL", "COD_QUALIFICACAO")
        df = df.join(broadcast(df_qual), "COD_QUALIFICACAO", "left").drop("COD_QUALIFICACAO")
        df = df.withColumnRenamed("NM_QUALIFICACAO", "NM_QUALIFICACAO_REPRESENTANTE_LEGAL")

        df = df.withColumnRenamed("QUALIFICAÇAO_SOCIO", "COD_QUALIFICACAO")
        df = df.join(broadcast(df_qual), "COD_QUALIFICACAO", "left").drop("COD_QUALIFICACAO")
        df = df.withColumnRenamed("NM_QUALIFICACAO", "NM_QUALIFICAÇAO_SOCIO")

        # Conversão da coluna de data.
        df = df.withColumn("DT_ENTRADA_SOCIEDADE", to_date(col('DATA_ENTRADA_SOCIEDADE'), "yyyyMMdd")).drop(df.DATA_ENTRADA_SOCIEDADE)

        # Leitura do arquivo CSV.
        file_path = os.path.join(save_base_path, file_name)
        df_csv = self.spark.read.csv(file_path, header=True, inferSchema=True)

        df_csv = df_csv.withColumn("alternative_name2", explode(split(df_csv["alternative_names"], "\|")))
        df_result = df_csv.select("alternative_name2", "group_name", "ratio", "classification").dropDuplicates(["alternative_name2"])

        # Extração do primeiro nome.
        df = df.withColumn("PRIMEIRO_NOME", split(col("NOME_SOCIO_RAZAO_SOCIAL"), " ")[0]).dropDuplicates()

        # Join entre dataframes.
        joined_df = df.join(df_result, df.PRIMEIRO_NOME == df_result.alternative_name2, "left").dropDuplicates()
        
        joined_df = joined_df.select('CNPJ_BASICO','NOME_SOCIO_RAZAO_SOCIAL','CNPJ_CPF_SOCIO','REPRESENTANTE_LEGAL',
        'NOME_REPRESENTANTE','NM_PAIS','NM_FAIXA_ETARIA','NM_IDENTIFICADOR_SOCIO','NM_QUALIFICACAO_REPRESENTANTE_LEGAL',
        'NM_QUALIFICAÇAO_SOCIO','DT_ENTRADA_SOCIEDADE','PRIMEIRO_NOME',col('ratio').alias('PROBABILIDADE_CLASSIFICACAO'),
        col('classification').alias('CLASSIFICACAO')).dropDuplicates()

        return joined_df