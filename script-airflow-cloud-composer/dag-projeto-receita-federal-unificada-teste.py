from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.utils.dates import days_ago

# Definições do Cluster
CLUSTER_NAME = 'projeto-receita-federal-2'
PROJECT_ID = 'kinetic-valor-414812'
REGION = 'us-central1'
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-ssd",
            "boot_disk_size_gb": 50
        }
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "custom-2-12288-ext",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 50
        }
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "spark:spark.dataproc.enhanced.optimizer.enabled": "true"
        }
    },
    "endpoint_config": {
        "enable_http_port_access": True
    }
    #"optional_components": ["JUPYTER"],
    #config_bucket": "gs://projeto-dados-receita-federal"  # Corrigido: Adicionada a vírgula
    #"metadata": {
    #    "PIP_PACKAGES": "google-cloud-bigquery google-cloud-storage"
    #}
}

# Definições da DAG
default_args = {
    'owner': 'pedro-almeida',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_receita_federal_unificada',
    default_args=default_args,
    description='Uma DAG para processar dados de CNPJ usando Dataproc',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['teste-projeto'],
)

# Tarefa para criar o cluster Dataproc
create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag,
)

# Tarefa para enviar o trabalho ao Dataproc
download_unzip_upload_bq_data = DataprocSubmitJobOperator(
    task_id='download_unzip_upload_bq_data',
    job={
        'reference': {'project_id': PROJECT_ID},
        'placement': {'cluster_name': CLUSTER_NAME},
        'pyspark_job': {'main_python_file_uri': 'gs://projeto-dados-receita-federal/script-dag-dataproc/script-spark-dag.py'},
    },
    region=REGION,
    project_id=PROJECT_ID, 
    dag=dag,
)


# Tarefa para excluir o cluster
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,
)

# Definindo dependências
create_cluster >> download_unzip_upload_bq_data >>  delete_cluster
