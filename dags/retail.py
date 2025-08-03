# retail.py

from airflow.decorators import dag, task
from datetime import datetime

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG, execution_config
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig



@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['retail'],
)
def retail():

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/dataset/online_retail.csv',
        dst='raw/online_retail.csv',
        bucket='arabiteff_online_retail',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )
    
    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='gcp',
    )
    
    gcs_to_raw = GCSToBigQueryOperator(
    task_id='gcs_to_raw',
    bucket='arabiteff_online_retail',
    source_objects=['raw/online_retail.csv'],
    destination_project_dataset_table='retail.raw_invoices',
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    schema_fields=[
        {"name": "InvoiceNo", "type": "STRING", "mode": "NULLABLE"},
        {"name": "StockCode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Quantity", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "InvoiceDate", "type": "STRING", "mode": "NULLABLE"},
        {"name": "UnitPrice", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "CustomerID", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "Country", "type": "STRING", "mode": "NULLABLE"},
    ],
    gcp_conn_id='gcp',
    )
    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        ),
        execution_config=execution_config
    )
	
    dbt_test = DbtTaskGroup(
    group_id='dbt_test',	
    project_config=DBT_PROJECT_CONFIG,
    profile_config=DBT_CONFIG,
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        select=['test']  # This will run all dbt tests
    ),
    execution_config=execution_config
	)


retail()