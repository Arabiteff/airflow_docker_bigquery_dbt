# include/dbt/cosmos_config.py

from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from pathlib import Path

DBT_CONFIG = ProfileConfig(
    profile_name='retail',
    target_name='dev',
    profiles_yml_filepath=Path('/opt/airflow/include/dbt/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/opt/airflow/include/dbt/',
)

execution_config = ExecutionConfig(
    dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt"
)