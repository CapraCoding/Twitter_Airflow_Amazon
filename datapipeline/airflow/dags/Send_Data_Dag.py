import sys

sys.path.append('/airflow/plugins')

from os.path import join
from airflow.models import DAG, TaskInstance
from operators.twitter_operator import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago
from pathlib import Path

ARGS = {
    "owner": "Capra",
    "depends_on_past": False,
    "start_date": days_ago(6)
}
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

#partition with data of your extraction
PARTITION_FOLDER = "extract_date = {{ ds }}"

#CHANGE THE PATH OF YOUR DATA
BASE_FOLDER = join(
    str(Path("~/Documentos").expanduser()),
    "datapipeline/datalake/{stage}/{query}/{partition}"
)


with DAG(dag_id="twitter_dag_database",
         default_args=ARGS,
         schedule_interval="0 9 * * *",
         max_active_runs=1
         ) as dag:

    twitter_operator_results = TwitterOperator(
        task_id="twitter_results_database",
        query="CHANGE YOUR QUERY",
        file_path=join(
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "query_{{ ds_nodash }}.json"
        ),
        start_time=("{{"
                    f"execution_date.strftime('{ TIMESTAMP_FORMAT }')"
                    "}}"),
        end_time=("{{"
                  f"next_execution_date.strftime('{ TIMESTAMP_FORMAT }')"
                  "}}")
    )

    twitter_BDD = SparkSubmitOperator(
        task_id="BDD_results",
        application=join(
            str(Path(__file__).parents[2]),
            "datalake/spark/spark_post.py"
                         ),
        name="twitter_BD",
        application_args=[
            "--src",
            BASE_FOLDER.format(stage="bronze", query = "CHANGE YOUR QUERY", partition=PARTITION_FOLDER),
        ]

    )

twitter_operator_results >> twitter_BDD
