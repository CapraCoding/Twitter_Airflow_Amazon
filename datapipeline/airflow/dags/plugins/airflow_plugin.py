from airflow.plugins_manager import AirflowPlugin
from operators.twitter_operator import TwitterOperator
from operators.alfa_operator import AlfaOperator


class CapraAirflowPlugin(AirflowPlugin):
    name = "capra"
    operators = [TwitterOperator, AlfaOperator]
