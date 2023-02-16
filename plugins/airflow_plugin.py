from airflow.plugins_manager import AirflowPlugin
from operators.twitter_operator import TwitterOperator
from operators.alfa_operator import AlOperator

#change the name
class CapraAirflowPlugin(AirflowPlugin):
    name = "capra"
    operators = [TwitterOperator, AlOperator]
