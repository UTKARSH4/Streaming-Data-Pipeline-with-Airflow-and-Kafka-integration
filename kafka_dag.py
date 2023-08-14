from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from consumer import  consume_function
from producer import prod_function
from airflow import DAG


KAFKA_TOPIC = "test"
# @dag(
with DAG("kafka_dag", start_date=datetime(2023, 4, 1), schedule=None, catchup=False,
         render_template_as_native_obj=True, ):
    producer = ProduceToTopicOperator(
        task_id="produce_treats",
        kafka_config_id="kafka_default",
        topic=KAFKA_TOPIC,
        producer_function=prod_function,
        # producer_function_args=["{{ ti.xcom_pull(task_ids='get_number_of_treats')}}"],
        # producer_function_kwargs={"prefix": "produced:::"},
        poll_timeout=10,
    )
    consumer = ConsumeFromTopicOperator(
        task_id="consume_treats",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC],
        apply_function=consume_function,
        apply_function_kwargs={"name": "consumed:::"},
        poll_timeout=20,
        max_messages=1000,
    )
    producer >> consumer


