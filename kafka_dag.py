from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import json
import random
from airflow import DAG
from cassandra.cluster import Cluster



KAFKA_TOPIC = "test"


def prod_function(num_treats=100):

    product_name_list = ["Laptop", "Desktop Computer", "Mobile Phone", "Wrist Band", "Wrist Watch", "LAN Cable",
                         "HDMI Cable", "TV", "TV Stand", "Text Books", "External Hard Drive", "Pen Drive", "Online Course"]

    order_card_type_list = ["Visa", "MasterCard", "Maestro"]

    country_name_city_name_list = ["Sydney,Australia", "Florida,United States", "New York City,United States",
                                   "Paris,France", "Colombo,Sri Lanka", "Dhaka,Bangladesh", "Islamabad,Pakistan",
                                   "Beijing,China", "Rome,Italy", "Berlin,Germany", "Ottawa,Canada",
                                   "London,United Kingdom", "Jerusalem,Israel", "Bangkok,Thailand",
                                   "Chennai,India", "Bangalore,India", "Mumbai,India", "Pune,India",
                                   "New Delhi,Inida", "Hyderabad,India", "Kolkata,India", "Singapore,Singapore"]

    ecommerce_website_name_list = ["www.datamaking.com", "www.amazon.com", "www.flipkart.com", "www.snapdeal.com", "www.ebay.com"]

    message_list = []
    message = None
    for i in range(500):
        i = i + 1
        message = {}
        print("Preparing message: " + str(i))
        event_datetime = datetime(2023, 4, 1)

        message["order_id"] = i
        message["order_product_name"] = random.choice(product_name_list)
        message["order_card_type"] = random.choice(order_card_type_list)
        message["order_amount"] = round(random.uniform(5.5, 555.5), 2)
        message["order_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
        country_name = None
        city_name = None
        country_name_city_name = None
        country_name_city_name = random.choice(country_name_city_name_list)
        country_name = country_name_city_name.split(",")[1]
        city_name = country_name_city_name.split(",")[0]
        message["order_country_name"] = country_name
        message["order_city_name"] = city_name
        message["order_ecommerce_website_name"] = random.choice(ecommerce_website_name_list)
        yield (
            json.dumps(i),
            json.dumps(message)
            )



def consume_function(message, name):
    # key = json.loads(message.key())
    # message_content = json.loads(message.value())
    # pet_name = message_content["pet_name"]
    # pet_mood_post_treat = message_content["pet_mood_post_treat"]


    # connect to cassandra
    cluster = Cluster(protocol_version=5) 
    session=cluster.connect('mypsace')
    message_content = json.loads(message.value())
    if message_content["order_id"]!=23:
        session.execute('Insert into mypsace.consumed (order_id,order_product_name,order_card_type,order_amount,order_country_name,order_city_name,order_ecommerce_website_name) values (' + 
                         str(message_content["order_id"]) + ",'"+ 
                         message_content["order_product_name"] + "','"+  
                         message_content["order_card_type"] + "','"+   
                         str(message_content["order_amount"]) + "','"+   
                         message_content["order_country_name"] + "','"+  
                         message_content["order_city_name"] + "','"+  
                         message_content["order_ecommerce_website_name"] +"');"
                         )
    print("Inserted record with order_id:",message_content["order_id"])
    # print('Insert into mypsace.consumed (order_id,order_product_name,order_card_type,order_amount,order_country_name,order_city_name,order_ecommerce_website_name) values (' + 
    #                      str(message_content["order_id"]) + ",'"+ 
    #                      message_content["order_product_name"] + "','"+  
    #                      message_content["order_card_type"] + "','"+   
    #                      str(message_content["order_amount"]) + "','"+   
    #                      message_content["order_country_name"] + "','"+  
    #                      message_content["order_city_name"] + "','"+  
    #                      message_content["order_ecommerce_website_name"] +"');'" 
    #                      )


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


