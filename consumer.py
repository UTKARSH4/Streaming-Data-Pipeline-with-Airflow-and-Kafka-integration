from cassandra.cluster import Cluster
import json


def consume_function(message, name):

    message_content = json.loads(message.value())
    # connect to cassandra
    cluster = Cluster(protocol_version=5)
    session = cluster.connect('mypsace')
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
