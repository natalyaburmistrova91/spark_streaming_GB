from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='bigdataanalytics2-worker-shdpt-v31-1-2:6667')
orders_topic_name = 'YOUR_ORDERS_TOPIC_NAME'

orders_json = open('orders_dataset.json', 'r')
orders = orders_json.readlines()

for order in orders:
    producer.send(orders_topic_name, order)