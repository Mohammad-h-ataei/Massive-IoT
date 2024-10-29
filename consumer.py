from kafka import KafkaConsumer
import json

if __name__ == "__main__":
    consumer = KafkaConsumer(
        "ibNav",
        bootstrap_servers='0.0.0.0:9092',
        auto_offset_reset='earliest',
        group_id="consumer-group-b")
    print("starting the consumer")
    for msg in consumer:
        print("ibNav = {}".format(json.loads(msg.value)))