from kafka import KafkaConsumer
import json

if __name__ == "__main__":
    consumer = KafkaConsumer(
        "pinnacle",
        bootstrap_servers='0.0.0.0:9092',
        auto_offset_reset='earliest',
        group_id="consumer-group-a")
    print("starting the consumer")
    for msg in consumer:
        print("pinnacle = {}".format(json.loads(msg.value)))