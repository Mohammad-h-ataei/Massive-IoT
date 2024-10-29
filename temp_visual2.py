from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt

# Connect to Kafka consumer
consumer = KafkaConsumer(
    'temp',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Define data and categories for interpretation analysis
data = {'dangerous': [], 'need attention': [], 'good': [], 'perfect': []}
categories = {'dangerous': lambda x: x < 0, 'need attention': lambda x: 0 <= x < 10, 'good': lambda x: 10 <= x < 15, 'perfect': lambda x: 15 <= x <= 30}

# Process Kafka messages
for message in consumer:
    celsius = message.value['celsius']
    # Classify data into categories
    for category, condition in categories.items():
        if condition(celsius):
            data[category].append(celsius)
            break

    # Plot data and categories
    plt.clf()
    plt.hist(data.values(), bins=10, label=data.keys())
    plt.xlabel('Celsius')
    plt.ylabel('Frequency')
    plt.title('Temperature Analysis')
    plt.legend()
    plt.draw()
    plt.pause(0.01)

    # Print interpretation analysis
    if len(data['dangerous']) > 0:
        print('Temperature is dangerous!')
    elif len(data['need attention']) > 0:
        print('Temperature needs attention.')
    elif len(data['good']) > 0:
        print('Temperature is good.')
    elif len(data['perfect']) > 0:
        print('Temperature is perfect!')
