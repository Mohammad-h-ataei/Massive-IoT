from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
import time

# Set up Kafka consumer
consumer = KafkaConsumer("temp",
                         bootstrap_servers=["localhost:9092"],
                         auto_offset_reset="latest",
                         value_deserializer=lambda m: json.loads(m.decode("utf-8")))

# Set up Matplotlib plot
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.set_xlabel("Time (s)")
ax.set_ylabel("Temperature (°C)")
xs = []
ys = []


# Continuously update plot with new temperature data
for message in consumer:
    data = message.value
    temperature = data["celsius"]
    xs.append(int(time.time()))
    ys.append(temperature)
    ax.clear()
    ax.plot(xs, ys)
    fig.canvas.draw()
    plt.pause(0.001)
