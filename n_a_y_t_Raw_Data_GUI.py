from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
from bokeh.server.server import Server
from bokeh.themes import Theme
from bokeh.layouts import column
from kafka import KafkaConsumer
import json
from datetime import datetime
import time

consumer = KafkaConsumer("temp",
                         bootstrap_servers=["localhost:9092"],
                         auto_offset_reset="latest",
                         value_deserializer=lambda m: json.loads(m.decode("utf-8")))

def update_data():
    global source
    data = {"x": [], "y": []}
    for message in consumer:
        temperature = message.value["celsius"]
        data["x"].append(datetime.now())
        data["y"].append(temperature)
        source.stream(data, rollover=100)

source = ColumnDataSource({"x": [], "y": []})

p = figure(title="Temperature Plot",
           x_axis_label="Time",
           y_axis_label="Temperature (°C)",
           x_axis_type="datetime")

p.line(x="x", y="y", source=source)

def modify_doc(doc):
    doc.add_root(column(p))

server = Server({'/': modify_doc}, num_procs=1)
server.start()

# Update the plot with new data in real-time
update_data()

server.stop()
