from flask import Flask, render_template
from flask import request
from flask import redirect
from flask import Response
from kafka import KafkaConsumer
from flask import url_for
import json
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import io
import time

# Set up Kafka consumer
consumer = KafkaConsumer(
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

app = Flask(__name__, template_folder='4.KAFKA APP DEMO')

# Home page that displays a form to select the Kafka topic
@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
        topic = request.form.get("topic")
        return redirect(url_for("plot", topic=topic))
    return render_template("home.html")

# Route that generates the real-time plot for the selected Kafka topic
@app.route("/plot/<topic>")
def plot(topic):
    fig = Figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Temperature (°C)")
    xs = []
    ys = []

    consumer.subscribe([topic])

    def gen():
        while True:
            for message in consumer:
                data = message.value
                temperature = data["celsius"]
                xs.append(int(time.time()))
                ys.append(temperature)
                ax.clear()
                ax.plot(xs, ys)
                fig.canvas.draw()

                # Convert Matplotlib figure to PNG image
                buf = io.BytesIO()
                canvas = FigureCanvas(fig)
                canvas.print_png(buf)
                data = buf.getvalue()

                yield (b"--frame\r\n"
                       b"Content-Type: image/png\r\n\r\n" + data + b"\r\n")

    return Response(gen(), mimetype="multipart/x-mixed-replace; boundary=frame")

if __name__ == "__main__":
    app.run(debug=True)
