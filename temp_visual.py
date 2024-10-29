import matplotlib.pyplot as plt
from data import get_registered_user
import time

# Generate initial empty data for the plot
celsius_data = []
time_data = []

# Create a figure and axis object for the plot
fig, ax = plt.subplots()

# Set up the axis labels
ax.set_xlabel('Time')
ax.set_ylabel('Temperature (°C)')

# Set the title of the plot
ax.set_title('Celsius Temperature over Time')

# Display the plot window
plt.ion()
plt.show()

while True:
    # Generate random Celsius temperature data
    celsius = get_registered_user()['celsius']

    # Get the current time
    now = time.time()

    # Add the new data to the plot
    celsius_data.append(celsius)
    time_data.append(now)

    # Limit the plot to the last 60 seconds of data
    ax.set_xlim(now - 60, now)

    # Update the plot with the new data
    ax.plot(time_data, celsius_data, 'r-', linewidth=2)

    # Refresh the plot window
    plt.draw()
    plt.pause(0.001)
