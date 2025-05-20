# Kafka IoT Sensor Monitoring – Task Description

## 1. Creating Kafka Topics

Create three Kafka topics:

- `building_sensors` – to store data from all sensors  
- `temperature_alerts` – to store alerts about exceeded temperature limits  
- `humidity_alerts` – to store alerts about out-of-range humidity values  


---

## 2. Sending Data to Topics

Write a Python script that simulates a sensor and periodically sends randomly generated temperature and humidity data to the `building_sensors` topic.

- Each message must contain:
  - Sensor ID (a constant random number per script run)
  - Timestamp (current time)
  - Temperature value (random number between **25°C and 45°C**)
  - Humidity value (random number between **15% and 85%**)

**Notes:**

- One script run simulates one sensor only.
- To simulate multiple sensors, run the script multiple times in parallel.
- Sensor ID should remain constant within a single run, but can differ in other runs.

---

## 3. Processing Incoming Data

Write a Python script that subscribes to the `building_sensors` topic, reads the incoming data, and checks the following conditions:

- If **temperature > 40°C**, generate an alert and send it to the `temperature_alerts` topic.
- If **humidity < 20% or > 80%**, generate an alert and send it to the `humidity_alerts` topic.

Each alert message should include:

- Sensor ID  
- Timestamp  
- The value that triggered the alert  
- A descriptive alert message

---

## 4. Consuming Final Alerts

Write a Python script that subscribes to the `temperature_alerts` and `humidity_alerts` topics, reads the alert messages, and prints them to the screen.


