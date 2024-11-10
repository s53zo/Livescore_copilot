import asyncio
import json
import logging
import paho.mqtt.client as mqtt

class AsyncMQTTForwarder:
    def __init__(self, mqtt_host="localhost", mqtt_port=1883, mqtt_username="live", mqtt_password="Hercules"):
        logging.debug("Initializing AsyncMQTTForwarder")
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_username = mqtt_username
        self.mqtt_password = mqtt_password
        self.message_queue = asyncio.Queue()
        self.loop = asyncio.get_event_loop()
        self.mqtt_client = mqtt.Client()
        self._configure_client()

    def _configure_client(self):
        self.mqtt_client.enable_logger()
        if self.mqtt_username and self.mqtt_password:
            self.mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_password)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect

    async def connect(self):
        logging.info("Connecting to MQTT broker")
        await self.loop.run_in_executor(None, self.mqtt_client.connect, self.mqtt_host, self.mqtt_port)
        self.mqtt_client.loop_start()

    async def disconnect(self):
        logging.info("Disconnecting from MQTT broker")
        self.mqtt_client.loop_stop()
        await self.loop.run_in_executor(None, self.mqtt_client.disconnect)

    async def add_message(self, message):
        logging.debug(f"Adding message to queue: {message}")
        await self.message_queue.put(message)

    async def process_messages(self):
        while True:
            message = await self.message_queue.get()
            topic = self.format_mqtt_topic(message)
            payload = json.dumps(message)
            await self._publish(topic, payload)

    async def _publish(self, topic, payload):
        try:
            await self.loop.run_in_executor(None, self.mqtt_client.publish, topic, payload)
            logging.debug(f"Published message to {topic}")
        except Exception as e:
            logging.error(f"Failed to publish message: {e}")

    def on_connect(self, client, userdata, flags, rc):
        logging.info("Connected to MQTT broker" if rc == 0 else f"Failed to connect: {rc}")

    def on_disconnect(self, client, userdata, rc):
        logging.info("Disconnected from MQTT broker")
