#!/usr/bin/env python3
import asyncio
import json
import logging
import paho.mqtt.client as mqtt

class AsyncMQTTForwarder:
    def __init__(self):
        logging.debug("Initializing AsyncMQTTForwarder")

        # MQTT broker settings
        self.mqtt_host = "localhost"
        self.mqtt_port = 1883
        self.mqtt_username = "s53zo"
        self.mqtt_password = "mqtt"
        self.mqtt_use_tls = False

        self.message_queue = asyncio.Queue()
        self.loop = asyncio.get_event_loop()

        # Set up MQTT client
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.enable_logger()

        if self.mqtt_username and self.mqtt_password:
            self.mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_password)

        # Assign event callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect

     
    async def start(self):
        logging.info("Starting MQTT forwarder")
        # Connect to MQTT broker in a separate thread
        await self.loop.run_in_executor(None, self.mqtt_client.connect, self.mqtt_host, self.mqtt_port)
        self.mqtt_client.loop_start()
        # Start the message processing task
        self.message_processor_task = asyncio.create_task(self.process_messages())

    async def stop(self):
        logging.info("Stopping MQTT forwarder")
        self.mqtt_client.loop_stop()
        await self.loop.run_in_executor(None, self.mqtt_client.disconnect)
        # Cancel the background task
        self.message_processor_task.cancel()
        try:
            await self.message_processor_task
        except asyncio.CancelledError:
            logging.debug("Message processor task cancelled")

    async def add_message(self, message):
        logging.debug(f"Adding message to queue: {message}")
        await self.message_queue.put(message)

    async def process_messages(self):
        logging.info("Starting message processing loop")
        try:
            while True:
                message = await self.message_queue.get()
                logging.debug(f"Processing message: {message}")
                topic = self.format_mqtt_topic(message)
                payload = json.dumps(message)
                try:
                    result = await self.loop.run_in_executor(
                        None, self.mqtt_client.publish, topic, payload
                    )
                    logging.debug(f"Published message to {topic}: {result}")
                except Exception as e:
                    logging.error(f"Failed to publish message: {e}")
                self.message_queue.task_done()
        except asyncio.CancelledError:
            logging.debug("Message processing loop cancelled")

    def format_mqtt_topic(self, message):
        # Define how to format the MQTT topic based on the message
        callsign = message.get('callsign', 'unknown')
        contest = message.get('contest', 'unknown')
        topic = f"livescore/{contest}/{callsign}"
        logging.debug(f"Formatted MQTT topic: {topic}")
        return topic

    # MQTT event callbacks
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT broker")
        else:
            logging.error(f"Failed to connect to MQTT broker: {rc}")

    def on_disconnect(self, client, userdata, rc):
        logging.info("Disconnected from MQTT broker")
