"""Contains functionality related to Weather"""
import logging
import json

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("weather process_message is incomplete - skipping")
        try:
            value = json.loads(message.value())
            temperature = value.get("temperature")
            status = value.get("status")
            if temperature is None or status is None:
                logger.debug("unable to handle message due to missing data")
            else:
                self.temperature = temperature
                self.status = status 
        except Exception as e:
                logger.fatal("bad weather data? %s, %s", value, e)