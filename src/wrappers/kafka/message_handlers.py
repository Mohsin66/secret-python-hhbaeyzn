from src.wrappers.kafka.producer import Producer
from src.utils.logger import get_logger

logger = get_logger(__name__)

async def handle_orders(message):
    """
    Handles messages from the 'orders' topic.
    """
    producer = Producer()
    data = message.value
    logger.info(f"📦 Processing Order: {data}")
    await producer.initialize()  # Ensure Kafka is initialized
    
    # Sending a test message
    await producer.send_message(
        topic="filtered-gateway-notification",
        key="message-key",
        value="Hello from another file!"
    )

async def handle_payments(message):
    """
    Handles messages from the 'payments' topic.
    """
    data = message.value
    logger.info(f"💰 Processing Payment: {data}")

async def convert_json_to_csv(message):
    """
    Handles messages from the 'shipments' topic.
    """
    data = message.value
    logger.info(f"🚚 Processing Shipment: {data}")
