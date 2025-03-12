import asyncio
from fastapi import FastAPI
from src.wrappers.kafka.consumer import ConsumerService
from src.wrappers.kafka.message_handlers import handle_orders, handle_payments, convert_json_to_csv
from src.utils.logger import get_logger

logger = get_logger(__name__)
app = FastAPI(title="Kafka Consumer Service")

consumer_service = ConsumerService()

@app.on_event("startup")
async def start_consumers():
    """
    Register all consumers when FastAPI starts.
    """
    logger.info("🚀 Starting Kafka consumers...")
    topic_handlers = {
        "fleet_data": handle_orders,
        "filtered-gateway-notification": handle_payments,
        "reports-json-csv": convert_json_to_csv
    }
    asyncio.create_task(consumer_service.register_consumers(topic_handlers)) 

@app.on_event("shutdown")
async def stop_consumers():
    """
    Gracefully stop consumers when FastAPI shuts down.
    """
    logger.info("🛑 Stopping Kafka consumers...")
    await consumer_service.stop_all()
