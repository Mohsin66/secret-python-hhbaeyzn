import redis
import logging
from typing import Dict, Any

class RedisAdapter:
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.client = None

    def __new__(cls):
        """
        Implement Singleton pattern to ensure only one Kafka client instance.
        """
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    async def connect(self) -> redis.Redis:
        """
        Initialize and connect to Redis server
        Returns:
            redis.Redis: Redis client instance
        """
        try:
            redis_config = self.config.get('cache', {}).get('redis', {})
            
            self.client = redis.Redis(
                host=redis_config.get('host'),
                port=redis_config.get('port'),
                decode_responses=True
            )

            # Test connection
            self.client.ping()
            self.logger.info('REDIS_EVENT [ready]')
            
            return self.client

        except redis.ConnectionError as e:
            self.logger.error(f'REDIS_EVENT [error] {str(e)}')
            raise
        except Exception as e:
            self.logger.error(f'REDIS_EVENT [error] {str(e)}')
            raise

    async def disconnect(self):
        """
        Close Redis connection
        """
        if self.client:
            self.client.close()
            self.logger.info('REDIS_EVENT [disconnect]')

    def get_client(self) -> redis.Redis:
        """
        Get Redis client instance
        Returns:
            redis.Redis: Redis client instance
        """
        if not self.client:
            raise redis.ConnectionError("Redis client not initialized")
        return self.client