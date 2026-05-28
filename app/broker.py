from taskiq_redis import ListQueueBroker

from app.config import settings

broker = ListQueueBroker(url=settings.REDIS_URL)
