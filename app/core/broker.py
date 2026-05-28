"""Брокер очереди задач taskiq.

Используется ``ListQueueBroker`` поверх Redis: UI кладёт задачу дедупликации в
очередь, отдельный воркер-процесс её разбирает и выполняет. Брокер вынесен в
отдельный модуль, чтобы и продюсер (UI), и консьюмер (воркер) импортировали
один и тот же экземпляр без циклических зависимостей.

Воркер запускается командой::

    taskiq worker app.core.broker:broker app.services.deduplication
"""
from taskiq_redis import ListQueueBroker

from app.core.config import settings

broker = ListQueueBroker(url=settings.REDIS_URL)
"""Единый экземпляр брокера для постановки и обработки задач."""
