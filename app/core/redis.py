"""Фабрика и контекстный менеджер подключений к Redis.

Раньше каждый обработчик UI и каждая задача создавали клиент Redis вручную
(``Redis.from_url(settings.REDIS_URL, decode_responses=True)``) и повторяли
один и тот же громоздкий ``try/finally`` с подавлением ``GeneratorExit`` при
закрытии. Этот модуль убирает дублирование: вся логика создания и безопасного
закрытия соединения собрана в одном месте.
"""
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from redis.asyncio import Redis

from app.core.config import settings

log = logging.getLogger(__name__)


def make_redis() -> Redis:
    """Создать новый асинхронный клиент Redis с декодированием ответов в ``str``.

    Вызывающий код обязан сам закрыть соединение (``await redis.aclose()``)
    либо использовать :func:`redis_session`, который сделает это автоматически.
    """
    return Redis.from_url(settings.REDIS_URL, decode_responses=True)


@asynccontextmanager
async def redis_session() -> AsyncIterator[Redis]:
    """Контекстный менеджер: выдаёт клиент Redis и гарантированно его закрывает.

    Закрытие защищено от ``GeneratorExit``: при отмене корутины (например, когда
    Flet рвёт websocket во время обработки) ``await`` в ``finally`` может
    спровоцировать предупреждение «coroutine ignored GeneratorExit». Мы
    пробрасываем ``GeneratorExit`` дальше, а прочие ошибки закрытия — глушим в лог.

    Пример::

        async with redis_session() as redis:
            await ProgressRepository(redis).get(task_id)
    """
    redis = make_redis()
    try:
        yield redis
    finally:
        try:
            await redis.aclose()
        except GeneratorExit:
            raise
        except BaseException as e:  # noqa: BLE001
            log.debug("redis_session: aclose() suppressed: %r", e)
