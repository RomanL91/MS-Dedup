"""Репозиторий heartbeat'ов живых процессов.

Пока задача выполняется, воркер периодически продлевает ключ ``alive:{task_id}``
с коротким TTL. Если воркер умирает (перезапуск сервиса), ключ протухает — так
UI понимает, что процесс осиротел, и предлагает перезапуск. Число живых ключей
служит счётчиком активных процессов для шедулера.
"""
from redis.asyncio import Redis

from app.core.config import settings


def alive_key(task_id: str) -> str:
    """Имя ключа heartbeat для задачи."""
    return f"alive:{task_id}"


class LivenessRepository:
    """Доступ к heartbeat'ам и счётчику активных процессов."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def touch(self, task_id: str) -> None:
        """Продлить heartbeat живого процесса (TTL = ``HEARTBEAT_TTL``)."""
        await self._redis.set(alive_key(task_id), "1", ex=settings.HEARTBEAT_TTL)

    async def is_alive(self, task_id: str) -> bool:
        """Жив ли воркер процесса (heartbeat ещё не протух)."""
        return await self._redis.exists(alive_key(task_id)) > 0

    async def clear(self, task_id: str) -> None:
        """Удалить heartbeat-ключ процесса."""
        await self._redis.delete(alive_key(task_id))

    async def active_count(self) -> int:
        """Сколько процессов реально работает сейчас — по живым heartbeat-ключам.

        Heartbeat сам протухает при смерти воркера, поэтому, в отличие от прежнего
        набора ``active:tasks``, осиротевшие задачи не залипают и не занимают слоты.
        """
        count = 0
        async for _ in self._redis.scan_iter(f"{alive_key('')}*", count=100):
            count += 1
        return count
