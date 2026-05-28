"""Репозиторий очереди отложенных запусков.

Очередь — это Redis sorted set ``sched:queue``, где score задачи равен времени
запуска (epoch_ms). Полезная нагрузка запуска (включая пароль) хранится отдельно
по ключу ``sched:job:{task_id}`` с ограниченным TTL. Шедулер периодически
выбирает «созревшие» задачи и запускает их.
"""
from redis.asyncio import Redis

from app.core.config import settings
from app.domain.models import ScheduledJob

QUEUE_KEY = "sched:queue"


class ScheduleRepository:
    """Доступ к очереди отложенных запусков в Redis."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    @staticmethod
    def sched_job_key(task_id: str) -> str:
        return f"sched:job:{task_id}"

    async def enqueue(self, job: ScheduledJob) -> None:
        """Поставить отложенный запуск в очередь.

        Сохраняет полезную нагрузку и добавляет задачу в sorted set со score,
        равным времени запуска.
        """
        await self._redis.set(
            self.sched_job_key(job.task_id),
            job.model_dump_json(),
            ex=settings.SCHEDULED_JOB_TTL,
        )
        await self._redis.zadd(QUEUE_KEY, {job.task_id: job.scheduled_at_ms})

    async def remove(self, task_id: str) -> None:
        """Снять задачу с очереди и удалить её полезную нагрузку."""
        await self._redis.zrem(QUEUE_KEY, task_id)
        await self._redis.delete(self.sched_job_key(task_id))

    async def scheduled_at_ms(self, task_id: str) -> int | None:
        """Время запланированного запуска задачи (epoch_ms) или ``None``."""
        score = await self._redis.zscore(QUEUE_KEY, task_id)
        return int(score) if score is not None else None

    async def due_task_ids(self, now_ms: int) -> list[str]:
        """Идентификаторы задач, у которых наступило время запуска (score ≤ now)."""
        return list(await self._redis.zrangebyscore(QUEUE_KEY, 0, now_ms))

    async def get_payload(self, task_id: str) -> ScheduledJob | None:
        """Прочитать полезную нагрузку отложенного запуска или ``None``."""
        raw = await self._redis.get(self.sched_job_key(task_id))
        if raw is None:
            return None
        return ScheduledJob.model_validate_json(raw)

    async def drop_from_queue(self, task_id: str) -> None:
        """Убрать задачу только из sorted set (когда полезная нагрузка уже потеряна)."""
        await self._redis.zrem(QUEUE_KEY, task_id)
