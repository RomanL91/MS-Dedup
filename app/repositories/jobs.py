"""Репозиторий метаданных задач и индекса задач пользователя.

Хранит публичные метаданные карточки (:class:`~app.domain.models.JobMeta`, без
пароля) по ключу ``job:meta:{task_id}`` и множество задач каждого пользователя
по ключу ``user:jobs:{login}``. Используется при восстановлении карточек после
релогина.
"""
from redis.asyncio import Redis

from app.core.config import settings
from app.domain.models import JobMeta


class JobRepository:
    """Доступ к метаданным задач дедупликации в Redis."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    @staticmethod
    def job_meta_key(task_id: str) -> str:
        return f"job:meta:{task_id}"

    @staticmethod
    def user_jobs_key(login: str) -> str:
        return f"user:jobs:{login}"

    async def save_meta(self, meta: JobMeta) -> None:
        """Сохранить метаданные карточки и добавить задачу в индекс пользователя.

        Метаданные не содержат пароля — только публичные поля для отрисовки карточки.
        """
        await self._redis.set(
            self.job_meta_key(meta.task_id),
            meta.model_dump_json(),
            ex=settings.JOB_META_TTL,
        )
        await self._redis.sadd(self.user_jobs_key(meta.login), meta.task_id)

    async def get_meta(self, task_id: str) -> JobMeta | None:
        """Прочитать метаданные задачи или ``None``, если их нет/истёк TTL."""
        raw = await self._redis.get(self.job_meta_key(task_id))
        if raw is None:
            return None
        return JobMeta.model_validate_json(raw)

    async def list_user_jobs(self, login: str) -> list[str]:
        """Список идентификаторов задач пользователя."""
        members = await self._redis.smembers(self.user_jobs_key(login))
        return list(members)

    async def forget_user_job(self, task_id: str, login: str) -> None:
        """Удалить метаданные карточки и убрать задачу из индекса пользователя."""
        await self._redis.delete(self.job_meta_key(task_id))
        await self._redis.srem(self.user_jobs_key(login), task_id)
