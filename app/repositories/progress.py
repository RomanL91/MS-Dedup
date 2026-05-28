"""Репозиторий прогресса процесса, карты замен и флагов управления.

Объединяет четыре группы ключей одного ``task_id``:

* ``progress:{id}`` — сериализованный :class:`~app.domain.models.ProgressState`;
* ``affected:{id}`` — список :class:`~app.domain.models.AffectedPosition` (карта замен);
* ``cancel:{id}``  — флаг запроса отмены;
* ``confirm:{id}`` — флаг подтверждения замены пользователем.

Воркер пишет прогресс и читает флаги, UI читает прогресс/карту и выставляет флаги.
"""
import json
from typing import Optional

from redis.asyncio import Redis

from app.core.config import settings
from app.domain.models import AffectedPosition, ProgressState


class ProgressRepository:
    """Доступ к состоянию одного процесса дедупликации в Redis."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    # ---- имена ключей ----

    @staticmethod
    def progress_key(task_id: str) -> str:
        return f"progress:{task_id}"

    @staticmethod
    def affected_key(task_id: str) -> str:
        return f"affected:{task_id}"

    @staticmethod
    def cancel_key(task_id: str) -> str:
        return f"cancel:{task_id}"

    @staticmethod
    def confirm_key(task_id: str) -> str:
        return f"confirm:{task_id}"

    # ---- прогресс ----

    async def set(self, task_id: str, state: ProgressState) -> None:
        """Сохранить текущее состояние процесса (с TTL ``PROGRESS_TTL``)."""
        await self._redis.set(
            self.progress_key(task_id),
            state.model_dump_json(),
            ex=settings.PROGRESS_TTL,
        )

    async def get(self, task_id: str) -> Optional[ProgressState]:
        """Прочитать состояние процесса или ``None``, если его нет/истёк TTL."""
        raw = await self._redis.get(self.progress_key(task_id))
        if raw is None:
            return None
        return ProgressState.model_validate_json(raw)

    # ---- карта замен (affected) ----

    async def set_affected(
        self, task_id: str, items: list[AffectedPosition]
    ) -> None:
        """Сохранить список затронутых позиций (карту замен)."""
        payload = "[" + ",".join(it.model_dump_json() for it in items) + "]"
        await self._redis.set(
            self.affected_key(task_id), payload, ex=settings.AFFECTED_TTL
        )

    async def get_affected(self, task_id: str) -> list[AffectedPosition]:
        """Прочитать карту замен (пустой список, если данных нет)."""
        raw = await self._redis.get(self.affected_key(task_id))
        if raw is None:
            return []
        data = json.loads(raw)
        return [AffectedPosition.model_validate(it) for it in data]

    # ---- флаги отмены/подтверждения ----

    async def request_cancel(self, task_id: str) -> None:
        """Выставить флаг отмены процесса."""
        await self._redis.set(
            self.cancel_key(task_id), "1", ex=settings.CANCEL_CONFIRM_TTL
        )

    async def is_cancelled(self, task_id: str) -> bool:
        """Запрошена ли отмена процесса."""
        return await self._redis.exists(self.cancel_key(task_id)) > 0

    async def request_confirm(self, task_id: str) -> None:
        """Выставить флаг подтверждения замены."""
        await self._redis.set(
            self.confirm_key(task_id), "1", ex=settings.CANCEL_CONFIRM_TTL
        )

    async def is_confirmed(self, task_id: str) -> bool:
        """Подтвердил ли пользователь замену."""
        return await self._redis.exists(self.confirm_key(task_id)) > 0

    # ---- очистка ----

    async def cleanup(self, task_id: str) -> None:
        """Удалить все ключи процесса (прогресс, карта замен, флаги)."""
        await self._redis.delete(
            self.progress_key(task_id),
            self.affected_key(task_id),
            self.cancel_key(task_id),
            self.confirm_key(task_id),
        )
