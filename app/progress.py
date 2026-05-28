from typing import Optional

from redis.asyncio import Redis

from app.config import settings
from app.models import AffectedPosition, ProgressState


def progress_key(task_id: str) -> str:
    return f"progress:{task_id}"


def affected_key(task_id: str) -> str:
    return f"affected:{task_id}"


def cancel_key(task_id: str) -> str:
    return f"cancel:{task_id}"


def confirm_key(task_id: str) -> str:
    return f"confirm:{task_id}"


async def set_progress(redis: Redis, task_id: str, state: ProgressState) -> None:
    await redis.set(progress_key(task_id), state.model_dump_json(), ex=settings.PROGRESS_TTL)


async def get_progress(redis: Redis, task_id: str) -> Optional[ProgressState]:
    raw = await redis.get(progress_key(task_id))
    if raw is None:
        return None
    return ProgressState.model_validate_json(raw)


async def set_affected(redis: Redis, task_id: str, items: list[AffectedPosition]) -> None:
    payload = "[" + ",".join(it.model_dump_json() for it in items) + "]"
    await redis.set(affected_key(task_id), payload, ex=settings.AFFECTED_TTL)


async def get_affected(redis: Redis, task_id: str) -> list[AffectedPosition]:
    raw = await redis.get(affected_key(task_id))
    if raw is None:
        return []
    import json

    data = json.loads(raw)
    return [AffectedPosition.model_validate(it) for it in data]


async def request_cancel(redis: Redis, task_id: str) -> None:
    await redis.set(cancel_key(task_id), "1", ex=settings.CANCEL_CONFIRM_TTL)


async def is_cancelled(redis: Redis, task_id: str) -> bool:
    return await redis.exists(cancel_key(task_id)) > 0


async def request_confirm(redis: Redis, task_id: str) -> None:
    await redis.set(confirm_key(task_id), "1", ex=settings.CANCEL_CONFIRM_TTL)


async def is_confirmed(redis: Redis, task_id: str) -> bool:
    return await redis.exists(confirm_key(task_id)) > 0


async def cleanup_process(redis: Redis, task_id: str) -> None:
    await redis.delete(
        progress_key(task_id),
        affected_key(task_id),
        cancel_key(task_id),
        confirm_key(task_id),
    )
