"""Отложенный запуск процессов дедупликации.

Шедулер хранит очередь в Redis (sorted set) и раз в N секунд проверяет
задачи у которых наступило время запуска. Если есть свободные слоты
(MAX_PROCESSES - active_count > 0) — запускает их через taskiq.
Иначе откладывает до следующего тика.

Также модуль содержит вспомогательные функции для:
- регистрации активных задач (глобальный счётчик для шедулера)
- сохранения метаданных карточки процесса (для восстановления после релогина)
"""
import asyncio
import json
import logging
import time
from typing import Any

from redis.asyncio import Redis

from app.config import settings
from app.models import JobMeta, ProcessStatus, ProgressState
from app.progress import is_cancelled, set_progress

log = logging.getLogger(__name__)

# ---------------- Redis keys ----------------

QUEUE_KEY = "sched:queue"


def sched_job_key(task_id: str) -> str:
    return f"sched:job:{task_id}"


def alive_key(task_id: str) -> str:
    return f"alive:{task_id}"


def job_meta_key(task_id: str) -> str:
    return f"job:meta:{task_id}"


def user_jobs_key(login: str) -> str:
    return f"user:jobs:{login}"


# ---------------- Job metadata (для карточек, без пароля) ----------------


async def save_job_meta(redis: Redis, meta: JobMeta) -> None:
    """Сохранить метаданные карточки. Без пароля — только публичные поля."""
    await redis.set(
        job_meta_key(meta.task_id),
        meta.model_dump_json(),
        ex=settings.JOB_META_TTL,
    )
    await redis.sadd(user_jobs_key(meta.login), meta.task_id)


async def get_job_meta(redis: Redis, task_id: str) -> JobMeta | None:
    raw = await redis.get(job_meta_key(task_id))
    if raw is None:
        return None
    return JobMeta.model_validate_json(raw)


async def list_user_jobs(redis: Redis, login: str) -> list[str]:
    members = await redis.smembers(user_jobs_key(login))
    return list(members)


async def forget_user_job(redis: Redis, task_id: str, login: str) -> None:
    """Удалить метаданные карточки и убрать task_id из индекса пользователя."""
    await redis.delete(job_meta_key(task_id))
    await redis.srem(user_jobs_key(login), task_id)


# ---------------- Очередь запланированных запусков (с паролем) ----------------


def _scheduled_payload(
    *,
    task_id: str,
    login: str,
    password: str,
    replace_ids: list[str],
    target_id: str,
    entity_type: str,
    cleanup_mode: str,
    auto_confirm: bool,
    scheduled_at_ms: int,
    target_type: str = "",
    replace_types: dict[str, str] | None = None,
) -> str:
    return json.dumps(
        {
            "task_id": task_id,
            "login": login,
            "password": password,
            "replace_ids": replace_ids,
            "target_id": target_id,
            "entity_type": entity_type,
            "target_type": target_type,
            "replace_types": replace_types or {},
            "cleanup_mode": cleanup_mode,
            "auto_confirm": auto_confirm,
            "scheduled_at_ms": scheduled_at_ms,
        }
    )


async def enqueue_scheduled(
    redis: Redis,
    *,
    task_id: str,
    login: str,
    password: str,
    replace_ids: list[str],
    target_id: str,
    entity_type: str,
    cleanup_mode: str,
    auto_confirm: bool,
    scheduled_at_ms: int,
    target_type: str = "",
    replace_types: dict[str, str] | None = None,
) -> None:
    payload = _scheduled_payload(
        task_id=task_id,
        login=login,
        password=password,
        replace_ids=replace_ids,
        target_id=target_id,
        entity_type=entity_type,
        target_type=target_type,
        replace_types=replace_types or {},
        cleanup_mode=cleanup_mode,
        auto_confirm=auto_confirm,
        scheduled_at_ms=scheduled_at_ms,
    )
    await redis.set(sched_job_key(task_id), payload, ex=settings.SCHEDULED_JOB_TTL)
    await redis.zadd(QUEUE_KEY, {task_id: scheduled_at_ms})


async def remove_scheduled(redis: Redis, task_id: str) -> None:
    await redis.zrem(QUEUE_KEY, task_id)
    await redis.delete(sched_job_key(task_id))


async def get_scheduled_at_ms(redis: Redis, task_id: str) -> int | None:
    score = await redis.zscore(QUEUE_KEY, task_id)
    return int(score) if score is not None else None


async def _due_task_ids(redis: Redis, now_ms: int) -> list[str]:
    return list(await redis.zrangebyscore(QUEUE_KEY, 0, now_ms))


async def _get_scheduled_payload(redis: Redis, task_id: str) -> dict[str, Any] | None:
    raw = await redis.get(sched_job_key(task_id))
    if raw is None:
        return None
    return json.loads(raw)


# ---------------- Liveness (heartbeat) и счётчик активных задач ----------------


async def touch_alive(redis: Redis, task_id: str) -> None:
    """Обновить heartbeat живого процесса (TTL = HEARTBEAT_TTL)."""
    await redis.set(alive_key(task_id), "1", ex=settings.HEARTBEAT_TTL)


async def is_alive(redis: Redis, task_id: str) -> bool:
    """Жив ли воркер процесса (heartbeat ещё не протух)."""
    return await redis.exists(alive_key(task_id)) > 0


async def active_count(redis: Redis) -> int:
    """Сколько процессов реально работает сейчас — по живым heartbeat-ключам.

    Heartbeat сам протухает при смерти воркера, поэтому, в отличие от прежнего
    set active:tasks, осиротевшие задачи не залипают и не занимают слоты.
    """
    count = 0
    async for _ in redis.scan_iter(f"{alive_key('')}*", count=100):
        count += 1
    return count


# ---------------- Шедулер ----------------


async def scheduler_loop(redis_url: str) -> None:
    """Фоновая корутина: запускает запланированные задачи когда наступило время.

    Соблюдает MAX_PROCESSES — если в данный момент работают MAX_PROCESSES задач,
    запуск откладывается до следующего тика.
    """
    # Поздний импорт — tasks.py импортирует helpers отсюда (touch_alive),
    # поэтому импортируем run_deduplication здесь, чтобы избежать цикла на import time.
    from app.tasks import run_deduplication

    redis = Redis.from_url(redis_url, decode_responses=True)
    log.info(
        "Scheduler loop started (interval=%ss, max_processes=%s)",
        settings.SCHEDULER_INTERVAL_SEC,
        settings.MAX_PROCESSES,
    )
    try:
        while True:
            try:
                now_ms = int(time.time() * 1000)
                due = await _due_task_ids(redis, now_ms)
                for task_id in due:
                    free_slots = settings.MAX_PROCESSES - await active_count(redis)
                    if free_slots <= 0:
                        break  # лимит исчерпан — ждём следующего тика
                    payload = await _get_scheduled_payload(redis, task_id)
                    if payload is None:
                        # данные потеряны — снимаем с очереди
                        await redis.zrem(QUEUE_KEY, task_id)
                        continue
                    # пользователь успел отменить до старта
                    if await is_cancelled(redis, task_id):
                        await remove_scheduled(redis, task_id)
                        await set_progress(
                            redis,
                            task_id,
                            ProgressState(
                                status=ProcessStatus.CANCELLED,
                                message="Отменено до старта",
                            ),
                        )
                        continue
                    try:
                        await run_deduplication.kiq(
                            payload["task_id"],
                            payload["replace_ids"],
                            payload["target_id"],
                            payload["login"],
                            payload["password"],
                            payload.get("entity_type", "product"),
                            payload.get("cleanup_mode", "archive"),
                            payload.get("auto_confirm", False),
                            payload.get("target_type") or None,
                            payload.get("replace_types") or None,
                        )
                        await remove_scheduled(redis, task_id)
                        log.info("Scheduler launched task %s", task_id)
                    except Exception as e:  # noqa: BLE001
                        log.warning(
                            "Failed to launch scheduled task %s: %s — will retry",
                            task_id,
                            e,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.exception("Scheduler loop iteration error: %s", e)

            await asyncio.sleep(settings.SCHEDULER_INTERVAL_SEC)
    except asyncio.CancelledError:
        log.info("Scheduler loop cancelled")
    finally:
        await redis.aclose()
