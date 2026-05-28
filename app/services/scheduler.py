"""Фоновый шедулер отложенных запусков.

Раз в ``SCHEDULER_INTERVAL_SEC`` секунд проверяет очередь и запускает задачи, у
которых наступило время старта. Соблюдает лимит ``MAX_PROCESSES``: если столько
процессов уже работает, запуск откладывается до следующего тика. Перед запуском
учитывает возможную отмену задачи пользователем.
"""
import asyncio
import logging
import time

from app.core.config import settings
from app.core.redis import make_redis
from app.domain.enums import ProcessStatus
from app.domain.models import ProgressState
from app.repositories.liveness import LivenessRepository
from app.repositories.progress import ProgressRepository
from app.repositories.schedule import ScheduleRepository

log = logging.getLogger(__name__)


async def scheduler_loop(redis_url: str) -> None:
    """Бесконечный цикл запуска запланированных задач.

    Параметр ``redis_url`` сохранён для совместимости с прежней сигнатурой; само
    подключение создаётся через общую фабрику :func:`app.core.redis.make_redis`.
    """
    # Поздний импорт — services.deduplication импортирует репозитории отсюда нет,
    # но импорт задачи откладываем, чтобы избежать циклов на import time.
    from app.services.deduplication import run_deduplication

    redis = make_redis()
    schedule = ScheduleRepository(redis)
    liveness = LivenessRepository(redis)
    progress = ProgressRepository(redis)
    log.info(
        "Scheduler loop started (interval=%ss, max_processes=%s)",
        settings.SCHEDULER_INTERVAL_SEC,
        settings.MAX_PROCESSES,
    )
    try:
        while True:
            try:
                now_ms = int(time.time() * 1000)
                due = await schedule.due_task_ids(now_ms)
                for task_id in due:
                    free_slots = settings.MAX_PROCESSES - await liveness.active_count()
                    if free_slots <= 0:
                        break  # лимит исчерпан — ждём следующего тика
                    job = await schedule.get_payload(task_id)
                    if job is None:
                        # данные потеряны — снимаем с очереди
                        await schedule.drop_from_queue(task_id)
                        continue
                    # пользователь успел отменить до старта
                    if await progress.is_cancelled(task_id):
                        await schedule.remove(task_id)
                        await progress.set(
                            task_id,
                            ProgressState(
                                status=ProcessStatus.CANCELLED,
                                message="Отменено до старта",
                            ),
                        )
                        continue
                    try:
                        await run_deduplication.kiq(
                            job.task_id,
                            job.replace_ids,
                            job.target_id,
                            job.login,
                            job.password,
                            job.entity_type,
                            job.cleanup_mode,
                            job.auto_confirm,
                        )
                        await schedule.remove(task_id)
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
