"""Восстановление карточек процессов после релогина или перезапуска сервиса.

Подтягивает из Redis запланированные и активные процессы текущего пользователя и
отрисовывает их карточки. Терминальные процессы пропускаются. Осиротевшие
(воркер умер, heartbeat протух, и задача не ждёт в очереди шедулера) помечаются
статусом INTERRUPTED, чтобы карточка предложила перезапуск.
"""
import asyncio
import logging

import flet as ft

from app.core.redis import redis_session
from app.domain.enums import TERMINAL_STATUSES, ProcessStatus
from app.domain.models import JobMeta, ProgressState
from app.repositories.jobs import JobRepository
from app.repositories.liveness import LivenessRepository
from app.repositories.progress import ProgressRepository
from app.repositories.schedule import ScheduleRepository
from app.ui.process_card import add_process_card

log = logging.getLogger(__name__)


async def restore_user_cards(
    page: ft.Page,
    processes_column: ft.Column,
    refresh_new_btn,
) -> None:
    """Восстановить карточки незавершённых процессов пользователя из Redis."""
    login = page.session.get("login")
    if not login:
        return
    async with redis_session() as redis:
        jobs = JobRepository(redis)
        progress = ProgressRepository(redis)
        schedule = ScheduleRepository(redis)
        liveness = LivenessRepository(redis)
        try:
            try:
                task_ids = await jobs.list_user_jobs(login)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("restore: list_user_jobs failed: %s", e)
                return
            if not task_ids:
                return
            log.info("restore: %d job(s) for %s", len(task_ids), login)
            # сортируем по scheduled_at_ms (если есть) для предсказуемого порядка
            metas: list[JobMeta] = []
            for tid in task_ids:
                try:
                    meta = await jobs.get_meta(tid)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001
                    log.warning("restore: meta load failed for %s: %s", tid, e)
                    meta = None
                if meta is None:
                    # meta истёк по TTL — чистим индекс
                    try:
                        await jobs.forget_user_job(tid, login)
                    except Exception:  # noqa: BLE001
                        pass
                    continue
                # пропустить терминальные процессы
                try:
                    state = await progress.get(tid)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    state = None
                if state is not None and state.status in TERMINAL_STATUSES:
                    continue
                # Осиротевший процесс: не завершён, НЕ ждёт в очереди шедулера и
                # heartbeat протух (воркер умер при перезапуске, immediate-задача не
                # переподхватывается). Помечаем INTERRUPTED, чтобы карточка предложила
                # перезапуск, а не висела как «Ожидание» с одной «Отменой».
                # Запланированные задачи (в очереди шедулера) не трогаем — их запустит
                # шедулер; их heartbeat и должен отсутствовать.
                status = state.status if state is not None else ProcessStatus.PENDING
                if status != ProcessStatus.SCHEDULED:
                    try:
                        sched = await schedule.scheduled_at_ms(tid)
                    except Exception:  # noqa: BLE001
                        sched = None
                    if sched is None:
                        try:
                            alive = await liveness.is_alive(tid)
                        except Exception:  # noqa: BLE001
                            alive = False
                        if not alive:
                            try:
                                await progress.set(
                                    tid,
                                    ProgressState(
                                        status=ProcessStatus.INTERRUPTED,
                                        message="Прервано перезапуском сервиса",
                                        replaced_count=(
                                            state.replaced_count if state is not None else 0
                                        ),
                                        deleted_count=(
                                            state.deleted_count if state is not None else 0
                                        ),
                                    ),
                                )
                            except Exception:  # noqa: BLE001
                                pass
                metas.append(meta)

            metas.sort(key=lambda m: m.scheduled_at_ms or 0)
            for meta in metas:
                try:
                    await add_process_card(
                        page,
                        processes_column,
                        refresh_new_btn,
                        meta.task_id,
                        meta.replace_ids,
                        meta.target_id,
                        meta.entity_type,
                        meta.cleanup_mode,
                        meta.auto_confirm,
                        meta.scheduled_at_ms,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001
                    log.warning("restore: add card failed for %s: %s", meta.task_id, e)
        except asyncio.CancelledError:
            log.info("restore: cancelled")
            raise
