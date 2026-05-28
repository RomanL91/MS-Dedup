"""Карточка процесса дедупликации и опрос её прогресса.

Карточка отображает параметры процесса, прогресс-бар, сообщения и кнопки
управления (Отмена / Подтвердить / Перезапустить / Убрать). Фоновая корутина
:func:`poll_progress` периодически читает состояние из Redis и обновляет вид
карточки, меняя её цвет и видимость кнопок в зависимости от статуса.
"""
import asyncio
import logging

import flet as ft

from app.core.redis import redis_session
from app.domain.entity_types import (
    ENTITY_TYPE_GROUP_NAMES,
    ENTITY_TYPE_TARGET_NAMES,
    ENTITY_TYPES,
)
from app.domain.enums import TERMINAL_STATUSES, ProcessStatus
from app.domain.models import ProgressState
from app.repositories.jobs import JobRepository
from app.repositories.liveness import LivenessRepository
from app.repositories.progress import ProgressRepository
from app.repositories.schedule import ScheduleRepository
from app.services.deduplication import run_deduplication
from app.ui.affected_view import build_affected_view
from app.ui.formatting import fmt_msk, ru_status

log = logging.getLogger(__name__)

# Статусы, при которых уместен прогресс-бар (что-то реально выполняется).
_ACTIVE_PROGRESS_STATUSES = {
    ProcessStatus.SCANNING,
    ProcessStatus.REPLACING,
    ProcessStatus.DELETING,
}


async def add_process_card(
    page: ft.Page,
    processes_column: ft.Column,
    refresh_new_btn,
    task_id: str,
    replace_ids: list[str],
    target_id: str,
    entity_type: str = "product",
    cleanup_mode: str = "archive",
    auto_confirm: bool = False,
    scheduled_at_ms: int | None = None,
) -> None:
    """Создать и добавить на экран карточку процесса, запустив опрос её прогресса."""
    active = page.session.get("active_processes") or {}
    process_number = sum(1 for _ in processes_column.controls) + 1

    entity_label = ENTITY_TYPES.get(entity_type, "Товар")
    entity_label_plural = ENTITY_TYPE_GROUP_NAMES.get(entity_type, "Товары")
    target_label_singular = ENTITY_TYPE_TARGET_NAMES.get(entity_type, "Целевой товар")
    cleanup_mode_label = {
        "archive": "архивировать",
        "delete": "удалить",
        "none": "ничего (оставить как есть)",
    }.get(cleanup_mode, "архивировать")
    if auto_confirm:
        cleanup_mode_label += " (без подтверждения)"

    title = ft.Text(
        f"Процесс #{process_number} — {entity_label}",
        size=16,
        weight=ft.FontWeight.BOLD,
    )
    replace_ids_display = ", ".join(replace_ids[:5]) + (
        f" и ещё {len(replace_ids) - 5}" if len(replace_ids) > 5 else ""
    )
    replace_ids_text = ft.Text(
        f"Заменить ({entity_label_plural}): {replace_ids_display}",
        size=12,
        selectable=True,
    )
    target_text = ft.Text(
        f"{target_label_singular}: {target_id}",
        size=12,
        selectable=True,
    )
    cleanup_text = ft.Text(
        f"Действие с дублями: {cleanup_mode_label}",
        size=12,
        color=ft.Colors.GREY,
    )
    schedule_text = ft.Text(
        (
            f"Запланирован на {fmt_msk(scheduled_at_ms)} (МСК)"
            if scheduled_at_ms is not None
            else ""
        ),
        size=12,
        color=ft.Colors.BLUE,
        visible=scheduled_at_ms is not None,
    )
    initial_status = (
        ProcessStatus.SCHEDULED
        if scheduled_at_ms is not None
        else ProcessStatus.PENDING
    )
    status_text = ft.Text(
        "Статус: " + ru_status(initial_status), size=14, weight=ft.FontWeight.W_500
    )
    progress_bar = ft.ProgressBar(width=500, value=0)
    counter_text = ft.Text("0/0", size=12)
    progress_row = ft.Row(
        [progress_bar, counter_text],
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
        visible=initial_status in _ACTIVE_PROGRESS_STATUSES,
    )
    message_text = ft.Text("", size=12, selectable=True)
    error_text = ft.Text("", color=ft.Colors.RED, size=12, selectable=True)

    confirm_btn = ft.ElevatedButton(
        "Подтвердить замену",
        icon=ft.Icons.CHECK_CIRCLE,
        visible=False,
    )
    cancel_btn = ft.OutlinedButton("Отмена", icon=ft.Icons.CANCEL)
    restart_btn = ft.ElevatedButton(
        "Перезапустить сначала",
        icon=ft.Icons.RESTART_ALT,
        visible=False,
    )
    dismiss_btn = ft.TextButton(
        "Убрать",
        icon=ft.Icons.CLOSE,
        visible=False,
    )

    affected_view = ft.Container(visible=False, padding=ft.padding.only(top=8))

    card_container = ft.Container(
        content=ft.Column(
            [
                ft.Row([title, ft.Container(expand=True), status_text]),
                replace_ids_text,
                target_text,
                cleanup_text,
                schedule_text,
                progress_row,
                message_text,
                error_text,
                affected_view,
                ft.Row([confirm_btn, cancel_btn, restart_btn, dismiss_btn]),
            ],
            spacing=8,
        ),
        padding=15,
        border_radius=10,
        border=ft.border.all(1, ft.Colors.GREY_300),
        bgcolor=ft.Colors.WHITE,
    )

    async def on_cancel(_):
        cancel_btn.disabled = True
        cancel_btn.text = "Отменяется..."
        # пока идёт отмена — Подтвердить не должна оставаться активной
        confirm_btn.disabled = True
        page.update()
        async with redis_session() as redis:
            progress = ProgressRepository(redis)
            schedule = ScheduleRepository(redis)
            await progress.request_cancel(task_id)
            # если задача ещё не стартовала — снимаем из очереди шедулера
            # сразу и помечаем CANCELLED, чтобы UI получил терминальный статус
            scheduled_score = await schedule.scheduled_at_ms(task_id)
            if scheduled_score is not None:
                await schedule.remove(task_id)
                await progress.set(
                    task_id,
                    ProgressState(
                        status=ProcessStatus.CANCELLED,
                        message="Отменено до старта",
                    ),
                )
                return
            # Форсируем терминальный CANCELLED прямо из UI для любого ещё
            # незавершённого состояния. Иначе при отмене «осиротевшей» карточки
            # (например после полного перезапуска сервиса — немедленная задача не
            # переподхватывается воркером, либо воркер ждёт в CONFIRMING) флаг
            # cancel некому обработать, и карточка зависает в «Отменяется...».
            # Если воркер жив, он тоже увидит флаг и сойдётся к CANCELLED —
            # расхождение самоустранится (воркер перезапишет статус со счётчиками).
            try:
                cur = await progress.get(task_id)
            except Exception:  # noqa: BLE001
                cur = None
            if cur is None or cur.status not in TERMINAL_STATUSES:
                await progress.set(
                    task_id,
                    ProgressState(
                        status=ProcessStatus.CANCELLED,
                        message="Отменено пользователем",
                        replaced_count=cur.replaced_count if cur else 0,
                        deleted_count=cur.deleted_count if cur else 0,
                        delete_errors=cur.delete_errors if cur else [],
                        skipped_ids=cur.skipped_ids if cur else [],
                    ),
                )

    async def on_confirm(_):
        confirm_btn.disabled = True
        confirm_btn.text = "Подтверждено..."
        # запретим отмену пока запускаем замену (терминал придёт от воркера)
        cancel_btn.disabled = True
        page.update()
        async with redis_session() as redis:
            await ProgressRepository(redis).request_confirm(task_id)

    async def on_restart(_):
        # Перезапуск осиротевшего процесса — с нуля (повторный скан найдёт только
        # оставшиеся дубли, уже заменённые позиции не находятся).
        restart_btn.disabled = True
        restart_btn.text = "Перезапуск..."
        dismiss_btn.disabled = True
        page.update()
        login = page.session.get("login")
        password = page.session.get("password")
        async with redis_session() as redis:
            progress = ProgressRepository(redis)
            try:
                # сбрасываем старое состояние (в т.ч. возможные cancel/confirm) и
                # заново ставим задачу в очередь с тем же task_id
                await progress.cleanup(task_id)
                await progress.set(
                    task_id,
                    ProgressState(
                        status=ProcessStatus.PENDING, message="Перезапуск..."
                    ),
                )
                await run_deduplication.kiq(
                    task_id,
                    replace_ids,
                    target_id,
                    login,
                    password,
                    entity_type,
                    cleanup_mode,
                    auto_confirm,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("restart failed for %s: %s", task_id, e)
                error_text.value = f"Не удалось перезапустить: {e}"
                restart_btn.disabled = False
                restart_btn.text = "Перезапустить сначала"
                dismiss_btn.disabled = False
        try:
            page.update()
        except Exception:  # noqa: BLE001
            pass

    async def on_dismiss(_):
        login = page.session.get("login")
        async with redis_session() as redis:
            try:
                if login:
                    await JobRepository(redis).forget_user_job(task_id, login)
            except Exception:  # noqa: BLE001
                pass
            try:
                await ProgressRepository(redis).cleanup(task_id)
            except Exception:  # noqa: BLE001
                pass
            try:
                await LivenessRepository(redis).clear(task_id)
            except Exception:  # noqa: BLE001
                pass
        active = page.session.get("active_processes") or {}
        info = active.pop(task_id, None)
        page.session.set("active_processes", active)
        if info:
            t = info.get("polling_task")
            if t and not t.done():
                t.cancel()
        try:
            processes_column.controls.remove(card_container)
        except ValueError:
            pass
        refresh_new_btn()
        page.update()

    cancel_btn.on_click = on_cancel
    confirm_btn.on_click = on_confirm
    restart_btn.on_click = on_restart
    dismiss_btn.on_click = on_dismiss

    processes_column.controls.append(card_container)

    polling_task = page.run_task(
        poll_progress,
        page,
        task_id,
        status_text,
        progress_bar,
        counter_text,
        progress_row,
        message_text,
        error_text,
        confirm_btn,
        cancel_btn,
        restart_btn,
        dismiss_btn,
        card_container,
        affected_view,
        refresh_new_btn,
    )

    active[task_id] = {"card": card_container, "polling_task": polling_task}
    page.session.set("active_processes", active)
    refresh_new_btn()
    page.update()


async def poll_progress(
    page: ft.Page,
    task_id: str,
    status_text: ft.Text,
    progress_bar: ft.ProgressBar,
    counter_text: ft.Text,
    progress_row: ft.Row,
    message_text: ft.Text,
    error_text: ft.Text,
    confirm_btn: ft.ElevatedButton,
    cancel_btn: ft.OutlinedButton,
    restart_btn: ft.ElevatedButton,
    dismiss_btn: ft.TextButton,
    card_container: ft.Container,
    affected_view: ft.Container,
    refresh_new_btn,
) -> None:
    """Фоновая корутина: читает прогресс из Redis и обновляет карточку, пока процесс не завершён."""
    affected_rendered = False
    async with redis_session() as redis:
        progress = ProgressRepository(redis)
        try:
            while True:
                try:
                    state: ProgressState | None = await progress.get(task_id)
                except Exception as e:  # noqa: BLE001
                    log.warning("Polling error for %s: %s", task_id, e)
                    state = None

                if state is not None:
                    status_text.value = "Статус: " + ru_status(state.status)
                    if state.total > 0:
                        progress_bar.value = min(1.0, state.current / state.total)
                    else:
                        progress_bar.value = None
                    counter_text.value = (
                        f"{state.current}/{state.total}"
                        if state.total
                        else f"{state.current}"
                    )
                    message_text.value = state.message or ""
                    error_text.value = state.error or ""

                    # прогресс-бар активен только когда что-то реально идёт
                    progress_row.visible = state.status in _ACTIVE_PROGRESS_STATUSES

                    confirm_btn.visible = state.status == ProcessStatus.CONFIRMING

                    # Осиротевший процесс: показываем «Перезапустить»/«Убрать»,
                    # прячем «Отмена»/«Подтвердить», карточка оранжевая. После рестарта
                    # статус сменится на PENDING/SCANNING и вид вернётся к обычному.
                    is_interrupted = state.status == ProcessStatus.INTERRUPTED
                    restart_btn.visible = is_interrupted
                    dismiss_btn.visible = is_interrupted
                    if is_interrupted:
                        confirm_btn.visible = False
                        cancel_btn.visible = False
                        card_container.bgcolor = ft.Colors.ORANGE_50
                        card_container.border = ft.border.all(1, ft.Colors.ORANGE_400)
                    elif state.status not in TERMINAL_STATUSES:
                        cancel_btn.visible = True
                        card_container.bgcolor = ft.Colors.WHITE
                        card_container.border = ft.border.all(1, ft.Colors.GREY_300)

                    # рендерим карту замен либо когда ждём подтверждения, либо когда
                    # уже начали замену без подтверждения (auto_confirm)
                    if (
                        state.status in (ProcessStatus.CONFIRMING, ProcessStatus.REPLACING)
                        and not affected_rendered
                    ):
                        try:
                            affected = await progress.get_affected(task_id)
                        except Exception as e:  # noqa: BLE001
                            log.warning("Failed to load affected for %s: %s", task_id, e)
                            affected = []
                        if affected:
                            affected_view.content = build_affected_view(page, affected)
                            affected_view.visible = True
                            affected_rendered = True

                    if state.status in TERMINAL_STATUSES:
                        cancel_btn.visible = False
                        confirm_btn.visible = False
                        restart_btn.visible = False
                        dismiss_btn.visible = False
                        if state.status == ProcessStatus.DONE:
                            card_container.bgcolor = ft.Colors.GREEN_50
                            card_container.border = ft.border.all(1, ft.Colors.GREEN_400)
                        elif state.status == ProcessStatus.ERROR:
                            card_container.bgcolor = ft.Colors.RED_50
                            card_container.border = ft.border.all(1, ft.Colors.RED_400)
                        elif state.status == ProcessStatus.CANCELLED:
                            card_container.bgcolor = ft.Colors.GREY_100
                            card_container.border = ft.border.all(1, ft.Colors.GREY_500)

                        if state.delete_errors:
                            msgs = "\n".join(state.delete_errors)
                            error_text.value = (
                                (error_text.value or "") + "\n" + msgs
                            ).strip()

                        if state.skipped_ids:
                            skipped_note = (
                                "Замена не прошла полностью — не тронуты при очистке:\n"
                                + ", ".join(state.skipped_ids)
                            )
                            error_text.value = (
                                (error_text.value or "") + "\n" + skipped_note
                            ).strip()

                        page.update()

                        active = page.session.get("active_processes") or {}
                        if task_id in active:
                            active.pop(task_id, None)
                            page.session.set("active_processes", active)
                            refresh_new_btn()
                            page.update()
                        return

                    page.update()

                await asyncio.sleep(2)
        except asyncio.CancelledError:
            return
