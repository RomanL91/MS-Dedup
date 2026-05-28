"""Главный экран: шапка с индикаторами, кнопки и список карточек процессов."""
import flet as ft

from app.core.config import settings
from app.ui.api_status import (
    make_put_indicator,
    make_rate_indicator,
    poll_api_status,
)
from app.ui.help_dialog import open_help_dialog
from app.ui.new_process_dialog import open_new_process_dialog
from app.ui.restore import restore_user_cards


async def show_main_screen(page: ft.Page) -> None:
    """Отрисовать главный экран и запустить фоновые задачи (статус API, восстановление карточек)."""
    processes_column = ft.Column(spacing=12)
    new_process_btn = ft.ElevatedButton("Новый процесс", icon=ft.Icons.ADD)

    def refresh_new_btn():
        active = page.session.get("active_processes") or {}
        new_process_btn.disabled = len(active) >= settings.MAX_PROCESSES
        new_process_btn.tooltip = (
            f"Максимум {settings.MAX_PROCESSES} активных процессов"
            if new_process_btn.disabled
            else None
        )

    async def on_new_process(_):
        await open_new_process_dialog(page, processes_column, refresh_new_btn)

    new_process_btn.on_click = on_new_process
    refresh_new_btn()

    help_btn = ft.OutlinedButton(
        "Инструкция",
        icon=ft.Icons.HELP_OUTLINE,
        on_click=lambda _: open_help_dialog(page),
    )

    # виджет статуса API
    rate_indicator = make_rate_indicator()
    put_indicator = make_put_indicator()
    wait_text = ft.Text("", size=11, color=ft.Colors.RED)

    status_polling_task = page.run_task(
        poll_api_status, page, rate_indicator, put_indicator, wait_text
    )

    async def on_logout(_):
        # отложенный импорт разрывает цикл login ⇄ main_screen
        from app.ui.login import show_login

        active = page.session.get("active_processes") or {}
        for info in list(active.values()):
            t = info.get("polling_task")
            if t and not t.done():
                t.cancel()
        if status_polling_task and not status_polling_task.done():
            status_polling_task.cancel()
        page.overlay.clear()
        page.session.remove("active_processes")
        page.session.remove("login")
        page.session.remove("password")
        page.session.set("active_processes", {})
        await show_login(page)

    header = ft.Row(
        [
            ft.Text("MS Dedup", size=22, weight=ft.FontWeight.BOLD),
            ft.Container(width=12),
            rate_indicator["row"],
            ft.Container(width=8),
            put_indicator["row"],
            ft.Container(width=8),
            wait_text,
            ft.Container(expand=True),
            ft.Text(f"Пользователь: {page.session.get('login')}", color=ft.Colors.GREY),
            ft.TextButton("Выйти", icon=ft.Icons.LOGOUT, on_click=on_logout),
        ],
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    page.controls.clear()
    page.add(
        header,
        ft.Divider(),
        ft.Row([new_process_btn, help_btn], spacing=8),
        ft.Container(height=10),
        processes_column,
    )
    page.update()

    # Восстановить карточки нетерминальных процессов в фоне — не блокируем
    # обработчик логина (иначе разрыв ws во время restore валит handler)
    page.run_task(restore_user_cards, page, processes_column, refresh_new_btn)
