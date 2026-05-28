"""Точка входа веб-приложения Flet.

Инициализирует страницу, проверяет доступность Redis, поднимает брокер задач и
фоновый шедулер, после чего показывает экран входа. Функция :func:`run`
запускает Flet-сервер (используется модулем ``app.main``).
"""
import asyncio
import logging

import flet as ft
from redis.asyncio import Redis

from app.core.broker import broker
from app.core.config import settings
from app.services.scheduler import scheduler_loop
from app.ui.login import show_login

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger(__name__)


_broker_started_lock = asyncio.Lock()
_broker_started = False
_scheduler_task: asyncio.Task | None = None


async def _ensure_broker_started() -> None:
    """Один раз поднять брокер taskiq и фоновый цикл шедулера.

    Защищено блокировкой, чтобы параллельные подключения не запускали брокер
    дважды; цикл шедулера перезапускается, если предыдущая задача завершилась.
    """
    global _broker_started, _scheduler_task
    async with _broker_started_lock:
        if not _broker_started:
            await broker.startup()
            _broker_started = True
        if _scheduler_task is None or _scheduler_task.done():
            _scheduler_task = asyncio.create_task(
                scheduler_loop(settings.REDIS_URL),
                name="dedup-scheduler-loop",
            )


async def _check_redis() -> bool:
    """Проверить доступность Redis (ping) перед запуском UI."""
    try:
        r = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        await r.ping()
        await r.aclose()
        return True
    except Exception as e:  # noqa: BLE001
        log.error("Redis is not available: %s", e)
        return False


async def main(page: ft.Page) -> None:
    """Обработчик новой страницы Flet: настройка, проверки и показ экрана входа."""
    page.title = "MS Dedup — Дедупликация товаров МойСклад"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 20
    page.scroll = ft.ScrollMode.AUTO

    if not await _check_redis():
        page.controls.clear()
        page.add(
            ft.Container(
                content=ft.Column(
                    [
                        ft.Text(
                            "Ошибка подключения к Redis",
                            size=20,
                            weight=ft.FontWeight.BOLD,
                            color=ft.Colors.RED,
                        ),
                        ft.Text(f"REDIS_URL={settings.REDIS_URL}"),
                        ft.Text("Проверьте что Redis запущен и доступен."),
                    ],
                    spacing=10,
                ),
                padding=20,
            )
        )
        page.update()
        return

    await _ensure_broker_started()

    page.session.set("active_processes", {})
    await show_login(page)


def run() -> None:
    """Запустить Flet-сервер веб-приложения."""
    ft.app(
        target=main,
        view=ft.AppView.WEB_BROWSER,
        port=settings.FLET_PORT,
        host="0.0.0.0",
    )
