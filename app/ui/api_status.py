"""Индикаторы нагрузки на API МойСклад в шапке приложения.

Показывают текущее заполнение корзинки запросов и PUT-гарда, а также паузу
ожидания, если МС вернул 429/5xx. Данные берутся из тех же распределённых
лимитеров, что и у воркера, поэтому индикаторы отражают суммарную нагрузку.
"""
import asyncio
import logging

import flet as ft

from app.core.config import settings
from app.core.redis import redis_session
from app.infrastructure.limiter import (
    RedisBucketLimiter,
    RedisPutGuard,
    get_wait_state,
)

log = logging.getLogger(__name__)


def _make_indicator(caption: str) -> dict:
    """Собрать виджет-индикатор: точка-светофор, подпись, прогресс-бар и счётчик."""
    bar = ft.ProgressBar(
        width=100, value=0, color=ft.Colors.GREEN, bgcolor=ft.Colors.GREY_200
    )
    label = ft.Text("0/0", size=11)
    dot = ft.Text("🟢", size=14)
    row = ft.Row(
        [dot, ft.Text(caption, size=11, color=ft.Colors.GREY), bar, label],
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
        spacing=4,
        tight=True,
    )
    return {"row": row, "bar": bar, "label": label, "dot": dot}


def make_rate_indicator() -> dict:
    """Индикатор общей корзинки запросов («Запросы»)."""
    return _make_indicator("Запросы")


def make_put_indicator() -> dict:
    """Индикатор PUT-гарда («PUT»)."""
    return _make_indicator("PUT")


def update_indicator(ind: dict, used: int, total: int) -> None:
    """Обновить значение и цвет индикатора по соотношению использовано/лимит."""
    ind["label"].value = f"{used}/{total}"
    if total > 0:
        frac = min(1.0, used / total)
    else:
        frac = 0.0
    ind["bar"].value = frac
    if frac >= 0.9:
        ind["bar"].color = ft.Colors.RED
        ind["dot"].value = "🔴"
    elif frac >= 0.7:
        ind["bar"].color = ft.Colors.ORANGE
        ind["dot"].value = "🟡"
    else:
        ind["bar"].color = ft.Colors.GREEN
        ind["dot"].value = "🟢"


async def poll_api_status(
    page: ft.Page,
    rate_ind: dict,
    put_ind: dict,
    wait_text: ft.Text,
) -> None:
    """Фоновая корутина: периодически обновляет индикаторы нагрузки в шапке."""
    import time as _time

    async with redis_session() as redis:
        limiter = RedisBucketLimiter(
            redis, size=settings.MS_BUCKET_SIZE, window=settings.MS_BUCKET_WINDOW_SEC
        )
        put_guard = RedisPutGuard(
            redis, max_per_min=settings.MS_PUT_PER_MIN, window=settings.MS_PUT_WINDOW_SEC
        )
        try:
            while True:
                try:
                    rate_used, rate_total = await limiter.usage()
                    put_used, put_total = await put_guard.max_usage()
                    wait = await get_wait_state(redis)
                except Exception as e:  # noqa: BLE001
                    log.debug("Status poll error: %s", e)
                    await asyncio.sleep(2)
                    continue

                update_indicator(rate_ind, rate_used, rate_total)
                update_indicator(put_ind, put_used, put_total)

                if wait:
                    until_ms, reason = wait
                    remaining = max(0, (until_ms - int(_time.time() * 1000)) / 1000.0)
                    wait_text.value = f"🔴 Пауза: {remaining:.1f}с ({reason})"
                else:
                    wait_text.value = ""

                try:
                    page.update()
                except Exception:  # noqa: BLE001
                    return

                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            return
