"""Транспортный слой клиента МойСклад.

:class:`MSTransport` отвечает исключительно за то, *как* выполняется HTTP-запрос:

* basic-авторизация по логину/паролю;
* соблюдение лимитов МС (общая корзинка запросов и PUT-гард по сущности);
* ограничение числа параллельных in-flight запросов;
* ретраи с экспоненциальным бэкоффом на сетевые ошибки, 429 и 5xx;
* единый разбор ошибочных ответов в :class:`~app.ms.errors.MSRequestError`.

Что именно запрашивается (сущности, документы, позиции) — задаёт уровнем выше
:class:`~app.ms.client.MSClient`.
"""
import asyncio
import base64
import json as _json
import logging
import time
from typing import Any

import httpx
from redis.asyncio import Redis

from app.core.config import settings
from app.infrastructure.limiter import (
    RedisBucketLimiter,
    RedisPutGuard,
    set_wait_until,
)
from app.ms.errors import MSAuthError, MSRequestError

log = logging.getLogger(__name__)


def format_ms_error(status_code: int, body_text: str) -> str:
    """Достаёт читаемое сообщение из JSON-ответа МС если возможно.

    МС возвращает ошибки массивом ``errors`` с полями ``error``/``code``/
    ``dependencies``. Если тело не разбирается как ожидаемый JSON — возвращаем
    усечённый текст ответа вместе с HTTP-кодом.
    """
    try:
        data = _json.loads(body_text)
    except Exception:  # noqa: BLE001
        return f"HTTP {status_code}: {body_text[:200]}"

    errors = data.get("errors") if isinstance(data, dict) else None
    if isinstance(errors, list) and errors:
        first = errors[0] if isinstance(errors[0], dict) else {}
        msg = first.get("error") or ""
        code = first.get("code")
        deps = first.get("dependencies") or []
        parts = [f"HTTP {status_code}"]
        if code is not None:
            parts.append(f"code={code}")
        if msg:
            parts.append(msg)
        if deps:
            parts.append(f"зависимостей: {len(deps)}")
        return " | ".join(parts)
    return f"HTTP {status_code}: {body_text[:200]}"


class MSTransport:
    """Низкоуровневый HTTP-транспорт к API МойСклад с лимитами и ретраями."""

    def __init__(
        self,
        login: str,
        password: str,
        redis: Redis,
        base_url: str | None = None,
        max_retries: int | None = None,
        timeout: float | None = None,
    ) -> None:
        token = base64.b64encode(f"{login}:{password}".encode("utf-8")).decode("ascii")
        self._auth_header = f"Basic {token}"
        self._base = (base_url or settings.MS_API_BASE).rstrip("/")
        self._redis = redis
        self._limiter = RedisBucketLimiter(
            redis,
            size=settings.MS_BUCKET_SIZE,
            window=settings.MS_BUCKET_WINDOW_SEC,
        )
        self._put_guard = RedisPutGuard(
            redis,
            max_per_min=settings.MS_PUT_PER_MIN,
            window=settings.MS_PUT_WINDOW_SEC,
        )
        self._parallel = asyncio.Semaphore(settings.MS_MAX_PARALLEL)
        self._max_retries = max_retries or settings.MS_MAX_RETRIES
        self._client = httpx.AsyncClient(
            timeout=timeout or settings.MS_REQUEST_TIMEOUT,
            follow_redirects=True,
            headers={
                "Authorization": self._auth_header,
                "Accept-Encoding": "gzip",
                "Content-Type": "application/json",
            },
        )

    async def aclose(self) -> None:
        """Закрыть underlying httpx-клиент."""
        await self._client.aclose()

    @property
    def base_url(self) -> str:
        """Базовый URL API без завершающего слэша."""
        return self._base

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> httpx.Response:
        """Выполнить запрос с соблюдением лимитов и ретраями.

        Относительный ``url`` достраивается до абсолютного по базовому URL.
        Бросает :class:`~app.ms.errors.MSAuthError` на 401 и
        :class:`~app.ms.errors.MSRequestError` на прочих ошибках после исчерпания ретраев.
        """
        if not url.startswith("http"):
            url = f"{self._base}{url}"
        attempt = 0
        backoff = 1.0
        is_put = method.upper() == "PUT"
        while True:
            # 1) общий rate limit (45 единиц / 3с, корзинка)
            await self._limiter.acquire()
            # 2) PUT-guard (не более N PUT/мин к одной сущности — защита от автоотключения)
            if is_put:
                await self._put_guard.acquire(url)
            # 3) семафор параллельных in-flight (МС: не более 5 от пользователя)
            async with self._parallel:
                try:
                    resp = await self._client.request(
                        method, url, params=params, json=json_body
                    )
                except (
                    httpx.ConnectError,
                    httpx.ReadTimeout,
                    httpx.RemoteProtocolError,
                    httpx.ReadError,
                    httpx.WriteError,
                ) as e:
                    attempt += 1
                    if attempt > self._max_retries:
                        raise MSRequestError(
                            f"Network error after {self._max_retries} retries: {e}"
                        ) from e
                    log.warning(
                        "Network error %s, retry %s in %ss", e, attempt, backoff
                    )
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue

            if resp.status_code == 401:
                raise MSAuthError("Authentication failed")
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                attempt += 1
                if attempt > self._max_retries:
                    raise MSRequestError(format_ms_error(resp.status_code, resp.text))
                log.warning(
                    "HTTP %s on %s %s, retry %s in %ss",
                    resp.status_code,
                    method,
                    url,
                    attempt,
                    backoff,
                )
                # подсказать UI отобразить «ждём от МС: X сек»
                try:
                    await set_wait_until(
                        self._redis,
                        int(time.time() * 1000) + int(backoff * 1000),
                        f"HTTP {resp.status_code} от МС, бэкофф {backoff:g}s",
                    )
                except Exception:  # noqa: BLE001
                    pass
                await asyncio.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code >= 400:
                raise MSRequestError(format_ms_error(resp.status_code, resp.text))
            return resp
