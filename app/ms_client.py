import asyncio
import base64
import logging
import time
from typing import Any

import httpx
from redis.asyncio import Redis

from app.config import settings
from app.limiter import RedisBucketLimiter, RedisPutGuard, set_wait_until

log = logging.getLogger(__name__)


DOCUMENT_TYPES: list[str] = [
    "customerorder",
    "purchaseorder",
    "invoiceout",
    "invoicein",
    "demand",
    "supply",
    "salesreturn",
    "purchasereturn",
    "loss",
    "enter",
    "move",
    "internalorder",
    "retaildemand",
    "retailsalesreturn",
    "inventory",
]


class MSAuthError(Exception):
    pass


class MSRequestError(Exception):
    pass


def _format_ms_error(status_code: int, body_text: str) -> str:
    """Достаёт читаемое сообщение из JSON-ответа МС если возможно."""
    import json as _json

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


def extract_id_from_href(href: str) -> str:
    return href.rstrip("/").rsplit("/", 1)[-1].split("?")[0]


def build_product_href(product_id: str, base: str | None = None) -> str:
    base = base or settings.MS_API_BASE
    return f"{base}/entity/product/{product_id}"


def build_entity_href(entity_type: str, entity_id: str, base: str | None = None) -> str:
    base = base or settings.MS_API_BASE
    return f"{base}/entity/{entity_type}/{entity_id}"


class MSClient:
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
        await self._client.aclose()

    async def __aenter__(self) -> "MSClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    @property
    def base_url(self) -> str:
        return self._base

    async def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> httpx.Response:
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
                    raise MSRequestError(_format_ms_error(resp.status_code, resp.text))
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
                raise MSRequestError(_format_ms_error(resp.status_code, resp.text))
            return resp

    async def check_auth(self) -> bool:
        try:
            await self._request("GET", "/context/employee")
            return True
        except MSAuthError:
            return False

    async def get_product(self, product_id: str) -> dict:
        return await self.get_entity("product", product_id)

    async def get_entity(self, entity_type: str, entity_id: str) -> dict:
        """GET /entity/{type}/{id}. Работает для product и variant."""
        resp = await self._request("GET", f"/entity/{entity_type}/{entity_id}")
        return resp.json()

    async def resolve_product_id(self, product_id: str) -> str:
        return await self.resolve_entity_id("product", product_id)

    async def resolve_entity_id(self, entity_type: str, entity_id: str) -> str:
        """Преобразовать введённый id (возможно UI id из URL веб-интерфейса)
        в настоящий API id сущности. У МС meta.href и uuidHref могут различаться."""
        data = await self.get_entity(entity_type, entity_id)
        href = ((data.get("meta") or {}).get("href")) or ""
        if not href:
            return entity_id
        real_id = extract_id_from_href(href)
        return real_id or entity_id

    async def resolve_entity_type(self, entity_id: str) -> str | None:
        """Определить тип сущности по UUID через API.

        Пробует последовательно product → bundle → service → variant.
        Возвращает первый найденный тип или None если сущность не найдена.
        Нужен когда тип нельзя определить по ссылке (чистый UUID или #good/edit?id=).
        """
        for entity_type in ("product", "bundle", "service", "variant"):
            try:
                await self.get_entity(entity_type, entity_id)
                return entity_type
            except MSRequestError:
                continue
        return None

    async def get_documents_page(
        self,
        doc_type: str,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        resp = await self._request(
            "GET",
            f"/entity/{doc_type}",
            params={"expand": "positions", "limit": limit, "offset": offset},
        )
        return resp.json()

    async def get_positions_page(
        self, positions_href: str, limit: int = 1000, offset: int = 0
    ) -> dict:
        resp = await self._request(
            "GET",
            positions_href,
            params={"limit": limit, "offset": offset},
        )
        return resp.json()

    async def get_all_positions(self, positions_meta: dict) -> list[dict]:
        """Загрузить все позиции документа с пагинацией.

        Не полагаемся на meta.size — итерируем пока приходят строки.
        """
        href = positions_meta.get("href")
        if not href:
            return []
        rows: list[dict] = []
        offset = 0
        limit = 1000
        while True:
            data = await self.get_positions_page(href, limit=limit, offset=offset)
            page_rows = data.get("rows", []) or []
            if not page_rows:
                break
            rows.extend(page_rows)
            if len(page_rows) < limit:
                break
            offset += limit
        return rows

    async def update_position(
        self,
        position_href: str,
        target_assortment_href: str,
        entity_type: str = "product",
    ) -> bool:
        body = {
            "assortment": {
                "meta": {
                    "href": target_assortment_href,
                    "type": entity_type,
                    "mediaType": "application/json",
                }
            }
        }
        await self._request("PUT", position_href, json_body=body)
        return True

    async def update_position_quantity(
        self, position_href: str, quantity: float
    ) -> bool:
        await self._request("PUT", position_href, json_body={"quantity": quantity})
        return True

    async def delete_position(self, position_href: str) -> bool:
        await self._request("DELETE", position_href)
        return True

    async def merge_positions(
        self,
        doc_type: str,
        doc_id: str,
        keep_position_href: str,
        remove_position_href: str,
        merged_quantity: float,
    ) -> bool:
        await self.update_position_quantity(keep_position_href, merged_quantity)
        await self.delete_position(remove_position_href)
        return True

    async def delete_product(self, product_id: str) -> bool:
        return await self.delete_entity("product", product_id)

    async def delete_entity(self, entity_type: str, entity_id: str) -> bool:
        """DELETE /entity/{type}/{id}. Работает для product и variant."""
        await self._request("DELETE", f"/entity/{entity_type}/{entity_id}")
        return True

    async def archive_entity(self, entity_type: str, entity_id: str) -> bool:
        """Мягкое удаление: переводит сущность в архив через PUT {"archived": true}.

        Альтернатива delete_entity для случаев, когда сущность используется
        в документах и физическое удаление вернёт 409. В архив можно отправить
        даже используемые товары/услуги/комплекты/модификации.
        """
        await self._request(
            "PUT",
            f"/entity/{entity_type}/{entity_id}",
            json_body={"archived": True},
        )
        return True
