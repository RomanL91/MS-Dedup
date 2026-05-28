"""Доменные операции над API МойСклад.

:class:`MSClient` — фасад поверх :class:`~app.ms.transport.MSTransport`,
предоставляющий осмысленные операции: получить сущность, определить её тип,
постранично загрузить документы и позиции, заменить ассортимент в позиции,
объединить позиции, архивировать или удалить сущность.

Класс не знает про ретраи и лимиты — этим занимается транспорт. Сам клиент
используется как асинхронный контекстный менеджер::

    async with MSClient(login=..., password=..., redis=redis) as client:
        await client.check_auth()
"""
from typing import Any

from redis.asyncio import Redis

from app.ms.errors import MSRequestError
from app.ms.transport import MSTransport
from app.parsing.hrefs import extract_id_from_href


class MSClient:
    """Высокоуровневый клиент МойСклад для операций дедупликации."""

    def __init__(
        self,
        login: str,
        password: str,
        redis: Redis,
        base_url: str | None = None,
        max_retries: int | None = None,
        timeout: float | None = None,
    ) -> None:
        self._transport = MSTransport(
            login=login,
            password=password,
            redis=redis,
            base_url=base_url,
            max_retries=max_retries,
            timeout=timeout,
        )

    async def aclose(self) -> None:
        """Закрыть транспорт (и underlying HTTP-клиент)."""
        await self._transport.aclose()

    async def __aenter__(self) -> "MSClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    @property
    def base_url(self) -> str:
        """Базовый URL API МойСклад."""
        return self._transport.base_url

    # ---------------- Авторизация ----------------

    async def check_auth(self) -> bool:
        """Проверить логин/пароль запросом ``/context/employee``.

        Возвращает ``True`` при успехе и ``False`` при ошибке авторизации.
        Прочие ошибки (сеть, 5xx) пробрасываются наружу.
        """
        from app.ms.errors import MSAuthError

        try:
            await self._transport.request("GET", "/context/employee")
            return True
        except MSAuthError:
            return False

    # ---------------- Сущности ----------------

    async def get_entity(self, entity_type: str, entity_id: str) -> dict:
        """GET ``/entity/{type}/{id}``. Работает для product/variant/service/bundle."""
        resp = await self._transport.request(
            "GET", f"/entity/{entity_type}/{entity_id}"
        )
        return resp.json()

    async def resolve_entity_id(self, entity_type: str, entity_id: str) -> str:
        """Преобразовать введённый id в настоящий API id сущности.

        У МС id из URL веб-интерфейса (UI id) и id из ``meta.href`` (API id)
        могут различаться — везде в логике замены нужен именно API id.
        """
        data = await self.get_entity(entity_type, entity_id)
        href = ((data.get("meta") or {}).get("href")) or ""
        if not href:
            return entity_id
        real_id = extract_id_from_href(href)
        return real_id or entity_id

    async def resolve_entity_type(self, entity_id: str) -> str | None:
        """Определить тип сущности по UUID через API.

        Пробует последовательно ``product → bundle → service → variant`` и
        возвращает первый найденный тип, либо ``None`` если сущность не найдена.
        Нужен, когда тип нельзя определить по ссылке (чистый UUID или ``#good/edit?id=``).
        """
        for entity_type in ("product", "bundle", "service", "variant"):
            try:
                await self.get_entity(entity_type, entity_id)
                return entity_type
            except MSRequestError:
                continue
        return None

    # ---------------- Документы и позиции ----------------

    async def get_documents_page(
        self,
        doc_type: str,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """Получить страницу документов заданного типа с раскрытыми позициями."""
        resp = await self._transport.request(
            "GET",
            f"/entity/{doc_type}",
            params={"expand": "positions", "limit": limit, "offset": offset},
        )
        return resp.json()

    async def get_positions_page(
        self, positions_href: str, limit: int = 1000, offset: int = 0
    ) -> dict:
        """Получить страницу позиций документа по ссылке ``positions.meta.href``."""
        resp = await self._transport.request(
            "GET",
            positions_href,
            params={"limit": limit, "offset": offset},
        )
        return resp.json()

    async def get_all_positions(self, positions_meta: dict) -> list[dict]:
        """Загрузить все позиции документа с пагинацией.

        Не полагаемся на ``meta.size`` — итерируем, пока приходят строки.
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
        """Заменить ассортимент позиции на целевую сущность (PUT по ссылке позиции)."""
        body = {
            "assortment": {
                "meta": {
                    "href": target_assortment_href,
                    "type": entity_type,
                    "mediaType": "application/json",
                }
            }
        }
        await self._transport.request("PUT", position_href, json_body=body)
        return True

    async def update_position_quantity(
        self, position_href: str, quantity: float
    ) -> bool:
        """Изменить количество в позиции (PUT ``{"quantity": ...}``)."""
        await self._transport.request(
            "PUT", position_href, json_body={"quantity": quantity}
        )
        return True

    async def delete_position(self, position_href: str) -> bool:
        """Удалить позицию документа (DELETE по её ссылке)."""
        await self._transport.request("DELETE", position_href)
        return True

    async def merge_positions(
        self,
        doc_type: str,
        doc_id: str,
        keep_position_href: str,
        remove_position_href: str,
        merged_quantity: float,
    ) -> bool:
        """Объединить две позиции: оставить целевую с суммарным количеством, удалить дубль.

        Применяется, когда в одном документе уже есть и дубль, и целевая
        сущность: их количества складываются в целевой позиции, а позиция-дубль
        удаляется.
        """
        await self.update_position_quantity(keep_position_href, merged_quantity)
        await self.delete_position(remove_position_href)
        return True

    # ---------------- Очистка дублей ----------------

    async def delete_entity(self, entity_type: str, entity_id: str) -> bool:
        """DELETE ``/entity/{type}/{id}`` — безвозвратное удаление сущности."""
        await self._transport.request("DELETE", f"/entity/{entity_type}/{entity_id}")
        return True

    async def archive_entity(self, entity_type: str, entity_id: str) -> bool:
        """Мягкое удаление: перевод сущности в архив через PUT ``{"archived": true}``.

        Альтернатива :meth:`delete_entity` для случаев, когда сущность
        используется в документах и физическое удаление вернёт 409. В архив можно
        отправить даже используемые товары/услуги/комплекты/модификации.
        """
        await self._transport.request(
            "PUT",
            f"/entity/{entity_type}/{entity_id}",
            json_body={"archived": True},
        )
        return True
