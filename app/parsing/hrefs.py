"""Построение и разбор API-ссылок (href) сущностей МойСклад.

В API МС каждая сущность адресуется ссылкой вида
``{base}/entity/{type}/{id}``. Здесь собраны функции для сборки такой ссылки и
для извлечения идентификатора обратно из href.
"""
from app.core.config import settings


def extract_id_from_href(href: str) -> str:
    """Извлекает идентификатор сущности из её API-ссылки.

    Отбрасывает завершающий слэш и query-параметры и берёт последний сегмент пути.
    """
    return href.rstrip("/").rsplit("/", 1)[-1].split("?")[0]


def build_entity_href(
    entity_type: str, entity_id: str, base: str | None = None
) -> str:
    """Собирает API-ссылку сущности по её типу и идентификатору.

    ``base`` по умолчанию берётся из настроек (:data:`settings.MS_API_BASE`).
    """
    base = base or settings.MS_API_BASE
    return f"{base}/entity/{entity_type}/{entity_id}"
