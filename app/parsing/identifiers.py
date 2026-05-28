"""Извлечение UUID и определение типа сущности из пользовательского ввода.

Пользователь может вставить идентификатор в разных форматах: чистый UUID,
ссылку из веб-интерфейса МС (``#good/edit?id=...``) или прямую API-ссылку
(``.../entity/product/UUID``). Эти функции приводят ввод к UUID и, по
возможности, определяют тип сущности без обращения к сети.
"""
import re

UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)
"""Регулярное выражение UUID в любом регистре."""

# /entity/<type>/UUID — однозначно определяет тип
_API_ENTITY_RE = re.compile(
    r"/entity/(product|variant|service|bundle)/[0-9a-f-]{36}",
    re.IGNORECASE,
)

# Веб-интерфейс МС:
#   #feature/edit?id=... — это однозначно модификация
#   #bundle/edit?id=...  — это однозначно комплект
#   #good/edit?id=...    — это товар ИЛИ услуга (отличить можно только через API)
_UI_FRAGMENT_FEATURE_RE = re.compile(r"#feature/edit\?id=", re.IGNORECASE)
_UI_FRAGMENT_BUNDLE_RE = re.compile(r"#bundle/edit\?id=", re.IGNORECASE)


def extract_uuid(raw: str) -> str | None:
    """Извлекает первый UUID из строки любого формата.

    Принимает: чистый UUID, ссылку МС веб-интерфейса (``?id=UUID``),
    ссылку API (``.../entity/product/UUID``). Возвращает UUID в нижнем
    регистре или ``None`` если не найден.
    """
    if not raw:
        return None
    match = UUID_PATTERN.search(raw)
    return match.group(0).lower() if match else None


def extract_uuids(text: str) -> list[str]:
    """Извлекает все UUID из текста.

    Результат — в нижнем регистре, без дубликатов, с сохранением порядка
    первого появления. Используется для разбора массовой вставки нескольких
    ссылок в одно поле.
    """
    if not text:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for m in UUID_PATTERN.findall(text):
        u = m.lower()
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


def detect_entity_type(raw: str) -> str | None:
    """Определяет тип сущности по ссылке/строке без обращения к API.

    Возвращает ``"product"`` | ``"variant"`` | ``"service"`` | ``"bundle"`` | ``None``.

    Однозначно распознаются:

    * API href: ``/entity/product|variant|service|bundle/UUID``;
    * веб-интерфейс ``#feature/edit?id=UUID`` — модификация;
    * веб-интерфейс ``#bundle/edit?id=UUID`` — комплект.

    Возвращают ``None`` (тип нужно определять через API):

    * ``#good/edit?id=UUID`` — товар или услуга (по URL не различить);
    * чистый UUID.
    """
    if not raw:
        return None
    m = _API_ENTITY_RE.search(raw)
    if m:
        return m.group(1).lower()
    if _UI_FRAGMENT_FEATURE_RE.search(raw):
        return "variant"
    if _UI_FRAGMENT_BUNDLE_RE.search(raw):
        return "bundle"
    return None
