import re

UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)

ENTITY_TYPES: dict[str, str] = {
    "product": "Товар",
    "variant": "Модификация",
    "service": "Услуга",
    "bundle": "Комплект",
}

ENTITY_TYPE_NAMES_PLURAL: dict[str, str] = {
    "product": "товаров",
    "variant": "модификаций",
    "service": "услуг",
    "bundle": "комплектов",
}

ENTITY_TYPE_TARGET_NAMES: dict[str, str] = {
    "product": "Целевой товар",
    "variant": "Целевая модификация",
    "service": "Целевая услуга",
    "bundle": "Целевой комплект",
}

ENTITY_TYPE_GROUP_NAMES: dict[str, str] = {
    "product": "Товары",
    "variant": "Модификации",
    "service": "Услуги",
    "bundle": "Комплекты",
}

# Человекочитаемые названия типов документов (коды МС → русский).
# Используется и в UI (карта замен), и в сообщениях прогресса воркера.
DOC_TYPE_NAMES: dict[str, str] = {
    "customerorder": "Заказ покупателя",
    "purchaseorder": "Заказ поставщику",
    "invoiceout": "Счёт покупателю",
    "invoicein": "Счёт поставщика",
    "demand": "Отгрузка",
    "supply": "Приёмка",
    "salesreturn": "Возврат покупателя",
    "purchasereturn": "Возврат поставщику",
    "loss": "Списание",
    "enter": "Оприходование",
    "move": "Перемещение",
    "internalorder": "Внутренний заказ",
    "retaildemand": "Розничные продажи",
    "retailsalesreturn": "Розничные возвраты",
    "inventory": "Инвентаризации",
}

# /entity/<type>/UUID — однозначно определяет тип
_API_ENTITY_RE = re.compile(
    r"/entity/(product|variant|service|bundle)/[0-9a-f-]{36}",
    re.IGNORECASE,
)

# Веб-интерфейс МС:
#   #feature/edit?id=... — это однозначно модификация
#   #bundle/edit?id=...  — это однозначно комплект
#   #good/edit?id=...    — это товар ИЛИ услуга (отличить можно только через API)
_UI_FRAGMENT_FEATURE_RE = re.compile(
    r"#feature/edit\?id=",
    re.IGNORECASE,
)
_UI_FRAGMENT_BUNDLE_RE = re.compile(
    r"#bundle/edit\?id=",
    re.IGNORECASE,
)


def extract_uuid(raw: str) -> str | None:
    """Извлекает первый UUID из строки любого формата.

    Принимает: чистый UUID, ссылку МС веб-интерфейса (?id=UUID),
    ссылку API (.../entity/product/UUID). Возвращает UUID в нижнем
    регистре или None если не найден.
    """
    if not raw:
        return None
    match = UUID_PATTERN.search(raw)
    return match.group(0).lower() if match else None


def extract_uuids(text: str) -> list[str]:
    """Извлекает все UUID из текста (в нижнем регистре, без дубликатов сохраняя порядок)."""
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
    """Определяет тип сущности по ссылке/строке.

    Возвращает "product" | "variant" | "service" | "bundle" | None.

    Однозначно распознаются:
    - API href: /entity/product|variant|service|bundle/UUID
    - Веб-интерфейс: #feature/edit?id=UUID — модификация
    - Веб-интерфейс: #bundle/edit?id=UUID  — комплект

    Возвращают None (тип нужно определять через API):
    - #good/edit?id=UUID — товар или услуга (по URL не различить)
    - Чистый UUID
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


def build_assortment_href(entity_type: str, uuid: str, base: str) -> str:
    """Строит API href для assortment.meta по типу сущности.

    base — базовый URL API (например settings.MS_API_BASE).
    """
    return f"{base.rstrip('/')}/entity/{entity_type}/{uuid}"
