"""Реестр человекочитаемых названий типов сущностей.

Единый источник истины для всех словесных форм типа сущности. Раньше эти данные
были разбросаны по нескольким словарям в ``utils.py`` и ещё раз продублированы
внутри ``main.py`` (``ENT_WORDS``). Теперь все формы описаны один раз в
:data:`ENTITY_TYPE_INFO`, а привычные «плоские» словари выводятся из него — это
устраняет дублирование и упрощает добавление нового типа (правится одно место).

Цвета для подсветки в UI сюда намеренно не входят: слой domain не должен знать
про Flet. Цветовая карта живёт в UI.
"""
from dataclasses import dataclass

from app.domain.enums import EntityType


@dataclass(frozen=True)
class EntityTypeInfo:
    """Набор словесных форм одного типа сущности (для UI и сообщений воркера)."""

    code: str            # код типа в API МС, напр. "product"
    singular: str        # именительный, ед.ч.: "Товар"
    group: str           # множественное (название группы): "Товары"
    genitive_plural: str # родительный, мн.ч.: "товаров"
    target: str          # подпись целевой сущности: "Целевой товар"
    word: str            # строчное слово для перечислений: "товар"


ENTITY_TYPE_INFO: dict[str, EntityTypeInfo] = {
    EntityType.PRODUCT.value: EntityTypeInfo(
        "product", "Товар", "Товары", "товаров", "Целевой товар", "товар"
    ),
    EntityType.VARIANT.value: EntityTypeInfo(
        "variant", "Модификация", "Модификации", "модификаций",
        "Целевая модификация", "модификация",
    ),
    EntityType.SERVICE.value: EntityTypeInfo(
        "service", "Услуга", "Услуги", "услуг", "Целевая услуга", "услуга"
    ),
    EntityType.BUNDLE.value: EntityTypeInfo(
        "bundle", "Комплект", "Комплекты", "комплектов", "Целевой комплект", "комплект"
    ),
}
"""Полное описание каждого поддерживаемого типа сущности."""


# --- Производные представления (единый источник истины — ENTITY_TYPE_INFO) ---

ENTITY_TYPES: dict[str, str] = {c: i.singular for c, i in ENTITY_TYPE_INFO.items()}
ENTITY_TYPE_GROUP_NAMES: dict[str, str] = {
    c: i.group for c, i in ENTITY_TYPE_INFO.items()
}
ENTITY_TYPE_NAMES_PLURAL: dict[str, str] = {
    c: i.genitive_plural for c, i in ENTITY_TYPE_INFO.items()
}
ENTITY_TYPE_TARGET_NAMES: dict[str, str] = {
    c: i.target for c, i in ENTITY_TYPE_INFO.items()
}
ENTITY_TYPE_WORDS: dict[str, str] = {c: i.word for c, i in ENTITY_TYPE_INFO.items()}
