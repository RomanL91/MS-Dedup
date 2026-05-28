"""Перечисления предметной области.

Используются строковые перечисления (``str``-наследники), чтобы значения
прозрачно сериализовались в JSON для Redis и совпадали со строковыми кодами,
которые приходят из API МойСклад и веб-интерфейса.
"""
from enum import Enum


class ProcessStatus(str, Enum):
    """Статус процесса дедупликации на протяжении его жизненного цикла."""

    PENDING = "pending"
    SCHEDULED = "scheduled"
    SCANNING = "scanning"
    CONFIRMING = "confirming"
    REPLACING = "replacing"
    DELETING = "deleting"
    DONE = "done"
    CANCELLED = "cancelled"
    ERROR = "error"
    # Процесс был запущен, но воркер умер (перезапуск сервиса). Не терминальный:
    # пользователю предлагается перезапустить его заново.
    INTERRUPTED = "interrupted"


TERMINAL_STATUSES = {ProcessStatus.DONE, ProcessStatus.CANCELLED, ProcessStatus.ERROR}
"""Статусы, после которых процесс уже не меняется — карточка больше не опрашивается."""


class EntityType(str, Enum):
    """Тип дедуплицируемой сущности МойСклад.

    Значения совпадают с кодами типов в API (``meta.type``), поэтому экземпляры
    можно напрямую сравнивать со строками из ответов МС.
    """

    PRODUCT = "product"      # Товар
    VARIANT = "variant"      # Модификация
    SERVICE = "service"      # Услуга
    BUNDLE = "bundle"        # Комплект


class CleanupMode(str, Enum):
    """Что делать с дублями после замены их в документах."""

    ARCHIVE = "archive"  # перевести в архив (обратимо, по умолчанию)
    DELETE = "delete"    # удалить безвозвратно
    NONE = "none"        # ничего не делать — только замена в документах


CLEANUP_MODES = tuple(m.value for m in CleanupMode)
"""Допустимые строковые значения режимов очистки (для валидации входных данных)."""
