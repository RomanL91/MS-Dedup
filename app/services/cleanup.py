"""Стратегии очистки дублей после замены позиций (паттерн «Стратегия»).

Раньше выбор действия над дублями (архивировать / удалить / ничего) был размазан
по ветвлениям ``if cleanup_mode == ...`` и наборам словесных форм в воркере. Здесь
каждое поведение оформлено отдельным классом с единым интерфейсом
:class:`CleanupStrategy`. Это убирает ветвления из оркестратора и держит все
словесные формы режима в одном месте.

Каждая стратегия знает:

* нужно ли вообще трогать сущности (``touches_entities``);
* как применить действие к одной сущности (:meth:`CleanupStrategy.apply`);
* словесные формы для сообщений прогресса и логов.
"""
from abc import ABC, abstractmethod

from app.domain.enums import CleanupMode
from app.ms.client import MSClient


class CleanupStrategy(ABC):
    """Базовый интерфейс стратегии очистки дублей."""

    #: режим, который реализует стратегия
    mode: CleanupMode
    #: трогает ли стратегия сами сущности (False — только замена в документах)
    touches_entities: bool = True

    # Словесные формы для сообщений (по умолчанию — для режимов с действием).
    gerund: str = ""          # «Архивирование» / «Удаление»
    past_negative: str = ""   # «не архивирован» / «не удалён»
    past: str = ""            # «архивировано» / «удалено»
    action_noun: str = ""     # «при {action_noun}»: «архивации» / «удалении»
    genitive: str = ""        # «без {genitive}»: «архивации» / «удаления»

    @abstractmethod
    async def apply(
        self, client: MSClient, entity_type: str, entity_id: str
    ) -> None:
        """Применить действие очистки к одной сущности (или ничего не делать)."""
        raise NotImplementedError


class ArchiveCleanup(CleanupStrategy):
    """Перевод дублей в архив (обратимо, режим по умолчанию)."""

    mode = CleanupMode.ARCHIVE
    touches_entities = True
    gerund = "Архивирование"
    past_negative = "не архивирован"
    past = "архивировано"
    action_noun = "архивации"
    genitive = "архивации"

    async def apply(
        self, client: MSClient, entity_type: str, entity_id: str
    ) -> None:
        await client.archive_entity(entity_type, entity_id)


class DeleteCleanup(CleanupStrategy):
    """Безвозвратное удаление дублей."""

    mode = CleanupMode.DELETE
    touches_entities = True
    gerund = "Удаление"
    past_negative = "не удалён"
    past = "удалено"
    action_noun = "удалении"
    genitive = "удаления"

    async def apply(
        self, client: MSClient, entity_type: str, entity_id: str
    ) -> None:
        await client.delete_entity(entity_type, entity_id)


class NoopCleanup(CleanupStrategy):
    """Ничего не делать с дублями — только замена в документах."""

    mode = CleanupMode.NONE
    touches_entities = False

    async def apply(
        self, client: MSClient, entity_type: str, entity_id: str
    ) -> None:
        return None


_STRATEGIES: dict[str, CleanupStrategy] = {
    ArchiveCleanup.mode.value: ArchiveCleanup(),
    DeleteCleanup.mode.value: DeleteCleanup(),
    NoopCleanup.mode.value: NoopCleanup(),
}


def get_cleanup_strategy(cleanup_mode: str) -> CleanupStrategy:
    """Вернуть стратегию по строковому коду режима.

    Неизвестный режим трактуется как ``archive`` (поведение по умолчанию,
    совпадающее с прежней валидацией воркера).
    """
    return _STRATEGIES.get(cleanup_mode, _STRATEGIES[CleanupMode.ARCHIVE.value])
