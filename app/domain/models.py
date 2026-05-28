"""Pydantic-модели предметной области.

Модели сериализуются в JSON и хранятся в Redis (прогресс, метаданные задач,
очередь отложенных запусков). Поля ``entity_type`` и ``cleanup_mode`` остаются
строками для стабильности формата хранения — на уровне сервисов они при
необходимости приводятся к :class:`~app.domain.enums.EntityType` и
:class:`~app.domain.enums.CleanupMode`.
"""
from typing import Optional

from pydantic import BaseModel, Field

from app.domain.enums import ProcessStatus


class ProgressState(BaseModel):
    """Текущее состояние процесса, которое воркер пишет, а UI опрашивает.

    Хранится в Redis по ключу ``progress:{task_id}`` и полностью описывает то,
    что нужно показать в карточке процесса: статус, счётчики и сообщения.
    """

    status: ProcessStatus = ProcessStatus.PENDING
    current: int = 0
    total: int = 0
    message: str = ""
    error: Optional[str] = None
    replaced_count: int = 0
    deleted_count: int = 0
    delete_errors: list[str] = Field(default_factory=list)
    # id сущностей, пропущенных при очистке: замена их позиций не прошла,
    # поэтому архивировать/удалять их нельзя (иначе осиротеют ссылки в документах)
    skipped_ids: list[str] = Field(default_factory=list)


class AffectedPosition(BaseModel):
    """Одна позиция документа, которую затронет замена.

    Формируется на фазе сканирования и сохраняется в Redis, чтобы UI мог
    отрисовать «карту замен», а фаза замены — применить изменения.
    """

    doc_type: str
    doc_id: str
    doc_name: str
    position_id: str
    position_href: str
    replace_id: str
    # "product" | "variant" — тип сущности заменяемой позиции
    replace_entity_type: str = "product"
    # тип целевой сущности (тот же в рамках одной задачи)
    target_entity_type: str = "product"
    quantity: float
    has_target_already: bool = False
    target_position_href: Optional[str] = None
    target_quantity: float = 0.0
    # ссылки на веб-интерфейс МС (из uuidHref в API ответах)
    doc_uuid_href: Optional[str] = None
    replace_product_uuid_href: Optional[str] = None
    target_product_uuid_href: Optional[str] = None


class DeduplicationProcess(BaseModel):
    """Агрегат процесса дедупликации целиком (модель верхнего уровня)."""

    task_id: str
    replace_ids: list[str]
    target_id: str
    status: ProcessStatus = ProcessStatus.PENDING
    progress: ProgressState = Field(default_factory=ProgressState)
    affected: list[AffectedPosition] = Field(default_factory=list)


class JobMeta(BaseModel):
    """Метаданные задачи дедупликации для отображения карточки.

    Сохраняются в Redis сразу при создании процесса (как immediate, так и scheduled).
    Используются при восстановлении карточек после релогина и шедулером при запуске.
    Пароль здесь намеренно НЕ хранится — только публичные поля.
    """

    task_id: str
    login: str
    replace_ids: list[str]
    target_id: str
    entity_type: str = "product"
    cleanup_mode: str = "archive"
    auto_confirm: bool = False
    # epoch_ms запланированного запуска (None для immediate)
    scheduled_at_ms: Optional[int] = None


class ScheduledJob(BaseModel):
    """Полезная нагрузка отложенного запуска в очереди шедулера.

    В отличие от :class:`JobMeta`, содержит пароль — он нужен шедулеру, чтобы
    запустить задачу от имени пользователя в назначенное время. Хранится в Redis
    по ключу ``sched:job:{task_id}`` с ограниченным TTL.
    """

    task_id: str
    login: str
    password: str
    replace_ids: list[str]
    target_id: str
    entity_type: str = "product"
    cleanup_mode: str = "archive"
    auto_confirm: bool = False
    scheduled_at_ms: int
