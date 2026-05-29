from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ProcessStatus(str, Enum):
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


class ProgressState(BaseModel):
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
    """
    task_id: str
    login: str
    replace_ids: list[str]
    target_id: str
    # Унаследованное поле: тип, когда вся задача однородна. Сохраняется для
    # обратной совместимости и как фолбэк, если target_type/replace_types пусты.
    entity_type: str = "product"
    # Тип целевой сущности. Пусто => использовать entity_type.
    target_type: str = ""
    # Карта тип-каждого-дубля: {id -> "product"|"variant"|"service"|"bundle"}.
    # Пусто => считать, что все дубли имеют тип entity_type.
    replace_types: dict[str, str] = Field(default_factory=dict)
    cleanup_mode: str = "archive"
    auto_confirm: bool = False
    # epoch_ms запланированного запуска (None для immediate)
    scheduled_at_ms: Optional[int] = None

    def effective_target_type(self) -> str:
        return self.target_type or self.entity_type

    def effective_replace_types(self) -> dict[str, str]:
        return {pid: self.replace_types.get(pid, self.entity_type) for pid in self.replace_ids}

    def is_mixed(self) -> bool:
        """True, если в задаче участвует более одного типа сущностей."""
        types = set(self.effective_replace_types().values())
        types.add(self.effective_target_type())
        return len(types) > 1
