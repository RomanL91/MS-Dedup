"""Общие функции форматирования для UI.

Сюда вынесены мелкие помощники, нужные нескольким экранам: часовой пояс МСК,
русские названия статусов, сокращение идентификаторов и форматирование времени.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from app.domain.enums import ProcessStatus

MSK_TZ = ZoneInfo("Europe/Moscow")
"""Часовой пояс, в котором пользователь задаёт время запуска (Москва)."""


STATUS_RU: dict[ProcessStatus, str] = {
    ProcessStatus.PENDING: "Ожидание",
    ProcessStatus.SCHEDULED: "Запланирован",
    ProcessStatus.SCANNING: "Сканирование",
    ProcessStatus.CONFIRMING: "Ожидание подтверждения",
    ProcessStatus.REPLACING: "Замена позиций",
    ProcessStatus.DELETING: "Очистка дублей",
    ProcessStatus.DONE: "Готово",
    ProcessStatus.CANCELLED: "Отменено",
    ProcessStatus.ERROR: "Ошибка",
    ProcessStatus.INTERRUPTED: "Прервано перезапуском",
}
"""Человекочитаемые названия статусов процесса для отображения в карточке."""


def short_id(s: str | None, head: int = 8, tail: int = 4) -> str:
    """Сократить длинный идентификатор до вида ``abcdef12…7890`` для компактного UI."""
    if not s:
        return "—"
    if len(s) <= head + tail + 1:
        return s
    return f"{s[:head]}…{s[-tail:]}"


def fmt_msk(epoch_ms: int) -> str:
    """Отформатировать epoch_ms как дату/время в часовом поясе МСК."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=MSK_TZ).strftime(
        "%d.%m.%Y %H:%M"
    )


def ru_status(s: ProcessStatus | str) -> str:
    """Вернуть русское название статуса; при неизвестном значении — его строку."""
    try:
        return STATUS_RU[ProcessStatus(s)]
    except Exception:  # noqa: BLE001
        return str(s)
