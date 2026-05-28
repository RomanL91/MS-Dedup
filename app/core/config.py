"""Конфигурация приложения.

Все настройки читаются из переменных окружения (или файла ``.env``) через
``pydantic-settings``. Единственный экземпляр :data:`settings` импортируется
всеми остальными модулями — это намеренная точка глобальной конфигурации.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Типизированные настройки сервиса.

    Значения по умолчанию рассчитаны на запуск в docker-compose (Redis доступен
    по хосту ``redis``). Любое поле можно переопределить одноимённой переменной
    окружения, см. ``.env.example``.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    REDIS_URL: str = "redis://redis:6379/0"
    FLET_PORT: int = 8080
    MS_API_BASE: str = "https://api.moysklad.ru/api/remap/1.2"

    MAX_PROCESSES: int = 3
    PROGRESS_TTL: int = 3600
    AFFECTED_TTL: int = 3600
    CANCEL_CONFIRM_TTL: int = 3600
    # Heartbeat живого процесса: воркер обновляет alive:{task_id} с этим TTL.
    # Если ключ протух — процесс осиротел (воркер умер) → предложим перезапуск.
    HEARTBEAT_TTL: int = 15

    # Шедулер запланированных задач
    SCHEDULER_INTERVAL_SEC: float = 5.0
    SCHEDULED_JOB_TTL: int = 30 * 24 * 3600  # 30 дней
    JOB_META_TTL: int = 24 * 3600            # 1 день (для восстановления карточек)

    MS_MAX_RETRIES: int = 3
    MS_REQUEST_TIMEOUT: float = 60.0
    # Корзинка МС: 45 единиц / 3 секунды, стоимость единицы зависит от даты
    MS_BUCKET_SIZE: int = 45
    MS_BUCKET_WINDOW_SEC: float = 3.0
    # Параллельные in-flight запросы от одного пользователя (МС: 5)
    MS_MAX_PARALLEL: int = 5
    # Защита от автоотключения: PUT к одной сущности в минуту (МС: 100, берём 90)
    MS_PUT_PER_MIN: int = 90
    MS_PUT_WINDOW_SEC: float = 60.0


settings = Settings()
"""Глобальный экземпляр настроек, используемый во всём приложении."""
