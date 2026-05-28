"""Распределённые ограничители запросов через Redis.

МойСклад накладывает несколько разных ограничений:

* 45 единиц в скользящем окне 3 секунды (стоимость единицы меняется по дате);
* не более 100 PUT к одной сущности в минуту (иначе автоотключение).

Лимитеры распределены через Redis: и worker, и UI используют общую корзинку,
поэтому суммарная нагрузка на API учитывается между всеми процессами.
"""

import asyncio
import datetime as _dt
import time
import uuid
from urllib.parse import urlparse

from redis.asyncio import Redis

BUCKET_KEY = "ms_rate_bucket"
PUT_PREFIX = "ms_put"


def request_cost(now: _dt.datetime | None = None) -> int:
    """Стоимость одного запроса в единицах корзинки 45/3с.

    Согласно политике МС:
      до 12 мая 2026:        1
      12 мая → 1 сен 2026:   2
      1 сен → 1 дек 2026:    3
      с 1 дек 2026:          4
    """
    now = now or _dt.datetime.utcnow()
    if now < _dt.datetime(2026, 5, 12):
        return 1
    if now < _dt.datetime(2026, 9, 1):
        return 2
    if now < _dt.datetime(2026, 12, 1):
        return 3
    return 4


class RedisBucketLimiter:
    """Корзинка `size` единиц в скользящем окне `window` секунд.

    Хранится в Redis ZSET: каждая единица стоимости — отдельная запись
    (score = ts_ms, member = "ts:uuid"). ZCARD возвращает суммарную стоимость.
    """

    def __init__(
        self,
        redis: Redis,
        size: int = 45,
        window: float = 3.0,
        key: str = BUCKET_KEY,
    ) -> None:
        self._redis = redis
        self._size = size
        self._window = window
        self._key = key

    @property
    def size(self) -> int:
        """Полный объём корзинки в единицах стоимости."""
        return self._size

    async def acquire(self) -> None:
        """Дождаться свободного места в корзинке и занять стоимость текущего запроса.

        Блокируется (через ``asyncio.sleep``), пока в скользящем окне не
        освободится достаточно места под стоимость запроса.
        """
        cost = request_cost()
        while True:
            now_ms = int(time.time() * 1000)
            cutoff = now_ms - int(self._window * 1000)
            await self._redis.zremrangebyscore(self._key, "-inf", cutoff)
            used = await self._redis.zcard(self._key)
            if used + cost <= self._size:
                pipe = self._redis.pipeline()
                for _ in range(cost):
                    pipe.zadd(self._key, {f"{now_ms}:{uuid.uuid4().hex}": now_ms})
                pipe.expire(self._key, int(self._window) + 2)
                await pipe.execute()
                return
            # ждём пока самая старая запись выйдет из окна
            oldest = await self._redis.zrange(self._key, 0, 0, withscores=True)
            if oldest:
                _, oldest_score = oldest[0]
                wait_ms = (int(oldest_score) + int(self._window * 1000)) - now_ms
                await asyncio.sleep(max(wait_ms / 1000.0, 0.05))
            else:
                await asyncio.sleep(0.05)

    async def usage(self) -> tuple[int, int]:
        """Вернуть пару ``(использовано_единиц, лимит)`` в текущем окне."""
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - int(self._window * 1000)
        await self._redis.zremrangebyscore(self._key, "-inf", cutoff)
        used = await self._redis.zcard(self._key)
        return int(used), self._size


def _entity_key(url: str) -> str:
    """Ключ сущности — path без query и без trailing slash."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return path


class RedisPutGuard:
    """Не более `max_per_min` PUT-запросов к одной сущности в минуту.

    Защита от автоотключения МС (>100 PUT/мин к одной сущности).
    Ключ — path URL без query.
    """

    def __init__(
        self,
        redis: Redis,
        max_per_min: int = 90,
        window: float = 60.0,
        key_prefix: str = PUT_PREFIX,
    ) -> None:
        self._redis = redis
        self._max = max_per_min
        self._window = window
        self._prefix = key_prefix

    @property
    def max_per_min(self) -> int:
        """Максимально допустимое число PUT к одной сущности за окно."""
        return self._max

    def _key(self, url: str) -> str:
        return f"{self._prefix}:{_entity_key(url)}"

    async def acquire(self, url: str) -> None:
        """Дождаться и занять «слот» PUT для конкретной сущности (по её URL)."""
        key = self._key(url)
        while True:
            now_ms = int(time.time() * 1000)
            cutoff = now_ms - int(self._window * 1000)
            await self._redis.zremrangebyscore(key, "-inf", cutoff)
            used = await self._redis.zcard(key)
            if used + 1 <= self._max:
                pipe = self._redis.pipeline()
                pipe.zadd(key, {f"{now_ms}:{uuid.uuid4().hex}": now_ms})
                pipe.expire(key, int(self._window) + 2)
                await pipe.execute()
                return
            oldest = await self._redis.zrange(key, 0, 0, withscores=True)
            if oldest:
                _, oldest_score = oldest[0]
                wait_ms = (int(oldest_score) + int(self._window * 1000)) - now_ms
                await asyncio.sleep(max(wait_ms / 1000.0, 0.1))
            else:
                await asyncio.sleep(0.1)

    async def max_usage(self) -> tuple[int, int]:
        """Вернуть ``(макс_PUT_в_минуту_среди_всех_сущностей, лимит)``."""
        max_used = 0
        async for key in self._redis.scan_iter(f"{self._prefix}:*", count=200):
            now_ms = int(time.time() * 1000)
            cutoff = now_ms - int(self._window * 1000)
            await self._redis.zremrangebyscore(key, "-inf", cutoff)
            used = await self._redis.zcard(key)
            if used > max_used:
                max_used = int(used)
        return max_used, self._max


async def set_wait_until(redis: Redis, until_ms: int, reason: str) -> None:
    """Установить флаг «ждём от МС» — UI покажет красный индикатор и таймер."""
    await redis.hset(
        "ms_wait",
        mapping={"until_ms": str(until_ms), "reason": reason},
    )
    await redis.expire("ms_wait", 70)


async def get_wait_state(redis: Redis) -> tuple[int, str] | None:
    """Вернуть ``(until_ms, reason)`` если активна пауза ожидания, иначе ``None``."""
    data = await redis.hgetall("ms_wait")
    if not data:
        return None
    until_ms = int(data.get("until_ms", 0) or 0)
    if until_ms <= int(time.time() * 1000):
        return None
    return until_ms, data.get("reason", "")
