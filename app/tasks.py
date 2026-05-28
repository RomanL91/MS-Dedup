import asyncio
import logging
from collections import Counter

from redis.asyncio import Redis

from app.broker import broker
from app.config import settings
from app.models import AffectedPosition, ProcessStatus, ProgressState
from app.ms_client import (
    DOCUMENT_TYPES,
    MSAuthError,
    MSClient,
    MSRequestError,
    build_entity_href,
    extract_id_from_href,
)
from app.progress import (
    is_cancelled,
    is_confirmed,
    set_affected,
    set_progress,
)
from app.scheduler import alive_key, touch_alive
from app.utils import DOC_TYPE_NAMES, ENTITY_TYPE_NAMES_PLURAL, ENTITY_TYPES

log = logging.getLogger(__name__)


async def _heartbeat_loop(redis: Redis, task_id: str) -> None:
    """Периодически обновляет heartbeat процесса, пока задача жива.

    При смерти воркера корутина исчезает вместе с процессом, ключ alive:{id}
    протухает по TTL — так UI понимает, что процесс осиротел, и предлагает рестарт.
    """
    interval = max(1, settings.HEARTBEAT_TTL // 3)
    while True:
        try:
            await touch_alive(redis, task_id)
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(interval)


async def _count_documents(client: MSClient) -> tuple[int, dict[str, int]]:
    """Получить общее количество документов всех типов (для прогресса SCANNING)."""
    per_type: dict[str, int] = {}
    total = 0
    for doc_type in DOCUMENT_TYPES:
        try:
            page = await client.get_documents_page(doc_type, limit=1, offset=0)
            size = int(page.get("meta", {}).get("size", 0) or 0)
            per_type[doc_type] = size
            total += size
        except MSRequestError as e:
            log.warning("Cannot count %s: %s", doc_type, e)
            per_type[doc_type] = 0
    return total, per_type


def _extract_positions_rows(doc: dict) -> tuple[list[dict], dict | None]:
    positions = doc.get("positions") or {}
    rows = positions.get("rows") or []
    meta = positions.get("meta") or {}
    return rows, meta if meta else None


async def _scan_document(
    client: MSClient,
    doc: dict,
    doc_type: str,
    replace_ids_set: set[str],
    target_id: str,
    entity_type: str,
) -> list[AffectedPosition]:
    rows, positions_meta = _extract_positions_rows(doc)
    # Если expand=positions не вернул rows, или вернул меньше чем size,
    # принудительно подгружаем позиции через positions.meta.href
    need_fetch = positions_meta is not None and (
        not rows or int(positions_meta.get("size", 0) or 0) > len(rows)
    )
    if need_fetch:
        try:
            fetched = await client.get_all_positions(positions_meta)
            if fetched:
                rows = fetched
        except MSRequestError as e:
            log.warning(
                "Failed to fetch positions for %s/%s: %s", doc_type, doc.get("id"), e
            )

    target_pos: dict | None = None
    replace_positions: list[tuple[str, dict]] = []
    for p in rows:
        assortment_meta = (p.get("assortment") or {}).get("meta") or {}
        href = assortment_meta.get("href") or ""
        pos_entity_type = assortment_meta.get("type") or ""
        # фильтруем позиции по типу сущности (product vs variant)
        if not href or pos_entity_type != entity_type:
            continue
        pid = extract_id_from_href(href)
        if pid == target_id and target_pos is None:
            target_pos = p
        elif pid in replace_ids_set:
            replace_positions.append((pid, p))

    if not replace_positions:
        return []

    log.info(
        "Match in %s/%s (%s): %d replace positions, target_present=%s",
        doc_type,
        doc.get("id"),
        doc.get("name") or "—",
        len(replace_positions),
        target_pos is not None,
    )

    target_position_href = None
    target_quantity = 0.0
    target_product_uuid_href: str | None = None
    if target_pos is not None:
        target_position_href = (target_pos.get("meta") or {}).get("href")
        target_quantity = float(target_pos.get("quantity") or 0)
        target_assortment_meta = (target_pos.get("assortment") or {}).get("meta") or {}
        target_product_uuid_href = target_assortment_meta.get("uuidHref")

    doc_uuid_href = (doc.get("meta") or {}).get("uuidHref")

    affected: list[AffectedPosition] = []
    for replace_id, p in replace_positions:
        replace_assortment_meta = (p.get("assortment") or {}).get("meta") or {}
        affected.append(
            AffectedPosition(
                doc_type=doc_type,
                doc_id=doc.get("id", ""),
                doc_name=doc.get("name") or "",
                position_id=p.get("id", ""),
                position_href=(p.get("meta") or {}).get("href", ""),
                replace_id=replace_id,
                replace_entity_type=entity_type,
                target_entity_type=entity_type,
                quantity=float(p.get("quantity") or 0),
                has_target_already=target_pos is not None,
                target_position_href=target_position_href,
                target_quantity=target_quantity,
                doc_uuid_href=doc_uuid_href,
                replace_product_uuid_href=replace_assortment_meta.get("uuidHref"),
                target_product_uuid_href=target_product_uuid_href,
            )
        )
    return affected


async def _phase_scan(
    client: MSClient,
    redis: Redis,
    task_id: str,
    replace_ids: list[str],
    target_id: str,
    entity_type: str,
) -> list[AffectedPosition] | None:
    """Возвращает список затронутых позиций, либо None если процесс отменён."""
    replace_ids_set = set(replace_ids)

    await set_progress(
        redis,
        task_id,
        ProgressState(status=ProcessStatus.SCANNING, message="Подсчёт документов..."),
    )
    total_docs, _per_type = await _count_documents(client)

    state = ProgressState(
        status=ProcessStatus.SCANNING,
        current=0,
        total=total_docs,
        message=f"Сканирование документов (0/{total_docs})",
    )
    await set_progress(redis, task_id, state)

    affected: list[AffectedPosition] = []
    scanned = 0

    for doc_type in DOCUMENT_TYPES:
        offset = 0
        while True:
            if await is_cancelled(redis, task_id):
                return None
            try:
                page = await client.get_documents_page(
                    doc_type, limit=100, offset=offset
                )
            except MSRequestError as e:
                log.warning(
                    "Skipping %s offset=%s due to error: %s", doc_type, offset, e
                )
                break

            rows = page.get("rows") or []
            if not rows:
                break

            for doc in rows:
                doc_affected = await _scan_document(
                    client, doc, doc_type, replace_ids_set, target_id, entity_type
                )
                affected.extend(doc_affected)

            scanned += len(rows)
            doc_type_ru = DOC_TYPE_NAMES.get(doc_type, doc_type)
            state = ProgressState(
                status=ProcessStatus.SCANNING,
                current=scanned,
                total=max(total_docs, scanned),
                message=(
                    f"Сканирование: {doc_type_ru} "
                    f"({scanned}/{max(total_docs, scanned)})"
                ),
            )
            await set_progress(redis, task_id, state)

            if len(rows) < 100:
                break
            offset += 100

    return affected


async def _phase_replace(
    client: MSClient,
    redis: Redis,
    task_id: str,
    affected: list[AffectedPosition],
    target_id: str,
    entity_type: str,
) -> tuple[int, set[str], bool]:
    """Возвращает (успешных_замен, id_сущностей_с_ошибкой_замены, был_ли_отменён).

    Если хотя бы одна позиция сущности не заменилась, её replace_id попадает в
    failed_ids — такую сущность нельзя архивировать/удалять (ссылка ещё жива).
    """
    target_href = build_entity_href(entity_type, target_id, client.base_url)
    total = len(affected)
    replaced = 0
    failed_ids: set[str] = set()
    target_qty_state: dict[str, float] = {}

    state = ProgressState(
        status=ProcessStatus.REPLACING,
        current=0,
        total=total,
        message=f"Замена позиций (0/{total})",
    )
    await set_progress(redis, task_id, state)

    for idx, ap in enumerate(affected, start=1):
        if await is_cancelled(redis, task_id):
            return replaced, failed_ids, True
        try:
            if ap.has_target_already and ap.target_position_href:
                key = ap.target_position_href
                if key not in target_qty_state:
                    target_qty_state[key] = ap.target_quantity
                new_qty = target_qty_state[key] + ap.quantity
                target_qty_state[key] = new_qty
                await client.merge_positions(
                    doc_type=ap.doc_type,
                    doc_id=ap.doc_id,
                    keep_position_href=ap.target_position_href,
                    remove_position_href=ap.position_href,
                    merged_quantity=new_qty,
                )
            else:
                await client.update_position(
                    ap.position_href, target_href, entity_type
                )
            replaced += 1
        except MSRequestError as e:
            failed_ids.add(ap.replace_id)
            log.warning(
                "Failed to replace position %s in %s/%s: %s",
                ap.position_id,
                ap.doc_type,
                ap.doc_id,
                e,
            )

        state = ProgressState(
            status=ProcessStatus.REPLACING,
            current=idx,
            total=total,
            message=f"Замена позиций ({idx}/{total})",
            replaced_count=replaced,
        )
        await set_progress(redis, task_id, state)

    return replaced, failed_ids, False


CLEANUP_MODES = ("archive", "delete", "none")


async def _phase_delete(
    client: MSClient,
    redis: Redis,
    task_id: str,
    replace_ids: list[str],
    replaced_count: int,
    entity_type: str,
    cleanup_mode: str,
) -> tuple[int, list[str], bool]:
    """Финальная фаза: архивация или удаление дублей.

    cleanup_mode = "archive" — PUT {"archived": true} (по умолчанию).
    cleanup_mode = "delete"  — DELETE /entity/{type}/{id}.
    """
    total = len(replace_ids)
    deleted = 0
    errors: list[str] = []
    entity_label = ENTITY_TYPES.get(entity_type, entity_type)
    entity_label_plural = ENTITY_TYPE_NAMES_PLURAL.get(entity_type, "товаров")
    is_archive = cleanup_mode == "archive"
    verb_gerund = "Архивирование" if is_archive else "Удаление"
    verb_past = "не архивирован" if is_archive else "не удалён"

    state = ProgressState(
        status=ProcessStatus.DELETING,
        current=0,
        total=total,
        message=f"{verb_gerund} {entity_label_plural} (0/{total})",
        replaced_count=replaced_count,
    )
    await set_progress(redis, task_id, state)

    for idx, pid in enumerate(replace_ids, start=1):
        if await is_cancelled(redis, task_id):
            return deleted, errors, True
        try:
            if is_archive:
                await client.archive_entity(entity_type, pid)
            else:
                await client.delete_entity(entity_type, pid)
            deleted += 1
        except MSRequestError as e:
            msg = f"{entity_label} {pid} {verb_past}: {e}"
            log.warning(msg)
            errors.append(msg)

        state = ProgressState(
            status=ProcessStatus.DELETING,
            current=idx,
            total=total,
            message=f"{verb_gerund} {entity_label_plural} ({idx}/{total})",
            replaced_count=replaced_count,
            deleted_count=deleted,
            delete_errors=errors,
        )
        await set_progress(redis, task_id, state)

    return deleted, errors, False


@broker.task
async def run_deduplication(
    task_id: str,
    replace_ids: list[str],
    target_id: str,
    login: str,
    password: str,
    entity_type: str = "product",
    cleanup_mode: str = "archive",
    auto_confirm: bool = False,
) -> None:
    if entity_type not in ENTITY_TYPES:
        entity_type = "product"
    if cleanup_mode not in CLEANUP_MODES:
        cleanup_mode = "archive"
    redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    hb_task = asyncio.create_task(_heartbeat_loop(redis, task_id))
    try:
        async with MSClient(login=login, password=password, redis=redis) as client:
            try:
                # Нормализация ID: у МС сущности могут быть UI id (из URL веб-интерфейса)
                # и API id (из meta.href) — это разные значения. Везде дальше нужен API id.
                resolved_replace: list[str] = []
                for pid in replace_ids:
                    try:
                        real = await client.resolve_entity_id(entity_type, pid)
                    except MSRequestError:
                        real = pid
                    if real != pid:
                        log.info("Resolved replace id %s -> %s", pid, real)
                    resolved_replace.append(real)
                replace_ids = resolved_replace

                try:
                    real_target = await client.resolve_entity_id(entity_type, target_id)
                except MSRequestError:
                    real_target = target_id
                if real_target != target_id:
                    log.info("Resolved target id %s -> %s", target_id, real_target)
                target_id = real_target

                # фаза 1: SCANNING
                affected = await _phase_scan(
                    client, redis, task_id, replace_ids, target_id, entity_type
                )
                if affected is None:
                    await set_progress(
                        redis,
                        task_id,
                        ProgressState(
                            status=ProcessStatus.CANCELLED,
                            message="Отменено пользователем",
                        ),
                    )
                    return

                # сохраняем найденные позиции — даже при auto_confirm UI отрисует
                # карту замен, чтобы пользователь видел что делает процесс
                await set_affected(redis, task_id, affected)
                type_counts = Counter(ap.doc_type for ap in affected)
                summary = (
                    ", ".join(
                        f"{DOC_TYPE_NAMES.get(t, t)}: {c}"
                        for t, c in type_counts.most_common()
                    )
                    or "—"
                )
                doc_ids = {(ap.doc_type, ap.doc_id) for ap in affected}

                if auto_confirm or not affected:
                    # пропускаем CONFIRMING — сразу в REPLACING
                    info_message = (
                        f"Найдено {len(affected)} позиций в {len(doc_ids)} документах. "
                        f"Типы: {summary}. Запуск без подтверждения..."
                    )
                    await set_progress(
                        redis,
                        task_id,
                        ProgressState(
                            status=ProcessStatus.REPLACING,
                            current=0,
                            total=len(affected),
                            message=info_message,
                        ),
                    )
                else:
                    confirm_message = (
                        f"Найдено {len(affected)} позиций в {len(doc_ids)} документах. "
                        f"Типы: {summary}. Ожидание подтверждения..."
                    )
                    await set_progress(
                        redis,
                        task_id,
                        ProgressState(
                            status=ProcessStatus.CONFIRMING,
                            current=0,
                            total=len(affected),
                            message=confirm_message,
                        ),
                    )
                    while True:
                        if await is_cancelled(redis, task_id):
                            await set_progress(
                                redis,
                                task_id,
                                ProgressState(
                                    status=ProcessStatus.CANCELLED,
                                    message="Отменено пользователем",
                                ),
                            )
                            return
                        if await is_confirmed(redis, task_id):
                            break
                        await asyncio.sleep(2)

                # фаза 2: REPLACING
                replaced, failed_ids, was_cancelled = await _phase_replace(
                    client, redis, task_id, affected, target_id, entity_type
                )
                if was_cancelled:
                    await set_progress(
                        redis,
                        task_id,
                        ProgressState(
                            status=ProcessStatus.CANCELLED,
                            message=f"Отменено пользователем. Заменено: {replaced}",
                            replaced_count=replaced,
                        ),
                    )
                    return

                # Сущности, у которых не прошла замена хотя бы одной позиции, НЕ
                # архивируем/не удаляем — иначе документ останется со ссылкой на
                # архивный/удалённый дубль. Чистим только полностью заменённые.
                to_cleanup = [pid for pid in replace_ids if pid not in failed_ids]
                skipped = [pid for pid in replace_ids if pid in failed_ids]

                entity_label_plural = ENTITY_TYPE_NAMES_PLURAL.get(
                    entity_type, "товаров"
                )

                # режим "none": дубли не трогаем — только замена в документах
                if cleanup_mode == "none":
                    final_message = (
                        f"Готово. Заменено позиций: {replaced}. "
                        f"Дубли ({entity_label_plural}) оставлены без изменений."
                    )
                    if skipped:
                        final_message += (
                            f" Замена прошла не полностью у {len(skipped)} — "
                            "см. лог воркера."
                        )
                    await set_progress(
                        redis,
                        task_id,
                        ProgressState(
                            status=ProcessStatus.DONE,
                            current=replaced,
                            total=replaced,
                            message=final_message,
                            replaced_count=replaced,
                        ),
                    )
                    return

                cleanup_verb_past = (
                    "архивировано" if cleanup_mode == "archive" else "удалено"
                )
                cleanup_action_noun = (
                    "архивации" if cleanup_mode == "archive" else "удалении"
                )
                cleanup_noun_genitive = (
                    "архивации" if cleanup_mode == "archive" else "удаления"
                )

                # фаза 3: CLEANUP (архивация или удаление) — только успешно заменённые
                deleted, errors, was_cancelled = await _phase_delete(
                    client, redis, task_id, to_cleanup, replaced,
                    entity_type, cleanup_mode,
                )
                if was_cancelled:
                    await set_progress(
                        redis,
                        task_id,
                        ProgressState(
                            status=ProcessStatus.CANCELLED,
                            message=(
                                f"Отменено пользователем. Заменено: {replaced}, "
                                f"{cleanup_verb_past} {entity_label_plural}: {deleted}"
                            ),
                            replaced_count=replaced,
                            deleted_count=deleted,
                            delete_errors=errors,
                            skipped_ids=skipped,
                        ),
                    )
                    return

                final_message = (
                    f"Готово. Заменено позиций: {replaced}, "
                    f"{cleanup_verb_past} {entity_label_plural}: {deleted}."
                )
                if skipped:
                    final_message += (
                        f" Замена не прошла полностью, поэтому оставлено без "
                        f"{cleanup_noun_genitive}: {len(skipped)}."
                    )
                if errors:
                    final_message += (
                        f" Ошибок при {cleanup_action_noun}: {len(errors)}."
                    )
                await set_progress(
                    redis,
                    task_id,
                    ProgressState(
                        status=ProcessStatus.DONE,
                        current=len(to_cleanup),
                        total=len(to_cleanup),
                        message=final_message,
                        replaced_count=replaced,
                        deleted_count=deleted,
                        delete_errors=errors,
                        skipped_ids=skipped,
                    ),
                )

            except MSAuthError as e:
                log.exception("Auth error in task %s", task_id)
                await set_progress(
                    redis,
                    task_id,
                    ProgressState(
                        status=ProcessStatus.ERROR,
                        message="Ошибка авторизации",
                        error=str(e),
                    ),
                )
            except MSRequestError as e:
                log.exception("Request error in task %s", task_id)
                await set_progress(
                    redis,
                    task_id,
                    ProgressState(
                        status=ProcessStatus.ERROR,
                        message="Ошибка запроса к МойСклад",
                        error=str(e),
                    ),
                )
            except Exception as e:  # noqa: BLE001
                log.exception("Unexpected error in task %s", task_id)
                await set_progress(
                    redis,
                    task_id,
                    ProgressState(
                        status=ProcessStatus.ERROR,
                        message="Неожиданная ошибка",
                        error=str(e),
                    ),
                )
    finally:
        hb_task.cancel()
        try:
            await redis.delete(alive_key(task_id))
        except Exception:  # noqa: BLE001
            pass
        await redis.aclose()
