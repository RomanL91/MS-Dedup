import asyncio
import logging
import uuid
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import flet as ft
from redis.asyncio import Redis

from app.broker import broker
from app.config import settings
from app.models import (
    TERMINAL_STATUSES,
    JobMeta,
    ProcessStatus,
    ProgressState,
)
from app.ms_client import MSAuthError, MSClient, MSRequestError
from app.limiter import RedisBucketLimiter, RedisPutGuard, get_wait_state
from app.progress import (
    cleanup_process,
    get_affected,
    get_progress,
    request_cancel,
    request_confirm,
    set_progress,
)
from app.scheduler import (
    alive_key,
    enqueue_scheduled,
    forget_user_job,
    get_job_meta,
    get_scheduled_at_ms,
    is_alive,
    list_user_jobs,
    remove_scheduled,
    save_job_meta,
    scheduler_loop,
)
from app.tasks import run_deduplication
from app.utils import (
    DOC_TYPE_NAMES,
    ENTITY_TYPE_GROUP_NAMES,
    ENTITY_TYPE_TARGET_NAMES,
    ENTITY_TYPES,
    detect_entity_type,
    extract_uuid,
    extract_uuids,
)

MSK_TZ = ZoneInfo("Europe/Moscow")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger(__name__)


STATUS_RU = {
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

def _short_id(s: str | None, head: int = 8, tail: int = 4) -> str:
    if not s:
        return "—"
    if len(s) <= head + tail + 1:
        return s
    return f"{s[:head]}…{s[-tail:]}"


_broker_started_lock = asyncio.Lock()
_broker_started = False
_scheduler_task: asyncio.Task | None = None


def _fmt_msk(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=MSK_TZ).strftime("%d.%m.%Y %H:%M")


def _ru_status(s: ProcessStatus | str) -> str:
    try:
        return STATUS_RU[ProcessStatus(s)]
    except Exception:  # noqa: BLE001
        return str(s)


async def _ensure_broker_started() -> None:
    global _broker_started, _scheduler_task
    async with _broker_started_lock:
        if not _broker_started:
            await broker.startup()
            _broker_started = True
        if _scheduler_task is None or _scheduler_task.done():
            _scheduler_task = asyncio.create_task(
                scheduler_loop(settings.REDIS_URL),
                name="dedup-scheduler-loop",
            )


async def _check_redis() -> bool:
    try:
        r = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        await r.ping()
        await r.aclose()
        return True
    except Exception as e:  # noqa: BLE001
        log.error("Redis is not available: %s", e)
        return False


async def main(page: ft.Page):
    page.title = "MS Dedup — Дедупликация товаров МойСклад"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 20
    page.scroll = ft.ScrollMode.AUTO

    if not await _check_redis():
        page.controls.clear()
        page.add(
            ft.Container(
                content=ft.Column(
                    [
                        ft.Text(
                            "Ошибка подключения к Redis",
                            size=20,
                            weight=ft.FontWeight.BOLD,
                            color=ft.Colors.RED,
                        ),
                        ft.Text(f"REDIS_URL={settings.REDIS_URL}"),
                        ft.Text("Проверьте что Redis запущен и доступен."),
                    ],
                    spacing=10,
                ),
                padding=20,
            )
        )
        page.update()
        return

    await _ensure_broker_started()

    page.session.set("active_processes", {})
    await _show_login(page)


# ---------- LOGIN ----------


async def _show_login(page: ft.Page) -> None:
    login_field = ft.TextField(label="Логин МойСклад", width=320, autofocus=True)
    password_field = ft.TextField(
        label="Пароль", width=320, password=True, can_reveal_password=True
    )
    error_text = ft.Text("", color=ft.Colors.RED)
    login_btn = ft.ElevatedButton("Войти", width=320)

    async def on_login_click(_):
        login = (login_field.value or "").strip()
        password = password_field.value or ""
        if not login or not password:
            error_text.value = "Введите логин и пароль"
            page.update()
            return
        error_text.value = ""
        login_btn.disabled = True
        login_btn.text = "Проверка..."
        page.update()

        ok = False
        err: str | None = None
        redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        try:
            async with MSClient(
                login=login, password=password, redis=redis_client
            ) as client:
                ok = await client.check_auth()
        except MSAuthError:
            ok = False
        except MSRequestError as e:
            err = f"Ошибка соединения: {e}"
        finally:
            await redis_client.aclose()

        if err:
            error_text.value = err
        elif not ok:
            error_text.value = "Неверный логин или пароль"
        else:
            page.session.set("login", login)
            page.session.set("password", password)
            await _show_main_screen(page)
            return

        login_btn.disabled = False
        login_btn.text = "Войти"
        page.update()

    login_btn.on_click = on_login_click

    page.controls.clear()
    page.add(
        ft.Container(
            content=ft.Column(
                [
                    ft.Text("MS Dedup", size=28, weight=ft.FontWeight.BOLD),
                    ft.Text(
                        "Дедупликация товаров МойСклад", size=14, color=ft.Colors.GREY
                    ),
                    ft.Container(height=20),
                    login_field,
                    password_field,
                    login_btn,
                    error_text,
                ],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=10,
            ),
            alignment=ft.alignment.center,
            padding=40,
        )
    )
    page.update()


# ---------- MAIN SCREEN ----------


async def _show_main_screen(page: ft.Page) -> None:
    processes_column = ft.Column(spacing=12)
    new_process_btn = ft.ElevatedButton("Новый процесс", icon=ft.Icons.ADD)

    def refresh_new_btn():
        active = page.session.get("active_processes") or {}
        new_process_btn.disabled = len(active) >= settings.MAX_PROCESSES
        new_process_btn.tooltip = (
            f"Максимум {settings.MAX_PROCESSES} активных процессов"
            if new_process_btn.disabled
            else None
        )

    async def on_new_process(_):
        await _open_new_process_dialog(page, processes_column, refresh_new_btn)

    new_process_btn.on_click = on_new_process
    refresh_new_btn()

    help_btn = ft.OutlinedButton(
        "Инструкция",
        icon=ft.Icons.HELP_OUTLINE,
        on_click=lambda _: _open_help_dialog(page),
    )

    # виджет статуса API
    rate_indicator = _make_rate_indicator()
    put_indicator = _make_put_indicator()
    wait_text = ft.Text("", size=11, color=ft.Colors.RED)

    status_polling_task = page.run_task(
        _poll_api_status, page, rate_indicator, put_indicator, wait_text
    )

    async def on_logout(_):
        active = page.session.get("active_processes") or {}
        for info in list(active.values()):
            t = info.get("polling_task")
            if t and not t.done():
                t.cancel()
        if status_polling_task and not status_polling_task.done():
            status_polling_task.cancel()
        page.overlay.clear()
        page.session.remove("active_processes")
        page.session.remove("login")
        page.session.remove("password")
        page.session.set("active_processes", {})
        await _show_login(page)

    header = ft.Row(
        [
            ft.Text("MS Dedup", size=22, weight=ft.FontWeight.BOLD),
            ft.Container(width=12),
            rate_indicator["row"],
            ft.Container(width=8),
            put_indicator["row"],
            ft.Container(width=8),
            wait_text,
            ft.Container(expand=True),
            ft.Text(f"Пользователь: {page.session.get('login')}", color=ft.Colors.GREY),
            ft.TextButton("Выйти", icon=ft.Icons.LOGOUT, on_click=on_logout),
        ],
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    page.controls.clear()
    page.add(
        header,
        ft.Divider(),
        ft.Row([new_process_btn, help_btn], spacing=8),
        ft.Container(height=10),
        processes_column,
    )
    page.update()

    # Восстановить карточки нетерминальных процессов в фоне — не блокируем
    # обработчик логина (иначе разрыв ws во время restore валит handler)
    page.run_task(_restore_user_cards, page, processes_column, refresh_new_btn)


# ---------- API STATUS WIDGET ----------


def _make_rate_indicator() -> dict:
    bar = ft.ProgressBar(
        width=100, value=0, color=ft.Colors.GREEN, bgcolor=ft.Colors.GREY_200
    )
    label = ft.Text("0/0", size=11)
    dot = ft.Text("🟢", size=14)
    row = ft.Row(
        [dot, ft.Text("Запросы", size=11, color=ft.Colors.GREY), bar, label],
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
        spacing=4,
        tight=True,
    )
    return {"row": row, "bar": bar, "label": label, "dot": dot}


def _make_put_indicator() -> dict:
    bar = ft.ProgressBar(
        width=100, value=0, color=ft.Colors.GREEN, bgcolor=ft.Colors.GREY_200
    )
    label = ft.Text("0/0", size=11)
    dot = ft.Text("🟢", size=14)
    row = ft.Row(
        [dot, ft.Text("PUT", size=11, color=ft.Colors.GREY), bar, label],
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
        spacing=4,
        tight=True,
    )
    return {"row": row, "bar": bar, "label": label, "dot": dot}


def _update_indicator(ind: dict, used: int, total: int) -> None:
    ind["label"].value = f"{used}/{total}"
    if total > 0:
        frac = min(1.0, used / total)
    else:
        frac = 0.0
    ind["bar"].value = frac
    if frac >= 0.9:
        ind["bar"].color = ft.Colors.RED
        ind["dot"].value = "🔴"
    elif frac >= 0.7:
        ind["bar"].color = ft.Colors.ORANGE
        ind["dot"].value = "🟡"
    else:
        ind["bar"].color = ft.Colors.GREEN
        ind["dot"].value = "🟢"


async def _poll_api_status(
    page: ft.Page,
    rate_ind: dict,
    put_ind: dict,
    wait_text: ft.Text,
) -> None:
    import time as _time

    redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    limiter = RedisBucketLimiter(
        redis, size=settings.MS_BUCKET_SIZE, window=settings.MS_BUCKET_WINDOW_SEC
    )
    put_guard = RedisPutGuard(
        redis, max_per_min=settings.MS_PUT_PER_MIN, window=settings.MS_PUT_WINDOW_SEC
    )
    try:
        while True:
            try:
                rate_used, rate_total = await limiter.usage()
                put_used, put_total = await put_guard.max_usage()
                wait = await get_wait_state(redis)
            except Exception as e:  # noqa: BLE001
                log.debug("Status poll error: %s", e)
                await asyncio.sleep(2)
                continue

            _update_indicator(rate_ind, rate_used, rate_total)
            _update_indicator(put_ind, put_used, put_total)

            if wait:
                until_ms, reason = wait
                remaining = max(0, (until_ms - int(_time.time() * 1000)) / 1000.0)
                wait_text.value = f"🔴 Пауза: {remaining:.1f}с ({reason})"
            else:
                wait_text.value = ""

            try:
                page.update()
            except Exception:  # noqa: BLE001
                return

            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        return
    finally:
        await redis.aclose()


# ---------- HELP DIALOG ----------


def _open_help_dialog(page: ft.Page) -> None:
    """Диалог-справка: что делает сервис, фазы, действия, статусы, ограничения."""

    def section(title: str, body: str) -> ft.Control:
        return ft.Column(
            [
                ft.Text(title, weight=ft.FontWeight.BOLD, size=14),
                ft.Text(body, size=12),
            ],
            spacing=2,
            tight=True,
        )

    content = ft.Column(
        [
            section(
                "Что делает сервис",
                "MS Dedup устраняет дубли сущностей в МойСклад. Вы указываете один "
                "или несколько дублей и одну целевую сущность. Сервис находит все "
                "документы, где встречаются дубли, заменяет их в позициях на целевую "
                "сущность, а затем (по вашему выбору) архивирует, удаляет или "
                "оставляет дубли.",
            ),
            ft.Divider(height=1),
            section(
                "Что можно дедуплицировать",
                "Товары, Услуги, Модификации, Комплекты. В одном процессе — только "
                "один тип, смешивать нельзя. ID можно вводить как UUID, ссылку из "
                "админки МС (#good / #feature / #bundle) или прямую API-ссылку. "
                "Тип Товар/Услуга по ссылке #good определяется автоматически.",
            ),
            ft.Divider(height=1),
            section(
                "Где выполняется замена",
                "15 типов документов: Заказы покупателя и поставщику, Счета "
                "покупателю и поставщика, Отгрузка, Приёмка, Возвраты покупателя и "
                "поставщику, Списание, Оприходование, Перемещение, Внутренний заказ, "
                "Розничные продажи и возвраты, Инвентаризация.",
            ),
            ft.Divider(height=1),
            section(
                "Как идёт процесс",
                "1. Сканирование — поиск позиций с дублями во всех документах.\n"
                "2. Ожидание подтверждения — показывается карта замен (если не "
                "включён запуск без подтверждения).\n"
                "3. Замена позиций — дубли в документах заменяются на целевую "
                "сущность. Если в одном документе есть и дубль, и целевая сущность — "
                "их количества объединяются.\n"
                "4. Очистка — дубли архивируются или удаляются (или остаются, если "
                "выбрано «Ничего»).",
            ),
            ft.Divider(height=1),
            section(
                "Действия с дублями",
                "• Архивировать (по умолчанию) — обратимо, дубли уходят в архив.\n"
                "• Удалить — безвозвратно; не пройдёт, если сущность ещё где-то "
                "используется (вернётся ошибка).\n"
                "• Ничего — только замена в документах, сами дубли не трогаются.\n"
                "Важно: если замена хотя бы одной позиции не прошла, такая сущность "
                "НЕ архивируется и не удаляется — вы увидите её среди пропущенных.",
            ),
            ft.Divider(height=1),
            section(
                "Статусы карточек",
                "• Ожидание — поставлен в очередь.\n"
                "• Запланирован — стартует в назначенное время.\n"
                "• Сканирование / Замена позиций / Очистка дублей — идёт работа.\n"
                "• Ожидание подтверждения — нужно нажать «Подтвердить замену».\n"
                "• Готово — успешно завершён (зелёная карточка).\n"
                "• Отменено — остановлен пользователем (серая).\n"
                "• Ошибка — что-то пошло не так (красная).\n"
                "• Прервано перезапуском — сервис перезапускали во время работы; "
                "нажмите «Перезапустить сначала» или «Убрать» (оранжевая).",
            ),
            ft.Divider(height=1),
            section(
                "Ограничения",
                "• Один тип сущностей на процесс.\n"
                "• До 3 одновременных процессов.\n"
                "• Комплекты бывают в позициях только у Заказа покупателя и "
                "Отгрузки; в Инвентаризации — только Товары и Модификации.\n"
                "• Сервис смотрит только позиции документов: дубль, входящий в "
                "комплект как компонент, там не заменяется.\n"
                "• Соблюдаются лимиты МойСклад — индикаторы «Запросы» и «PUT» в "
                "шапке показывают текущую нагрузку.",
            ),
            ft.Divider(height=1),
            section(
                "Сохранение и восстановление",
                "Процессы хранятся в Redis и переживают перелогин и перезапуск "
                "сервиса. После входа незавершённые карточки восстанавливаются "
                "автоматически (завершённые — Готово/Отменено/Ошибка — не "
                "показываются). Запланированные задачи запускаются по времени, даже "
                "если вы выходили из приложения.",
            ),
        ],
        spacing=10,
        tight=True,
        scroll=ft.ScrollMode.AUTO,
    )

    dialog = ft.AlertDialog(
        modal=True,
        title=ft.Text("Инструкция — MS Dedup"),
        content=ft.Container(content=content, width=600, height=640),
        actions=[ft.TextButton("Закрыть", on_click=lambda e: page.close(dialog))],
        actions_alignment=ft.MainAxisAlignment.END,
    )
    page.open(dialog)


# ---------- NEW PROCESS DIALOG ----------


async def _open_new_process_dialog(
    page: ft.Page,
    processes_column: ft.Column,
    refresh_new_btn,
) -> None:
    PLACEHOLDER = "UUID или ссылка из МойСклад"

    # row.data = {"detected_type": "product" | "variant" | None}
    replace_inputs_column = ft.Column(spacing=6, tight=True)
    target_field = ft.TextField(
        hint_text=PLACEHOLDER,
        width=520,
        dense=True,
    )
    target_type_label = ft.Text("", size=11)
    target_state: dict = {"detected_type": None}
    error_text = ft.Text("", color=ft.Colors.RED, selectable=True)
    run_btn = ft.ElevatedButton("Запустить")
    cancel_btn = ft.TextButton("Отмена")

    def style_field_valid(field: ft.TextField, valid: bool):
        if not (field.value or "").strip():
            field.border_color = None
            field.helper_text = None
            return
        if valid:
            field.border_color = ft.Colors.GREEN
            field.helper_text = None
        else:
            field.border_color = ft.Colors.RED
            field.helper_text = "UUID не найден"

    def render_type_label(label: ft.Text, entity_type: str | None, has_value: bool):
        if not has_value:
            label.value = ""
            return
        name = ENTITY_TYPES.get(entity_type or "")
        if name:
            label.value = f"✅ {name}"
            label.color = {
                "product": ft.Colors.GREEN,
                "variant": ft.Colors.BLUE,
                "service": ft.Colors.PURPLE,
                "bundle": ft.Colors.ORANGE,
            }.get(entity_type, ft.Colors.GREY)
        else:
            label.value = "тип определится при проверке"
            label.color = ft.Colors.GREY

    # --- Живое определение типа с дебаунсом 3с ---
    # Для #feature/#bundle/API-ссылок тип известен сразу (без сети). Для
    # неоднозначных (#good или голый UUID) спрашиваем МС, но не на каждый
    # символ, а через 3с после последнего изменения поля.
    probe_tasks: dict = {}  # ключ поля -> asyncio.Task

    async def _probe_type(uid: str) -> str | None:
        login = page.session.get("login")
        password = page.session.get("password")
        redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        try:
            async with MSClient(
                login=login, password=password, redis=redis_client
            ) as client:
                return await client.resolve_entity_type(uid)
        except Exception as e:  # noqa: BLE001
            log.debug("type probe failed for %s: %s", uid, e)
            return None
        finally:
            try:
                await redis_client.aclose()
            except BaseException:  # noqa: BLE001
                pass

    def _cancel_probe(key) -> None:
        t = probe_tasks.pop(key, None)
        if t and not t.done():
            t.cancel()

    def _schedule_probe(key, value_getter, label: ft.Text, state_setter) -> None:
        _cancel_probe(key)
        uid_now = extract_uuid(value_getter() or "")
        if not uid_now:
            return
        label.value = "Определяем тип…"
        label.color = ft.Colors.GREY

        async def _run(uid_start: str):
            try:
                await asyncio.sleep(3.0)
            except asyncio.CancelledError:
                return
            et = await _probe_type(uid_start)
            # применяем результат, только если поле не изменилось за время паузы
            if extract_uuid(value_getter() or "") != uid_start:
                return
            state_setter(et)
            render_type_label(label, et, True)
            try:
                page.update()
            except Exception:  # noqa: BLE001
                pass

        probe_tasks[key] = page.run_task(_run, uid_now)

    def on_target_change(_):
        raw = target_field.value or ""
        uid = extract_uuid(raw)
        style_field_valid(target_field, uid is not None)
        et = detect_entity_type(raw) if uid else None
        target_state["detected_type"] = et
        render_type_label(target_type_label, et, bool(uid))
        if uid and et is None:
            _schedule_probe(
                "target",
                lambda: target_field.value,
                target_type_label,
                lambda x: target_state.__setitem__("detected_type", x),
            )
        else:
            _cancel_probe("target")
        page.update()

    target_field.on_change = on_target_change

    def remove_row(row: ft.Row):
        _cancel_probe(id(row))
        if len(replace_inputs_column.controls) <= 1:
            # последняя — просто очистить
            inner = row.controls[0]
            field = inner.controls[0].controls[0]
            type_label = inner.controls[1]
            field.value = ""
            field.border_color = None
            field.helper_text = None
            type_label.value = ""
            row.data = {"detected_type": None}
        else:
            replace_inputs_column.controls.remove(row)
        page.update()

    def add_field(value: str = "", detected_type: str | None = None):
        field = ft.TextField(
            value=value,
            hint_text=PLACEHOLDER,
            expand=True,
            dense=True,
        )
        type_label = ft.Text("", size=11)
        remove_btn = ft.IconButton(
            icon=ft.Icons.REMOVE_CIRCLE_OUTLINE,
            tooltip="Удалить",
        )
        # структура: row → [Column(Row(field, remove_btn), type_label)]
        field_row = ft.Row(
            [field, remove_btn],
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=4,
        )
        inner = ft.Column([field_row, type_label], spacing=0, tight=True, expand=True)
        row = ft.Row(
            [inner],
            vertical_alignment=ft.CrossAxisAlignment.START,
            spacing=4,
        )
        row.data = {"detected_type": detected_type}
        remove_btn.on_click = lambda e, r=row: remove_row(r)

        def on_field_change(_, f=field, r=row, lbl=type_label):
            raw = f.value or ""
            uuids = extract_uuids(raw)
            et = detect_entity_type(raw) if raw else None
            if len(uuids) > 1:
                # множественный ввод — разбить, применить общий detected_type ко всем
                f.value = uuids[0]
                style_field_valid(f, True)
                r.data["detected_type"] = et
                render_type_label(lbl, et, True)
                # вставить остальные UUID после текущей строки
                try:
                    idx = replace_inputs_column.controls.index(r)
                except ValueError:
                    idx = len(replace_inputs_column.controls) - 1
                for i, u in enumerate(uuids[1:], start=1):
                    add_field(u, detected_type=et)
                    new_row = replace_inputs_column.controls.pop()
                    replace_inputs_column.controls.insert(idx + i, new_row)
                if et is None:
                    _schedule_probe(
                        id(r),
                        lambda f=f: f.value,
                        lbl,
                        lambda x, r=r: r.data.__setitem__("detected_type", x),
                    )
                else:
                    _cancel_probe(id(r))
            elif len(uuids) == 1:
                style_field_valid(f, True)
                r.data["detected_type"] = et
                render_type_label(lbl, et, True)
                if et is None:
                    _schedule_probe(
                        id(r),
                        lambda f=f: f.value,
                        lbl,
                        lambda x, r=r: r.data.__setitem__("detected_type", x),
                    )
                else:
                    _cancel_probe(id(r))
            else:
                style_field_valid(f, False)
                r.data["detected_type"] = None
                render_type_label(lbl, None, False)
                _cancel_probe(id(r))
            page.update()

        field.on_change = on_field_change
        if value:
            style_field_valid(field, True)
            render_type_label(type_label, detected_type, True)
            if detected_type is None:
                _schedule_probe(
                    id(row),
                    lambda field=field: field.value,
                    type_label,
                    lambda x, row=row: row.data.__setitem__("detected_type", x),
                )
        replace_inputs_column.controls.append(row)

    def add_field_btn_click(_):
        add_field("")
        page.update()

    add_btn = ft.TextButton(
        "Добавить",
        icon=ft.Icons.ADD,
        on_click=add_field_btn_click,
    )

    # стартовое пустое поле
    add_field("")

    cleanup_radio = ft.RadioGroup(
        value="archive",
        content=ft.Column(
            [
                ft.Radio(
                    value="archive",
                    label="Архивировать дубли (по умолчанию)",
                ),
                ft.Radio(
                    value="delete",
                    label="Удалить дубли",
                ),
                ft.Radio(
                    value="none",
                    label="Ничего — оставить дубли как есть",
                ),
            ],
            tight=True,
            spacing=0,
        ),
    )

    auto_confirm_checkbox = ft.Checkbox(
        label="Запустить без финального подтверждения",
        value=False,
    )

    # ---- Расписание ----
    schedule_state: dict = {"dt": None}  # datetime в МСК или None (запуск сейчас)
    schedule_summary = ft.Text(
        "Запуск: сейчас",
        size=12,
        color=ft.Colors.GREY,
    )
    schedule_error = ft.Text("", size=11, color=ft.Colors.RED)

    def _refresh_schedule_summary():
        dt = schedule_state["dt"]
        if dt is None:
            schedule_summary.value = "Запуск: сейчас"
            schedule_summary.color = ft.Colors.GREY
        else:
            schedule_summary.value = f"Запуск: {dt.strftime('%d.%m.%Y %H:%M')} (МСК)"
            schedule_summary.color = ft.Colors.BLUE
        page.update()

    # Flet picker ожидает naive datetime для границ — конвертируем из МСК-now
    _now_msk_naive = datetime.now(MSK_TZ).replace(tzinfo=None)
    date_picker = ft.DatePicker(
        first_date=_now_msk_naive.replace(hour=0, minute=0, second=0, microsecond=0),
        last_date=_now_msk_naive.replace(year=_now_msk_naive.year + 2),
        help_text="Выберите дату запуска (МСК)",
    )
    time_picker = ft.TimePicker(
        confirm_text="Готово",
        cancel_text="Отмена",
        help_text="Выберите время запуска (МСК)",
    )

    def on_date_picked(e):
        try:
            picked = date_picker.value
            if picked is None:
                return
            cur = schedule_state["dt"]
            hh, mm = (cur.hour, cur.minute) if cur is not None else (0, 0)
            schedule_state["dt"] = datetime(
                year=picked.year,
                month=picked.month,
                day=picked.day,
                hour=hh,
                minute=mm,
                tzinfo=MSK_TZ,
            )
            schedule_error.value = ""
            _refresh_schedule_summary()
        except Exception as err:  # noqa: BLE001
            log.exception("on_date_picked failed: %s", err)

    def on_time_picked(e):
        try:
            picked = time_picker.value
            if picked is None:
                return
            if isinstance(picked, dtime):
                hh, mm = picked.hour, picked.minute
            else:
                return
            cur = schedule_state["dt"]
            if cur is None:
                today = datetime.now(MSK_TZ)
                cur = datetime(
                    year=today.year,
                    month=today.month,
                    day=today.day,
                    tzinfo=MSK_TZ,
                )
            schedule_state["dt"] = cur.replace(hour=hh, minute=mm)
            schedule_error.value = ""
            _refresh_schedule_summary()
        except Exception as err:  # noqa: BLE001
            log.exception("on_time_picked failed: %s", err)

    date_picker.on_change = on_date_picked
    time_picker.on_change = on_time_picked

    # Подкладываем пикеры в overlay; убираем их при закрытии диалога чтобы не
    # копились между переоткрытиями
    page.overlay.append(date_picker)
    page.overlay.append(time_picker)

    def open_date_picker(_):
        try:
            page.open(date_picker)
        except Exception as err:  # noqa: BLE001
            log.exception("open_date_picker failed: %s", err)

    def open_time_picker(_):
        try:
            page.open(time_picker)
        except Exception as err:  # noqa: BLE001
            log.exception("open_time_picker failed: %s", err)

    def reset_schedule(_):
        schedule_state["dt"] = None
        schedule_error.value = ""
        _refresh_schedule_summary()

    schedule_row = ft.Row(
        [
            ft.OutlinedButton(
                "📅 Выбрать дату",
                on_click=open_date_picker,
            ),
            ft.OutlinedButton(
                "🕒 Выбрать время",
                on_click=open_time_picker,
            ),
            ft.TextButton(
                "Сбросить",
                icon=ft.Icons.CLEAR,
                on_click=reset_schedule,
            ),
        ],
        spacing=8,
        wrap=True,
    )

    instructions = ft.ExpansionTile(
        title=ft.Text(
            "Как это работает и правила",
            weight=ft.FontWeight.W_500,
            size=13,
            color=ft.Colors.BLUE,
        ),
        initially_expanded=False,
        tile_padding=ft.padding.symmetric(horizontal=8, vertical=0),
        controls=[
            ft.Container(
                content=ft.Column(
                    [
                        ft.Text(
                            "Как это работает", weight=ft.FontWeight.BOLD, size=13
                        ),
                        ft.Text(
                            "1. Укажите ID дублей и один целевой ID.\n"
                            "2. Сервис заменит дубли на целевую сущность во всех "
                            "документах.\n"
                            "3. Затем дубли будут заархивированы или удалены.",
                            size=12,
                        ),
                        ft.Divider(height=1),
                        ft.Text(
                            "Правила и ограничения",
                            weight=ft.FontWeight.BOLD,
                            size=13,
                        ),
                        ft.Text(
                            "• Все ID — одного типа: только Товары, только Услуги, "
                            "только Модификации или только Комплекты. Смешивать "
                            "нельзя.\n"
                            "• Целевой ID не должен быть среди дублей.\n"
                            "• Принимается: чистый UUID, ссылка из админки МС "
                            "(#good / #feature / #bundle) или прямая API-ссылка.\n"
                            "• Товар и Услугу по ссылке #good/edit не различить — тип "
                            "определится автоматически при проверке.\n"
                            "• Комплекты встречаются в позициях только у Заказов "
                            "покупателя и Отгрузок.",
                            size=12,
                        ),
                    ],
                    spacing=4,
                    tight=True,
                ),
                bgcolor=ft.Colors.BLUE_50,
                border_radius=8,
                padding=12,
            )
        ],
    )

    dialog = ft.AlertDialog(
        modal=True,
        title=ft.Text("Новый процесс дедупликации"),
        content=ft.Container(
            content=ft.Column(
                [
                    instructions,
                    ft.Text(
                        "ID дублей — что заменяем",
                        weight=ft.FontWeight.W_500,
                    ),
                    ft.Text(
                        "Можно несколько. Вставьте сразу несколько ссылок в одно "
                        "поле — они разобьются автоматически.",
                        size=11,
                        color=ft.Colors.GREY,
                    ),
                    replace_inputs_column,
                    add_btn,
                    ft.Container(height=6),
                    ft.Text(
                        "Целевой ID — на него заменяем",
                        weight=ft.FontWeight.W_500,
                    ),
                    target_field,
                    target_type_label,
                    ft.Container(height=6),
                    ft.Text(
                        "Что делать с дублями после замены",
                        weight=ft.FontWeight.W_500,
                    ),
                    cleanup_radio,
                    ft.Text(
                        "Архивирование обратимо. Удаление безвозвратно и не пройдёт, "
                        "если сущность ещё используется в других объектах. «Ничего» — "
                        "только замена в документах, сами дубли остаются нетронутыми.",
                        size=11,
                        color=ft.Colors.GREY,
                    ),
                    ft.Container(height=2),
                    auto_confirm_checkbox,
                    ft.Text(
                        "Без галочки сервис сначала покажет карту замен и дождётся "
                        "вашего подтверждения.",
                        size=11,
                        color=ft.Colors.GREY,
                    ),
                    ft.Container(height=6),
                    ft.Text("Когда запустить", weight=ft.FontWeight.W_500),
                    ft.Text(
                        "Пусто — запуск сейчас. Либо выберите дату и время (МСК).",
                        size=11,
                        color=ft.Colors.GREY,
                    ),
                    schedule_row,
                    schedule_summary,
                    schedule_error,
                    error_text,
                ],
                tight=True,
                spacing=8,
                scroll=ft.ScrollMode.AUTO,
            ),
            width=580,
            height=720,
        ),
        actions=[cancel_btn, run_btn],
        actions_alignment=ft.MainAxisAlignment.END,
    )

    def close_dialog():
        for key in list(probe_tasks.keys()):
            _cancel_probe(key)
        page.close(dialog)

    cancel_btn.on_click = lambda e: close_dialog()

    async def on_run(_):
        try:
            await _on_run_impl()
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            log.exception("on_run failed: %s", err)
            error_text.value = f"Внутренняя ошибка: {err}"
            run_btn.disabled = False
            run_btn.text = "Запустить"
            try:
                page.update()
            except Exception:  # noqa: BLE001
                pass

    async def _on_run_impl():
        log.info("on_run: started")
        # извлечь UUID + предварительно определённые типы из всех полей
        replace_entries: list[tuple[str, str | None]] = []  # (uuid, detected_type)
        seen: set[str] = set()
        for row in replace_inputs_column.controls:
            inner = row.controls[0]
            field = inner.controls[0].controls[0]
            uid = extract_uuid(field.value or "")
            det = row.data.get("detected_type") if row.data else None
            if uid and uid not in seen:
                seen.add(uid)
                replace_entries.append((uid, det))
        log.info("on_run: parsed %d replace ids", len(replace_entries))

        target_id = extract_uuid(target_field.value or "")
        target_detected = target_state["detected_type"]

        # подсветить поля
        for row in replace_inputs_column.controls:
            inner = row.controls[0]
            field = inner.controls[0].controls[0]
            uid = extract_uuid(field.value or "")
            style_field_valid(field, uid is not None)
        style_field_valid(target_field, target_id is not None)
        page.update()

        if not replace_entries:
            error_text.value = "Укажите хотя бы один валидный ID для замены"
            page.update()
            return
        if not target_id:
            error_text.value = "Укажите валидный целевой ID"
            page.update()
            return
        replace_ids = [u for u, _ in replace_entries]
        if target_id in replace_ids:
            error_text.value = "Целевой ID не может быть в списке замены"
            page.update()
            return

        # 1) быстрая отбраковка: если по ссылкам однозначно определены ≥2 разных
        #    типов (например variant + bundle) — это точно микс, без обращения к API.
        #    #good-ссылки (товар/услуга неотличимы по URL) сюда не попадают — их тип
        #    определяется пробой ниже, поэтому глобальный тип здесь НЕ фиксируем.
        detected_types = {t for _, t in replace_entries if t is not None}
        if target_detected is not None:
            detected_types.add(target_detected)
        if len(detected_types) > 1:
            error_text.value = (
                "Нельзя смешивать разные типы. "
                "Все позиции должны быть одного типа: "
                "только Товары, Услуги, Модификации или Комплекты."
            )
            page.update()
            return

        # тип, определённый по ссылке, для каждого id (None у #good — уточним пробой)
        detected_by_id: dict[str, str | None] = {u: det for u, det in replace_entries}
        detected_by_id[target_id] = target_detected

        run_btn.disabled = True
        run_btn.text = "Проверка ID..."
        error_text.value = ""
        page.update()

        login = page.session.get("login")
        password = page.session.get("password")
        invalid: list[str] = []
        # после проверки: какой тип у каждого id (для тех, что не были детектированы)
        probed_types: dict[str, str | None] = {}
        log.info("on_run: opening redis client")
        redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        try:
            try:
                log.info("on_run: opening MS client and starting ID probe")
                async with MSClient(
                    login=login, password=password, redis=redis_client
                ) as client:
                    ids_to_check = [target_id] + replace_ids
                    sem = asyncio.Semaphore(10)

                    async def check(pid: str):
                        async with sem:
                            try:
                                # тип, однозначно определённый по ссылке этого id —
                                # один GET; подсказка применяется только к ЭТОМУ id
                                hint = detected_by_id.get(pid)
                                if hint is not None:
                                    try:
                                        await client.get_entity(hint, pid)
                                        probed_types[pid] = hint
                                    except MSRequestError:
                                        invalid.append(pid)
                                    return
                                # тип неизвестен — пробуем product → bundle → service → variant
                                et = await client.resolve_entity_type(pid)
                                if et is None:
                                    invalid.append(pid)
                                else:
                                    probed_types[pid] = et
                            except Exception as e:  # noqa: BLE001
                                log.exception("ID probe failed for %s: %s", pid, e)
                                invalid.append(pid)

                    await asyncio.gather(*(check(pid) for pid in ids_to_check))
                log.info(
                    "on_run: ID probe done — invalid=%d, probed=%d",
                    len(invalid),
                    len(probed_types),
                )
            except MSAuthError:
                error_text.value = "Сессия истекла — перезайдите"
                run_btn.disabled = False
                run_btn.text = "Запустить"
                page.update()
                return
            except MSRequestError as e:
                error_text.value = f"Ошибка соединения: {e}"
                run_btn.disabled = False
                run_btn.text = "Запустить"
                page.update()
                return

            if invalid:
                error_text.value = "Не найдены сущности: " + ", ".join(invalid)
                run_btn.disabled = False
                run_btn.text = "Запустить"
                page.update()
                return

            # 2) основная проверка однородности — по реально определённым типам
            #    (включая #good-id, чей тип стал известен только после пробы)
            probed_set = {t for t in probed_types.values() if t is not None}
            if len(probed_set) > 1:
                error_text.value = (
                    "Нельзя смешивать разные типы. "
                    "Все позиции должны быть одного типа: "
                    "только Товары, Услуги, Модификации или Комплекты."
                )
                run_btn.disabled = False
                run_btn.text = "Запустить"
                page.update()
                return
            entity_type = next(iter(probed_set), "product")
            cleanup_mode = cleanup_radio.value or "archive"
            auto_confirm = bool(auto_confirm_checkbox.value)

            # Расписание: если задано — валидируем что в будущем
            scheduled_dt = schedule_state["dt"]
            scheduled_at_ms: int | None = None
            if scheduled_dt is not None:
                now_msk = datetime.now(MSK_TZ)
                if scheduled_dt <= now_msk:
                    schedule_error.value = "Время запуска должно быть в будущем (МСК)"
                    run_btn.disabled = False
                    run_btn.text = "Запустить"
                    page.update()
                    return
                scheduled_at_ms = int(scheduled_dt.timestamp() * 1000)

            task_id = str(uuid.uuid4())
            log.info(
                "on_run: task_id=%s entity_type=%s cleanup=%s scheduled_at_ms=%s",
                task_id,
                entity_type,
                cleanup_mode,
                scheduled_at_ms,
            )

            # Метаданные карточки — нужны для восстановления после релогина
            try:
                await save_job_meta(
                    redis_client,
                    JobMeta(
                        task_id=task_id,
                        login=login,
                        replace_ids=replace_ids,
                        target_id=target_id,
                        entity_type=entity_type,
                        cleanup_mode=cleanup_mode,
                        auto_confirm=auto_confirm,
                        scheduled_at_ms=scheduled_at_ms,
                    ),
                )
                log.info("on_run: job meta saved")
            except Exception as e:  # noqa: BLE001
                log.warning("Failed to save job meta for %s: %s", task_id, e)

            if scheduled_at_ms is None:
                # Запуск сейчас — старый путь
                try:
                    log.info("on_run: calling run_deduplication.kiq")
                    await run_deduplication.kiq(
                        task_id,
                        replace_ids,
                        target_id,
                        login,
                        password,
                        entity_type,
                        cleanup_mode,
                        auto_confirm,
                    )
                    log.info("on_run: kiq enqueued")
                except Exception as e:  # noqa: BLE001
                    error_text.value = f"Не удалось поставить задачу в очередь: {e}"
                    run_btn.disabled = False
                    run_btn.text = "Запустить"
                    page.update()
                    return
            else:
                # Отложенный запуск — кладём в очередь шедулера
                try:
                    log.info("on_run: enqueue_scheduled")
                    await enqueue_scheduled(
                        redis_client,
                        task_id=task_id,
                        login=login,
                        password=password,
                        replace_ids=replace_ids,
                        target_id=target_id,
                        entity_type=entity_type,
                        cleanup_mode=cleanup_mode,
                        auto_confirm=auto_confirm,
                        scheduled_at_ms=scheduled_at_ms,
                    )
                    # начальное состояние SCHEDULED для прогресс-таски
                    await set_progress(
                        redis_client,
                        task_id,
                        ProgressState(
                            status=ProcessStatus.SCHEDULED,
                            message=(
                                f"Запланирован на {_fmt_msk(scheduled_at_ms)} (МСК)"
                            ),
                        ),
                    )
                    log.info("on_run: scheduled progress set")
                except Exception as e:  # noqa: BLE001
                    error_text.value = f"Не удалось запланировать задачу: {e}"
                    run_btn.disabled = False
                    run_btn.text = "Запустить"
                    page.update()
                    return

            log.info("on_run: closing dialog and creating card")
            close_dialog()
            await _add_process_card(
                page,
                processes_column,
                refresh_new_btn,
                task_id,
                replace_ids,
                target_id,
                entity_type,
                cleanup_mode,
                auto_confirm,
                scheduled_at_ms,
            )
            log.info("on_run: card created OK")
        finally:
            # await в finally при close() корутины может вызывать
            # "coroutine ignored GeneratorExit" — защитим явно
            try:
                await redis_client.aclose()
            except GeneratorExit:
                raise
            except BaseException as e:  # noqa: BLE001
                log.debug("redis_client.aclose() suppressed: %r", e)

    run_btn.on_click = on_run

    page.open(dialog)


# ---------- RESTORE ----------


async def _restore_user_cards(
    page: ft.Page,
    processes_column: ft.Column,
    refresh_new_btn,
) -> None:
    """Подтянуть из Redis запланированные и активные процессы текущего пользователя
    и отрисовать карточки. Терминальные пропускаем."""
    login = page.session.get("login")
    if not login:
        return
    redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    try:
        try:
            task_ids = await list_user_jobs(redis, login)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("restore: list_user_jobs failed: %s", e)
            return
        if not task_ids:
            return
        log.info("restore: %d job(s) for %s", len(task_ids), login)
        # сортируем по scheduled_at_ms (если есть) для предсказуемого порядка
        metas: list[JobMeta] = []
        for tid in task_ids:
            try:
                meta = await get_job_meta(redis, tid)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("restore: meta load failed for %s: %s", tid, e)
                meta = None
            if meta is None:
                # meta истёк по TTL — чистим индекс
                try:
                    await forget_user_job(redis, tid, login)
                except Exception:  # noqa: BLE001
                    pass
                continue
            # пропустить терминальные процессы
            try:
                state = await get_progress(redis, tid)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                state = None
            if state is not None and state.status in TERMINAL_STATUSES:
                continue
            # Осиротевший процесс: не завершён, НЕ ждёт в очереди шедулера и
            # heartbeat протух (воркер умер при перезапуске, immediate-задача не
            # переподхватывается). Помечаем INTERRUPTED, чтобы карточка предложила
            # перезапуск, а не висела как «Ожидание» с одной «Отменой».
            # Запланированные задачи (в очереди шедулера) не трогаем — их запустит
            # шедулер; их heartbeat и должен отсутствовать.
            status = state.status if state is not None else ProcessStatus.PENDING
            if status != ProcessStatus.SCHEDULED:
                try:
                    sched = await get_scheduled_at_ms(redis, tid)
                except Exception:  # noqa: BLE001
                    sched = None
                if sched is None:
                    try:
                        alive = await is_alive(redis, tid)
                    except Exception:  # noqa: BLE001
                        alive = False
                    if not alive:
                        try:
                            await set_progress(
                                redis,
                                tid,
                                ProgressState(
                                    status=ProcessStatus.INTERRUPTED,
                                    message="Прервано перезапуском сервиса",
                                    replaced_count=(
                                        state.replaced_count if state is not None else 0
                                    ),
                                    deleted_count=(
                                        state.deleted_count if state is not None else 0
                                    ),
                                ),
                            )
                        except Exception:  # noqa: BLE001
                            pass
            metas.append(meta)

        metas.sort(key=lambda m: m.scheduled_at_ms or 0)
        for meta in metas:
            try:
                await _add_process_card(
                    page,
                    processes_column,
                    refresh_new_btn,
                    meta.task_id,
                    meta.replace_ids,
                    meta.target_id,
                    meta.entity_type,
                    meta.cleanup_mode,
                    meta.auto_confirm,
                    meta.scheduled_at_ms,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("restore: add card failed for %s: %s", meta.task_id, e)
    except asyncio.CancelledError:
        log.info("restore: cancelled")
        raise
    finally:
        # await в finally при close() корутины может вызывать
        # "coroutine ignored GeneratorExit" — глушим явно
        try:
            await redis.aclose()
        except GeneratorExit:
            raise
        except BaseException as e:  # noqa: BLE001
            log.debug("restore: redis.aclose() suppressed: %r", e)


# ---------- PROCESS CARD ----------


async def _add_process_card(
    page: ft.Page,
    processes_column: ft.Column,
    refresh_new_btn,
    task_id: str,
    replace_ids: list[str],
    target_id: str,
    entity_type: str = "product",
    cleanup_mode: str = "archive",
    auto_confirm: bool = False,
    scheduled_at_ms: int | None = None,
) -> None:
    active = page.session.get("active_processes") or {}
    process_number = sum(1 for _ in processes_column.controls) + 1

    entity_label = ENTITY_TYPES.get(entity_type, "Товар")
    entity_label_plural = ENTITY_TYPE_GROUP_NAMES.get(entity_type, "Товары")
    target_label_singular = ENTITY_TYPE_TARGET_NAMES.get(entity_type, "Целевой товар")
    cleanup_mode_label = {
        "archive": "архивировать",
        "delete": "удалить",
        "none": "ничего (оставить как есть)",
    }.get(cleanup_mode, "архивировать")
    if auto_confirm:
        cleanup_mode_label += " (без подтверждения)"

    title = ft.Text(
        f"Процесс #{process_number} — {entity_label}",
        size=16,
        weight=ft.FontWeight.BOLD,
    )
    replace_ids_display = ", ".join(replace_ids[:5]) + (
        f" и ещё {len(replace_ids) - 5}" if len(replace_ids) > 5 else ""
    )
    replace_ids_text = ft.Text(
        f"Заменить ({entity_label_plural}): {replace_ids_display}",
        size=12,
        selectable=True,
    )
    target_text = ft.Text(
        f"{target_label_singular}: {target_id}",
        size=12,
        selectable=True,
    )
    cleanup_text = ft.Text(
        f"Действие с дублями: {cleanup_mode_label}",
        size=12,
        color=ft.Colors.GREY,
    )
    schedule_text = ft.Text(
        (
            f"Запланирован на {_fmt_msk(scheduled_at_ms)} (МСК)"
            if scheduled_at_ms is not None
            else ""
        ),
        size=12,
        color=ft.Colors.BLUE,
        visible=scheduled_at_ms is not None,
    )
    initial_status = (
        ProcessStatus.SCHEDULED
        if scheduled_at_ms is not None
        else ProcessStatus.PENDING
    )
    status_text = ft.Text(
        "Статус: " + _ru_status(initial_status), size=14, weight=ft.FontWeight.W_500
    )
    # Прогресс-бар имеет смысл только когда процесс активно что-то делает.
    # Скрываем для SCHEDULED/PENDING/CONFIRMING/DONE/CANCELLED/ERROR.
    _active_progress_statuses = {
        ProcessStatus.SCANNING,
        ProcessStatus.REPLACING,
        ProcessStatus.DELETING,
    }
    progress_bar = ft.ProgressBar(width=500, value=0)
    counter_text = ft.Text("0/0", size=12)
    progress_row = ft.Row(
        [progress_bar, counter_text],
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
        visible=initial_status in _active_progress_statuses,
    )
    message_text = ft.Text("", size=12, selectable=True)
    error_text = ft.Text("", color=ft.Colors.RED, size=12, selectable=True)

    confirm_btn = ft.ElevatedButton(
        "Подтвердить замену",
        icon=ft.Icons.CHECK_CIRCLE,
        visible=False,
    )
    cancel_btn = ft.OutlinedButton("Отмена", icon=ft.Icons.CANCEL)
    restart_btn = ft.ElevatedButton(
        "Перезапустить сначала",
        icon=ft.Icons.RESTART_ALT,
        visible=False,
    )
    dismiss_btn = ft.TextButton(
        "Убрать",
        icon=ft.Icons.CLOSE,
        visible=False,
    )

    affected_view = ft.Container(visible=False, padding=ft.padding.only(top=8))

    card_container = ft.Container(
        content=ft.Column(
            [
                ft.Row([title, ft.Container(expand=True), status_text]),
                replace_ids_text,
                target_text,
                cleanup_text,
                schedule_text,
                progress_row,
                message_text,
                error_text,
                affected_view,
                ft.Row([confirm_btn, cancel_btn, restart_btn, dismiss_btn]),
            ],
            spacing=8,
        ),
        padding=15,
        border_radius=10,
        border=ft.border.all(1, ft.Colors.GREY_300),
        bgcolor=ft.Colors.WHITE,
    )

    async def on_cancel(_):
        cancel_btn.disabled = True
        cancel_btn.text = "Отменяется..."
        # пока идёт отмена — Подтвердить не должна оставаться активной
        confirm_btn.disabled = True
        page.update()
        redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        try:
            await request_cancel(redis, task_id)
            # если задача ещё не стартовала — снимаем из очереди шедулера
            # сразу и помечаем CANCELLED, чтобы UI получил терминальный статус
            scheduled_score = await get_scheduled_at_ms(redis, task_id)
            if scheduled_score is not None:
                await remove_scheduled(redis, task_id)
                await set_progress(
                    redis,
                    task_id,
                    ProgressState(
                        status=ProcessStatus.CANCELLED,
                        message="Отменено до старта",
                    ),
                )
                return
            # Форсируем терминальный CANCELLED прямо из UI для любого ещё
            # незавершённого состояния. Иначе при отмене «осиротевшей» карточки
            # (например после полного перезапуска сервиса — немедленная задача не
            # переподхватывается воркером, либо воркер ждёт в CONFIRMING) флаг
            # cancel некому обработать, и карточка зависает в «Отменяется...».
            # Если воркер жив, он тоже увидит флаг и сойдётся к CANCELLED —
            # расхождение самоустранится (воркер перезапишет статус со счётчиками).
            try:
                cur = await get_progress(redis, task_id)
            except Exception:  # noqa: BLE001
                cur = None
            if cur is None or cur.status not in TERMINAL_STATUSES:
                await set_progress(
                    redis,
                    task_id,
                    ProgressState(
                        status=ProcessStatus.CANCELLED,
                        message="Отменено пользователем",
                        replaced_count=cur.replaced_count if cur else 0,
                        deleted_count=cur.deleted_count if cur else 0,
                        delete_errors=cur.delete_errors if cur else [],
                        skipped_ids=cur.skipped_ids if cur else [],
                    ),
                )
        finally:
            try:
                await redis.aclose()
            except GeneratorExit:
                raise
            except BaseException as e:  # noqa: BLE001
                log.debug("on_cancel: redis.aclose() suppressed: %r", e)

    async def on_confirm(_):
        confirm_btn.disabled = True
        confirm_btn.text = "Подтверждено..."
        # запретим отмену пока запускаем замену (терминал придёт от воркера)
        cancel_btn.disabled = True
        page.update()
        redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        try:
            await request_confirm(redis, task_id)
        finally:
            try:
                await redis.aclose()
            except GeneratorExit:
                raise
            except BaseException as e:  # noqa: BLE001
                log.debug("on_confirm: redis.aclose() suppressed: %r", e)

    async def on_restart(_):
        # Перезапуск осиротевшего процесса — с нуля (повторный скан найдёт только
        # оставшиеся дубли, уже заменённые позиции не находятся).
        restart_btn.disabled = True
        restart_btn.text = "Перезапуск..."
        dismiss_btn.disabled = True
        page.update()
        login = page.session.get("login")
        password = page.session.get("password")
        redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        try:
            # сбрасываем старое состояние (в т.ч. возможные cancel/confirm) и
            # заново ставим задачу в очередь с тем же task_id
            await cleanup_process(redis, task_id)
            await set_progress(
                redis,
                task_id,
                ProgressState(
                    status=ProcessStatus.PENDING, message="Перезапуск..."
                ),
            )
            await run_deduplication.kiq(
                task_id,
                replace_ids,
                target_id,
                login,
                password,
                entity_type,
                cleanup_mode,
                auto_confirm,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("restart failed for %s: %s", task_id, e)
            error_text.value = f"Не удалось перезапустить: {e}"
            restart_btn.disabled = False
            restart_btn.text = "Перезапустить сначала"
            dismiss_btn.disabled = False
        finally:
            try:
                await redis.aclose()
            except BaseException as e:  # noqa: BLE001
                log.debug("on_restart: redis.aclose() suppressed: %r", e)
        try:
            page.update()
        except Exception:  # noqa: BLE001
            pass

    async def on_dismiss(_):
        login = page.session.get("login")
        redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        try:
            try:
                if login:
                    await forget_user_job(redis, task_id, login)
            except Exception:  # noqa: BLE001
                pass
            try:
                await cleanup_process(redis, task_id)
            except Exception:  # noqa: BLE001
                pass
            try:
                await redis.delete(alive_key(task_id))
            except Exception:  # noqa: BLE001
                pass
        finally:
            try:
                await redis.aclose()
            except BaseException as e:  # noqa: BLE001
                log.debug("on_dismiss: redis.aclose() suppressed: %r", e)
        active = page.session.get("active_processes") or {}
        info = active.pop(task_id, None)
        page.session.set("active_processes", active)
        if info:
            t = info.get("polling_task")
            if t and not t.done():
                t.cancel()
        try:
            processes_column.controls.remove(card_container)
        except ValueError:
            pass
        refresh_new_btn()
        page.update()

    cancel_btn.on_click = on_cancel
    confirm_btn.on_click = on_confirm
    restart_btn.on_click = on_restart
    dismiss_btn.on_click = on_dismiss

    processes_column.controls.append(card_container)

    polling_task = page.run_task(
        _poll_progress,
        page,
        task_id,
        status_text,
        progress_bar,
        counter_text,
        progress_row,
        message_text,
        error_text,
        confirm_btn,
        cancel_btn,
        restart_btn,
        dismiss_btn,
        card_container,
        affected_view,
        refresh_new_btn,
    )

    active[task_id] = {"card": card_container, "polling_task": polling_task}
    page.session.set("active_processes", active)
    refresh_new_btn()
    page.update()


# ---------- AFFECTED MAP ----------


def _build_affected_view(page: ft.Page, affected: list) -> ft.Control:
    """Карта замен: документы со ссылками и их позиции."""
    from collections import defaultdict
    from app.models import AffectedPosition  # type: ignore

    groups: dict[tuple[str, str], list[AffectedPosition]] = defaultdict(list)
    for ap in affected:
        groups[(ap.doc_type, ap.doc_id)].append(ap)

    doc_ids = {(ap.doc_type, ap.doc_id) for ap in affected}
    header = ft.Text(
        f"Затронуто документов: {len(doc_ids)} | позиций: {len(affected)}",
        size=12,
        weight=ft.FontWeight.W_500,
    )

    tiles: list[ft.Control] = []
    for (doc_type, doc_id), items in groups.items():
        first = items[0]
        type_ru = DOC_TYPE_NAMES.get(doc_type, doc_type)
        doc_label = f"{type_ru}: {first.doc_name or '—'}"
        merge_count = sum(1 for it in items if it.has_target_already)
        subtitle = f"позиций: {len(items)}"
        if merge_count:
            subtitle += f" (с объединением: {merge_count})"

        tile_header_children = [
            ft.Column(
                [
                    ft.Text(doc_label, size=12, weight=ft.FontWeight.W_500),
                    ft.Text(subtitle, size=11, color=ft.Colors.GREY),
                ],
                spacing=2,
                expand=True,
            ),
        ]
        if first.doc_uuid_href:
            tile_header_children.append(
                ft.IconButton(
                    icon=ft.Icons.OPEN_IN_NEW,
                    tooltip="Открыть в МойСклад",
                    icon_size=18,
                    on_click=lambda e, url=first.doc_uuid_href: page.launch_url(url),
                )
            )

        ENT_WORDS = {
            "product": "товар",
            "variant": "модификация",
            "service": "услуга",
            "bundle": "комплект",
        }
        position_rows: list[ft.Control] = []
        for it in items:
            ent_word = ENT_WORDS.get(it.replace_entity_type, "товар")
            replace_label = ft.Text(
                f"{ent_word} {_short_id(it.replace_id)} × {it.quantity:g}",
                size=11,
                selectable=True,
            )
            action_label = ft.Text(
                "ОБЪЕДИНЕНИЕ С ЦЕЛЕВЫМ" if it.has_target_already else "ЗАМЕНА",
                size=10,
                color=ft.Colors.ORANGE if it.has_target_already else ft.Colors.BLUE,
                weight=ft.FontWeight.W_500,
            )
            row_children: list[ft.Control] = [
                replace_label,
                ft.Container(expand=True),
                action_label,
            ]
            if it.replace_product_uuid_href:
                row_children.append(
                    ft.IconButton(
                        icon=ft.Icons.OPEN_IN_NEW,
                        tooltip="Открыть товар",
                        icon_size=14,
                        on_click=lambda e, url=it.replace_product_uuid_href: page.launch_url(
                            url
                        ),
                    )
                )
            position_rows.append(
                ft.Row(
                    row_children,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    spacing=4,
                )
            )

        tile = ft.ExpansionTile(
            title=ft.Row(
                tile_header_children, vertical_alignment=ft.CrossAxisAlignment.CENTER
            ),
            controls=[
                ft.Container(
                    content=ft.Column(position_rows, spacing=4, tight=True),
                    padding=ft.padding.symmetric(horizontal=16, vertical=4),
                )
            ],
            initially_expanded=False,
            tile_padding=ft.padding.symmetric(horizontal=8, vertical=0),
        )
        tiles.append(tile)

    return ft.Container(
        content=ft.Column(
            [header, ft.Container(height=4), *tiles],
            spacing=2,
            tight=True,
        ),
        border=ft.border.all(1, ft.Colors.GREY_200),
        border_radius=8,
        padding=8,
        bgcolor=ft.Colors.GREY_50,
    )


# ---------- POLLING ----------


async def _poll_progress(
    page: ft.Page,
    task_id: str,
    status_text: ft.Text,
    progress_bar: ft.ProgressBar,
    counter_text: ft.Text,
    progress_row: ft.Row,
    message_text: ft.Text,
    error_text: ft.Text,
    confirm_btn: ft.ElevatedButton,
    cancel_btn: ft.OutlinedButton,
    restart_btn: ft.ElevatedButton,
    dismiss_btn: ft.TextButton,
    card_container: ft.Container,
    affected_view: ft.Container,
    refresh_new_btn,
) -> None:
    _active_progress_statuses = {
        ProcessStatus.SCANNING,
        ProcessStatus.REPLACING,
        ProcessStatus.DELETING,
    }
    redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    affected_rendered = False
    try:
        while True:
            try:
                state: ProgressState | None = await get_progress(redis, task_id)
            except Exception as e:  # noqa: BLE001
                log.warning("Polling error for %s: %s", task_id, e)
                state = None

            if state is not None:
                status_text.value = "Статус: " + _ru_status(state.status)
                if state.total > 0:
                    progress_bar.value = min(1.0, state.current / state.total)
                else:
                    progress_bar.value = None
                counter_text.value = (
                    f"{state.current}/{state.total}"
                    if state.total
                    else f"{state.current}"
                )
                message_text.value = state.message or ""
                error_text.value = state.error or ""

                # прогресс-бар активен только когда что-то реально идёт
                progress_row.visible = state.status in _active_progress_statuses

                confirm_btn.visible = state.status == ProcessStatus.CONFIRMING

                # Осиротевший процесс: показываем «Перезапустить»/«Убрать»,
                # прячем «Отмена»/«Подтвердить», карточка оранжевая. После рестарта
                # статус сменится на PENDING/SCANNING и вид вернётся к обычному.
                is_interrupted = state.status == ProcessStatus.INTERRUPTED
                restart_btn.visible = is_interrupted
                dismiss_btn.visible = is_interrupted
                if is_interrupted:
                    confirm_btn.visible = False
                    cancel_btn.visible = False
                    card_container.bgcolor = ft.Colors.ORANGE_50
                    card_container.border = ft.border.all(1, ft.Colors.ORANGE_400)
                elif state.status not in TERMINAL_STATUSES:
                    cancel_btn.visible = True
                    card_container.bgcolor = ft.Colors.WHITE
                    card_container.border = ft.border.all(1, ft.Colors.GREY_300)

                # рендерим карту замен либо когда ждём подтверждения, либо когда
                # уже начали замену без подтверждения (auto_confirm)
                if (
                    state.status in (ProcessStatus.CONFIRMING, ProcessStatus.REPLACING)
                    and not affected_rendered
                ):
                    try:
                        affected = await get_affected(redis, task_id)
                    except Exception as e:  # noqa: BLE001
                        log.warning("Failed to load affected for %s: %s", task_id, e)
                        affected = []
                    if affected:
                        affected_view.content = _build_affected_view(page, affected)
                        affected_view.visible = True
                        affected_rendered = True

                if state.status in TERMINAL_STATUSES:
                    cancel_btn.visible = False
                    confirm_btn.visible = False
                    restart_btn.visible = False
                    dismiss_btn.visible = False
                    if state.status == ProcessStatus.DONE:
                        card_container.bgcolor = ft.Colors.GREEN_50
                        card_container.border = ft.border.all(1, ft.Colors.GREEN_400)
                    elif state.status == ProcessStatus.ERROR:
                        card_container.bgcolor = ft.Colors.RED_50
                        card_container.border = ft.border.all(1, ft.Colors.RED_400)
                    elif state.status == ProcessStatus.CANCELLED:
                        card_container.bgcolor = ft.Colors.GREY_100
                        card_container.border = ft.border.all(1, ft.Colors.GREY_500)

                    if state.delete_errors:
                        msgs = "\n".join(state.delete_errors)
                        error_text.value = (
                            (error_text.value or "") + "\n" + msgs
                        ).strip()

                    if state.skipped_ids:
                        skipped_note = (
                            "Замена не прошла полностью — не тронуты при очистке:\n"
                            + ", ".join(state.skipped_ids)
                        )
                        error_text.value = (
                            (error_text.value or "") + "\n" + skipped_note
                        ).strip()

                    page.update()

                    active = page.session.get("active_processes") or {}
                    if task_id in active:
                        active.pop(task_id, None)
                        page.session.set("active_processes", active)
                        refresh_new_btn()
                        page.update()
                    return

                page.update()

            await asyncio.sleep(2)
    except asyncio.CancelledError:
        return
    finally:
        await redis.aclose()


# ---------- ENTRY POINT ----------

if __name__ == "__main__":
    ft.app(
        target=main,
        view=ft.AppView.WEB_BROWSER,
        port=settings.FLET_PORT,
        host="0.0.0.0",
    )
