"""Диалог создания нового процесса дедупликации.

Здесь пользователь указывает ID дублей и целевой сущности, выбирает действие с
дублями и (опционально) время запуска. Диалог:

* в реальном времени определяет тип сущности по ссылке, а для неоднозначных
  случаев (#good / голый UUID) уточняет его через API с дебаунсом;
* проверяет однородность типов и существование сущностей;
* ставит задачу в очередь немедленно либо в очередь шедулера на заданное время.
"""
import asyncio
import logging
import uuid
from datetime import datetime, time as dtime

import flet as ft

from app.core.redis import redis_session
from app.domain.enums import ProcessStatus
from app.domain.models import JobMeta, ProgressState, ScheduledJob
from app.domain.entity_types import ENTITY_TYPES
from app.ms import MSAuthError, MSClient, MSRequestError
from app.parsing.identifiers import detect_entity_type, extract_uuid, extract_uuids
from app.repositories.jobs import JobRepository
from app.repositories.progress import ProgressRepository
from app.repositories.schedule import ScheduleRepository
from app.services.deduplication import run_deduplication
from app.ui.formatting import MSK_TZ, fmt_msk
from app.ui.process_card import add_process_card

log = logging.getLogger(__name__)


async def open_new_process_dialog(
    page: ft.Page,
    processes_column: ft.Column,
    refresh_new_btn,
) -> None:
    """Открыть модальный диалог создания нового процесса дедупликации."""
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
        async with redis_session() as redis_client:
            try:
                async with MSClient(
                    login=login, password=password, redis=redis_client
                ) as client:
                    return await client.resolve_entity_type(uid)
            except Exception as e:  # noqa: BLE001
                log.debug("type probe failed for %s: %s", uid, e)
                return None

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
        async with redis_session() as redis_client:
            jobs = JobRepository(redis_client)
            progress = ProgressRepository(redis_client)
            schedule = ScheduleRepository(redis_client)
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
                await jobs.save_meta(
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
                    await schedule.enqueue(
                        ScheduledJob(
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
                    )
                    # начальное состояние SCHEDULED для прогресс-таски
                    await progress.set(
                        task_id,
                        ProgressState(
                            status=ProcessStatus.SCHEDULED,
                            message=(
                                f"Запланирован на {fmt_msk(scheduled_at_ms)} (МСК)"
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
            await add_process_card(
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

    run_btn.on_click = on_run

    page.open(dialog)
