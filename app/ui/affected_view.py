"""Виджет «карта замен»: какие документы и позиции затронет процесс.

Группирует найденные позиции по документам и показывает для каждого документа
раскрывающийся список позиций с пометкой «ЗАМЕНА» либо «ОБЪЕДИНЕНИЕ С ЦЕЛЕВЫМ».
"""
from collections import defaultdict

import flet as ft

from app.domain.documents import DOC_TYPE_NAMES
from app.domain.entity_types import ENTITY_TYPE_WORDS
from app.domain.models import AffectedPosition
from app.ui.formatting import short_id


def build_affected_view(page: ft.Page, affected: list[AffectedPosition]) -> ft.Control:
    """Построить контейнер с картой замен по списку затронутых позиций."""
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

        position_rows: list[ft.Control] = []
        for it in items:
            ent_word = ENTITY_TYPE_WORDS.get(it.replace_entity_type, "товар")
            replace_label = ft.Text(
                f"{ent_word} {short_id(it.replace_id)} × {it.quantity:g}",
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
