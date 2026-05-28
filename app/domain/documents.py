"""Типы документов МойСклад, в позициях которых выполняется замена.

:data:`DOCUMENT_TYPES` задаёт и набор обрабатываемых документов, и порядок их
сканирования. :data:`DOC_TYPE_NAMES` переводит коды типов в русские названия —
используется и в UI («карта замен»), и в сообщениях прогресса воркера.
"""

DOCUMENT_TYPES: list[str] = [
    "customerorder",
    "purchaseorder",
    "invoiceout",
    "invoicein",
    "demand",
    "supply",
    "salesreturn",
    "purchasereturn",
    "loss",
    "enter",
    "move",
    "internalorder",
    "retaildemand",
    "retailsalesreturn",
    "inventory",
]
"""Коды типов документов, которые сканируются в поисках позиций с дублями."""


DOC_TYPE_NAMES: dict[str, str] = {
    "customerorder": "Заказ покупателя",
    "purchaseorder": "Заказ поставщику",
    "invoiceout": "Счёт покупателю",
    "invoicein": "Счёт поставщика",
    "demand": "Отгрузка",
    "supply": "Приёмка",
    "salesreturn": "Возврат покупателя",
    "purchasereturn": "Возврат поставщику",
    "loss": "Списание",
    "enter": "Оприходование",
    "move": "Перемещение",
    "internalorder": "Внутренний заказ",
    "retaildemand": "Розничные продажи",
    "retailsalesreturn": "Розничные возвраты",
    "inventory": "Инвентаризации",
}
"""Сопоставление кода типа документа его русскому названию."""
