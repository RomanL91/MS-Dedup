"""Веб-интерфейс на Flet.

UI разбит по экранам и виджетам, каждый — в своём модуле:

* :mod:`app.ui.app` — точка входа, инициализация страницы, проверка Redis, запуск брокера/шедулера;
* :mod:`app.ui.login` — экран входа;
* :mod:`app.ui.main_screen` — главный экран со списком процессов;
* :mod:`app.ui.new_process_dialog` — диалог создания нового процесса;
* :mod:`app.ui.process_card` — карточка процесса и опрос её прогресса;
* :mod:`app.ui.affected_view` — «карта замен»;
* :mod:`app.ui.api_status` — индикаторы нагрузки на API в шапке;
* :mod:`app.ui.help_dialog` — диалог-справка;
* :mod:`app.ui.restore` — восстановление карточек после релогина;
* :mod:`app.ui.formatting` — общие функции форматирования.

UI обращается к данным через репозитории (:mod:`app.repositories`) и сервисы
(:mod:`app.services`), не работая с Redis напрямую мимо них.
"""
