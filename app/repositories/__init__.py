"""Репозитории состояния в Redis.

Каждый класс инкапсулирует группу ключей Redis и операции над ними, скрывая от
бизнес-логики детали хранения (имена ключей, TTL, сериализацию):

* :class:`~app.repositories.progress.ProgressRepository` — прогресс, карта замен, флаги отмены/подтверждения;
* :class:`~app.repositories.jobs.JobRepository` — метаданные задач и индекс задач пользователя;
* :class:`~app.repositories.schedule.ScheduleRepository` — очередь отложенных запусков;
* :class:`~app.repositories.liveness.LivenessRepository` — heartbeat и счётчик активных процессов.

Репозитории принимают готовый клиент Redis в конструкторе — это упрощает их
подмену в тестах и убирает создание соединений из бизнес-логики.
"""
