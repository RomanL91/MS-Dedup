"""Клиент API МойСклад.

Слой разделён по ответственностям:

* :mod:`app.ms.errors` — типы исключений клиента;
* :mod:`app.ms.transport` — транспорт: HTTP-запросы, ретраи, лимиты, авторизация;
* :mod:`app.ms.client` — доменные операции над сущностями и документами поверх транспорта.

Такое разделение отделяет «как ходим в сеть» от «что именно запрашиваем».
"""
from app.ms.client import MSClient
from app.ms.errors import MSAuthError, MSRequestError

__all__ = ["MSClient", "MSAuthError", "MSRequestError"]
