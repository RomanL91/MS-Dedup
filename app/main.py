"""Точка входа приложения (``python -m app.main``).

Вся логика вынесена в пакеты ``app.ui`` (интерфейс) и ``app.services`` (фоновые
задачи). Здесь оставлен только запуск Flet-сервера.
"""
from app.ui.app import run

if __name__ == "__main__":
    run()
