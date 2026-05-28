"""Экран входа по логину и паролю МойСклад.

Проверяет учётные данные запросом к API; при успехе сохраняет их в сессии Flet
и открывает главный экран.
"""
import flet as ft

from app.core.redis import redis_session
from app.ms import MSAuthError, MSClient, MSRequestError


async def show_login(page: ft.Page) -> None:
    """Отрисовать форму входа и навесить обработчик авторизации."""
    login_field = ft.TextField(label="Логин МойСклад", width=320, autofocus=True)
    password_field = ft.TextField(
        label="Пароль", width=320, password=True, can_reveal_password=True
    )
    error_text = ft.Text("", color=ft.Colors.RED)
    login_btn = ft.ElevatedButton("Войти", width=320)

    async def on_login_click(_):
        # отложенный импорт разрывает цикл login ⇄ main_screen
        from app.ui.main_screen import show_main_screen

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
        async with redis_session() as redis_client:
            try:
                async with MSClient(
                    login=login, password=password, redis=redis_client
                ) as client:
                    ok = await client.check_auth()
            except MSAuthError:
                ok = False
            except MSRequestError as e:
                err = f"Ошибка соединения: {e}"

        if err:
            error_text.value = err
        elif not ok:
            error_text.value = "Неверный логин или пароль"
        else:
            page.session.set("login", login)
            page.session.set("password", password)
            await show_main_screen(page)
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
