# main.py
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Optional

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand, Message, CallbackQuery
from aiogram.dispatcher.middlewares.base import BaseMiddleware

from config import load_settings
from db import Database
from handlers import router
from utils import is_director, is_officer, is_superadmin

log = logging.getLogger("main")


class PinAuthMiddleware(BaseMiddleware):
    """
    Если включён BOT_PIN, то:
      - директор/уполномоченный/superadmin проходят без PIN
      - остальные обязаны один раз выполнить /pin <код>
    """

    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any],
    ) -> Any:
        settings = data.get("settings")
        db: Database = data.get("db")
        if not settings or not getattr(settings, "bot_pin", None):
            return await handler(event, data)

        uid: Optional[int] = None
        if isinstance(event, Message) and event.from_user:
            uid = event.from_user.id
        elif isinstance(event, CallbackQuery) and event.from_user:
            uid = event.from_user.id

        if uid is None:
            return await handler(event, data)

        # exemptions
        if is_superadmin(uid, settings) or is_director(uid, settings) or is_officer(uid, settings):
            return await handler(event, data)

        if await db.is_authed(uid):
            return await handler(event, data)

        # not authed
        if isinstance(event, Message):
            text = (event.text or "").strip()
            if text.startswith("/pin"):
                return await handler(event, data)

            await event.answer(
                "🔐 Доступ к боту защищён PIN-кодом.\n\n"
                "Введи PIN одной командой:\n"
                "/pin 1234"
            )
            return

        if isinstance(event, CallbackQuery):
            try:
                await event.answer("🔐 Введите PIN: /pin 1234", show_alert=True)
            except Exception:
                pass
            return


async def director_reminder_loop(bot: Bot, db: Database, settings) -> None:
    """
    Напоминания директору о заявках в статусе REQUESTED.
    """
    log.info(
        "Director reminders enabled: after %s min, repeat %s min, check every %s sec",
        settings.remind_after_minutes,
        settings.remind_repeat_minutes,
        settings.remind_check_seconds,
    )

    while True:
        try:
            rows = await db.pending_for_remind(
                after_minutes=settings.remind_after_minutes,
                repeat_minutes=settings.remind_repeat_minutes,
            )
            if rows:
                lines = [
                    "⏰ Напоминание: есть заявки на согласовании.",
                    f"Количество: {len(rows)}",
                    "",
                ]
                for r in rows[:20]:
                    lines.append(f"#{r.id} — tg:{r.tg_id} — {str(r.purpose)[:60]}")
                if len(rows) > 20:
                    lines.append(f"... и ещё {len(rows) - 20}")

                lines.append("\nОткройте: /pending (или меню → «Директор: На согласовании»).")

                await bot.send_message(settings.director_tg_id, "\n".join(lines))
                await db.mark_reminded([r.id for r in rows])

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Director reminder loop error: %s", e)

        await asyncio.sleep(max(10, int(settings.remind_check_seconds)))


async def startup(bot: Bot, db: Database, settings) -> None:
    await db.init()
    log.info("Database initialized")

    # Команды (шорткаты)
    cmds = [
        BotCommand(command="menu", description="Показать меню"),
        BotCommand(command="help", description="Справка"),
        BotCommand(command="profile", description="Профиль / ФИО"),
        BotCommand(command="cancel", description="Отмена"),
        BotCommand(command="request", description="Создать заявку"),
        BotCommand(command="my", description="Мои заявки"),
        BotCommand(command="tokens", description="Статусы токенов"),
        BotCommand(command="pending", description="Директор: согласование"),
        BotCommand(command="active", description="Уполномоченный: активные"),
    ]
    if getattr(settings, "bot_pin", None):
        cmds.append(BotCommand(command="pin", description="Ввести PIN-код"))
    cmds.append(BotCommand(command="admin", description="Админ-панель (superadmin)"))
    cmds.append(BotCommand(command="admindel", description="Удалить заявку по ID (superadmin)"))

    try:
        await bot.set_my_commands(cmds)
        log.info("Telegram command menu configured")
    except Exception as e:
        log.warning("Failed to set bot commands: %s", e)

    # Запуск напоминаний, только если включены
    if (
        getattr(settings, "director_tg_id", None)
        and int(getattr(settings, "remind_check_seconds", 0)) > 0
        and int(getattr(settings, "remind_after_minutes", 0)) > 0
        and int(getattr(settings, "remind_repeat_minutes", 0)) > 0
    ):
        asyncio.create_task(director_reminder_loop(bot, db, settings))
    else:
        log.info("Director reminders disabled (check REMIND_* settings).")


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    settings = load_settings()

    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher()
    db = Database(settings.db_path)

    dp.update.middleware(PinAuthMiddleware())
    dp.include_router(router)

    dp.startup.register(startup)

    log.info("Bot started")
    await dp.start_polling(bot, db=db, settings=settings)


if __name__ == "__main__":
    asyncio.run(main())
