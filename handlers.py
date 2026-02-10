# handlers.py
from __future__ import annotations

import logging
from typing import Any, Optional, Set, List, Dict

from aiogram import F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import COMPANIES, COMPANY_TOKEN_MAP, TOKEN_AVAILABLE
from db import Database
from utils import (
    append_journal_row,
    kb_director_decision,
    kb_officer_actions,
    request_card_text,
    unpack_cb,
    pack_cb,
    is_director,
    is_officer,
    is_superadmin,
    kb_admin_menu,
    safe_edit_text,
    format_statistics,
    format_token_list,
    kb_back_to_admin,
    webdav_healthcheck,
)

log = logging.getLogger(__name__)
router = Router()

# -------------------------
# Меню
# -------------------------
BTN_REQUEST = "✅ Создать заявку"
BTN_MY = "📋 Мои заявки"
BTN_PENDING = "🧑‍💼 Директор: На согласовании"
BTN_ACTIVE = "🛡 Уполномоченный: Активные"
BTN_HELP = "ℹ️ Помощь"
BTN_PROFILE = "🪪 Профиль (ФИО)"
BTN_TOKENS = "🔑 Статусы токенов"
BTN_CANCEL = "❌ Отмена"


def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_REQUEST)],
            [KeyboardButton(text=BTN_MY)],
            [KeyboardButton(text=BTN_PENDING)],
            [KeyboardButton(text=BTN_ACTIVE)],
            [KeyboardButton(text=BTN_HELP), KeyboardButton(text=BTN_PROFILE)],
            [KeyboardButton(text=BTN_TOKENS)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выберите действие…",
    )


def help_text() -> str:
    return (
        "📋 <b>Как пользоваться ботом:</b>\n\n"
        f"• <b>{BTN_REQUEST}</b> — оформить заявку (можно выбрать несколько компаний)\n"
        f"• <b>{BTN_MY}</b> — просмотреть свои заявки\n"
        f"• <b>{BTN_PENDING}</b> — раздел для директора (согласование заявок)\n"
        f"• <b>{BTN_ACTIVE}</b> — раздел для уполномоченного (выдача/приём токенов)\n"
        f"• <b>{BTN_HELP}</b> — показать эту справку\n"
        f"• <b>{BTN_PROFILE}</b> — заполнить/обновить ФИО\n"
        f"• <b>{BTN_TOKENS}</b> — посмотреть какие токены свободны/заняты\n"
        "• <b>/tokens</b> — то же действие командой\n"
        "• Если токен занят, вы автоматически встанете в очередь и получите уведомление, когда он освободится\n"
        "• <b>/profile</b> — то же действие командой\n"
        f"• <b>{BTN_CANCEL}</b> — отменить текущее действие\n\n"
        "Если что-то не получается — напишите системному администратору."
    )


# -------------------------
# States / FSM
# -------------------------
class RequestFSM(StatesGroup):
    full_name = State()
    companies = State()
    purpose = State()


# -------------------------
# Helpers
# -------------------------
def kb_companies_multi(selected_idx: Set[int], max_selection: int = 5) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()

    for idx, name in enumerate(COMPANIES):
        checked = "☑️" if idx in selected_idx else "⬜️"
        b.add(InlineKeyboardButton(text=f"{checked} {name}", callback_data=pack_cb("cmpt", str(idx))))

    b.add(InlineKeyboardButton(text="✅ Готово", callback_data=pack_cb("cmpdone", "1")))
    b.add(
        InlineKeyboardButton(text="Выбрать все", callback_data=pack_cb("cmpall", "1")),
        InlineKeyboardButton(text="Снять все", callback_data=pack_cb("cmpnone", "1")),
    )

    if selected_idx:
        selection_info = f"Выбрано: {len(selected_idx)}/{max_selection}"
    else:
        selection_info = f"Выберите компании (макс. {max_selection})"

    b.add(InlineKeyboardButton(text=selection_info, callback_data="info"))
    b.adjust(1)
    return b.as_markup()


async def safe_append_journal(
        *,
        settings,
        request_row: Any,
        action: str,
        actor_tg_id: int,
        request_items: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Пишет строку в XLSX-журнал в Nextcloud WebDAV.
    Ничего не валит: ошибки только в лог.
    """
    try:
        if not settings.nc_webdav_url or not settings.nc_user or not settings.nc_app_password or not settings.journal_path:
            return

        await append_journal_row(
            webdav_url=settings.nc_webdav_url,
            nc_user=settings.nc_user,
            nc_app_password=settings.nc_app_password,
            journal_path=settings.journal_path,
            request_row=request_row,
            action=action,
            actor_tg_id=actor_tg_id,
            request_items=request_items,
        )
    except Exception as e:
        log.warning("Journal append failed: %s: %s", type(e).__name__, e)


def _build_tokens_status_text(tokens: List[Dict[str, Any]], user_waitlist: List[Dict[str, Any]]) -> str:
    status_by_token = {str(t.get("token_id")): str(t.get("status", "unknown")) for t in tokens}
    lines = ["🔑 <b>Статусы токенов по компаниям</b>", ""]

    for company in COMPANIES:
        token_id = COMPANY_TOKEN_MAP.get(company, "-")
        token_status = status_by_token.get(token_id, "unknown")
        status_human = {
            "available": "✅ свободен",
            "reserved": "🟡 занят (ожидает выдачи)",
            "issued": "📦 выдан",
        }.get(token_status, f"❓ {token_status}")
        lines.append(f"• <b>{company}</b> — <code>{token_id}</code> — {status_human}")

    if user_waitlist:
        lines.extend(["", "⏳ <b>Вы в очереди:</b>"])
        for idx, row in enumerate(user_waitlist, start=1):
            company = row.get("company") or "(компания не указана)"
            token_id = row.get("token_id") or "-"
            lines.append(f"{idx}. {company} — <code>{token_id}</code>")

    return "\n".join(lines)


async def notify_waiters_for_tokens(bot, db: Database, token_ids: List[str]) -> None:
    rows = await db.pop_waiters_for_available_tokens(token_ids)
    for row in rows:
        try:
            await bot.send_message(
                int(row["tg_id"]),
                "🔔 <b>Токен освободился</b>\n\n"
                f"Компания: <b>{row.get('company') or '-'}</b>\n"
                f"Токен: <code>{row.get('token_id') or '-'}</code>\n\n"
                "Теперь вы можете создать новую заявку.",
            )
        except Exception as e:
            log.warning("Failed to notify waitlist user %s: %s", row.get("tg_id"), e)


# -------------------------
# Commands
# -------------------------
@router.message(CommandStart())
async def cmd_start(message: Message, db: Database) -> None:
    welcome_text = (
        "👋 <b>Приветствуем в системе учёта USB-носителей с ЭЦП!</b>\n\n"
        "Я помогу вам управлять заявками на выдачу токенов для подписи документов.\n\n"
        "Для начала работы используйте кнопки меню ниже."
    )
    await message.answer(welcome_text, reply_markup=main_menu_kb())
    await message.answer(help_text(), reply_markup=main_menu_kb())

    full_name = await db.get_user_full_name(message.from_user.id)
    if not full_name:
        await message.answer(
            "⚠️ Для работы с заявками обязательно заполните ФИО: нажмите кнопку «🪪 Профиль (ФИО)»."
        )


@router.message(Command("menu"))
async def cmd_menu(message: Message) -> None:
    await message.answer("📱 <b>Главное меню</b> 👇", reply_markup=main_menu_kb())


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(help_text(), reply_markup=main_menu_kb())


async def _ask_full_name(message: Message, state: FSMContext, *, next_step: str) -> None:
    await state.set_state(RequestFSM.full_name)
    await state.update_data(next_step=next_step)
    await message.answer(
        "🪪 <b>Профиль пользователя</b>\n\n"
        "Введите ваше ФИО (обязательно):\n"
        "<i>Фамилия Имя Отчество</i>\n\n"
        "ФИО будет привязано к вашему tg_id и отображаться в заявках."
    )


@router.message(Command("profile"))
async def cmd_profile(message: Message, state: FSMContext) -> None:
    await _ask_full_name(message, state, next_step="menu")


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Нечего отменять.", reply_markup=main_menu_kb())
        return

    await state.clear()
    await message.answer("❌ Действие отменено.", reply_markup=main_menu_kb())

@router.message(Command("request"))
async def cmd_request_alias(message: Message, state: FSMContext, settings, db: Database) -> None:
    # Шорткат к кнопке "✅ Создать заявку"
    await cmd_request(message, state, settings, db)


@router.message(Command("my"))
async def cmd_my_alias(message: Message, db: Database) -> None:
    # Шорткат к кнопке "📋 Мои заявки"
    await cmd_my(message, db)


@router.message(Command("pending"))
async def cmd_pending_alias(message: Message, db: Database, settings) -> None:
    # Шорткат к разделу директора
    await cmd_pending(message, db, settings)


@router.message(Command("active"))
async def cmd_active_alias(message: Message, db: Database, settings) -> None:
    # Шорткат к разделу уполномоченного
    await cmd_active(message, db, settings)


@router.message(Command("tokens"))
async def cmd_tokens(message: Message, db: Database) -> None:
    tokens = await db.list_all_tokens()
    user_waitlist = await db.list_user_waitlist(message.from_user.id, limit=20)
    await message.answer(_build_tokens_status_text(tokens, user_waitlist), reply_markup=main_menu_kb())

# -------------------------
# Menu buttons
# -------------------------
@router.message(F.text == BTN_HELP)
async def btn_help(message: Message) -> None:
    await cmd_help(message)


@router.message(F.text == BTN_PROFILE)
async def btn_profile(message: Message, state: FSMContext) -> None:
    await cmd_profile(message, state)


@router.message(F.text == BTN_TOKENS)
async def btn_tokens(message: Message, db: Database) -> None:
    await cmd_tokens(message, db)


@router.message(F.text == BTN_CANCEL)
async def btn_cancel(message: Message, state: FSMContext) -> None:
    await cmd_cancel(message, state)


@router.message(F.text == BTN_REQUEST)
async def btn_request(message: Message, state: FSMContext, settings, db: Database) -> None:
    await cmd_request(message, state, settings, db)


@router.message(F.text == BTN_MY)
async def btn_my(message: Message, db: Database) -> None:
    await cmd_my(message, db)


@router.message(F.text == BTN_PENDING)
async def btn_pending(message: Message, db: Database, settings) -> None:
    await cmd_pending(message, db, settings)


@router.message(F.text == BTN_ACTIVE)
async def btn_active(message: Message, db: Database, settings) -> None:
    await cmd_active(message, db, settings)


# -------------------------
# Flows
# -------------------------
async def cmd_my(message: Message, db: Database) -> None:
    try:
        rows = await db.list_requests_by_user(message.from_user.id, limit=20)
        if not rows:
            await message.answer(
                "📭 <b>У вас пока нет заявок.</b>\n\n"
                "Нажмите «✅ Создать заявку» для оформления новой.",
                reply_markup=main_menu_kb()
            )
            return

        lines = ["📋 <b>Ваши заявки:</b>\n"]
        for r in rows:
            items = await db.get_request_items(r.id)
            lines.append(request_card_text(r, items))
            lines.append("—" * 20)

        await message.answer("\n".join(lines), reply_markup=main_menu_kb())
    except Exception as e:
        log.error("Error in cmd_my: %s", e)
        await message.answer("Ошибка при загрузке заявок.", reply_markup=main_menu_kb())


async def _start_request_companies_step(message: Message, state: FSMContext, settings) -> None:
    await state.set_state(RequestFSM.companies)
    await state.update_data(selected_companies=[])
    max_companies = getattr(settings, "max_companies_per_request", 5)

    await message.answer(
        f"📋 <b>Создание заявки</b>\n\n"
        f"Выберите компании (можно несколько, максимум {max_companies}).",
        reply_markup=kb_companies_multi(set(), max_companies),
    )


async def cmd_request(message: Message, state: FSMContext, settings, db: Database) -> None:
    await state.clear()

    full_name = await db.get_user_full_name(message.from_user.id)
    if not full_name:
        await state.set_state(RequestFSM.full_name)
        await state.update_data(next_step="request")
        await message.answer(
            "🪪 <b>Идентификация пользователя</b>\n\n"
            "Перед первой заявкой укажите ваше ФИО (например: <i>Иванов Иван Иванович</i>).\n"
            "Это ФИО будет привязано к вашему tg_id и отображаться в заявках."
        )
        return

    await _start_request_companies_step(message, state, settings)


async def cmd_pending(message: Message, db: Database, settings) -> None:
    if not is_director(message.from_user.id, settings):
        await message.answer("⛔ Доступ только для директора.", reply_markup=main_menu_kb())
        return

    rows = await db.list_pending_for_director(limit=20)
    if not rows:
        await message.answer("Нет заявок на согласовании.", reply_markup=main_menu_kb())
        return

    await message.answer(f"🧑‍💼 <b>На согласовании:</b> {len(rows)} заявок")

    for r in rows:
        items = await db.get_request_items(r.id)
        await message.answer(
            request_card_text(r, items),
            reply_markup=kb_director_decision(r.id),
        )


async def cmd_active(message: Message, db: Database, settings) -> None:
    if not is_officer(message.from_user.id, settings):
        await message.answer("⛔ Доступ только для уполномоченного.", reply_markup=main_menu_kb())
        return

    approved = await db.list_active_for_officer(limit=30)
    issued = await db.list_requests_by_status("ISSUED", limit=30)
    rows = approved + issued

    if not rows:
        await message.answer("Нет активных заявок.", reply_markup=main_menu_kb())
        return

    await message.answer(f"🛡 <b>Активные заявки:</b> {len(rows)}")

    for r in rows:
        items = await db.get_request_items(r.id)
        await message.answer(
            request_card_text(r, items),
            reply_markup=kb_officer_actions(r.id, r.status),
        )


@router.message(RequestFSM.full_name)
async def msg_full_name(message: Message, state: FSMContext, db: Database, settings) -> None:
    full_name = " ".join((message.text or "").strip().split())
    if len(full_name) < 5:
        await message.answer("ФИО слишком короткое. Пример: Иванов Иван Иванович")
        return
    if len(full_name) > 120:
        await message.answer("ФИО слишком длинное. Максимум 120 символов.")
        return
    if " " not in full_name:
        await message.answer("Укажите минимум имя и фамилию через пробел.")
        return

    data = await state.get_data()
    next_step = data.get("next_step", "menu")

    await db.set_user_full_name(message.from_user.id, full_name)
    await message.answer(f"✅ ФИО сохранено: <b>{full_name}</b>")

    if next_step == "request":
        await _start_request_companies_step(message, state, settings)
        return

    await state.clear()
    await message.answer("📱 <b>Главное меню</b> 👇", reply_markup=main_menu_kb())


# -------------------------
# Companies selection (callbacks)
# -------------------------
@router.callback_query(F.data.startswith("act:cmpt:"), RequestFSM.companies)
async def cb_company_toggle(callback: CallbackQuery, state: FSMContext, settings) -> None:
    try:
        action, value = unpack_cb(callback.data)
        if action != "cmpt":
            return
        idx = int(value)
        if idx < 0 or idx >= len(COMPANIES):
            await callback.answer("Некорректная компания", show_alert=True)
            return
    except Exception:
        await callback.answer("Ошибка данных", show_alert=True)
        return

    data = await state.get_data()
    selected: List[int] = data.get("selected_companies", [])
    selected_set = set(int(x) for x in selected)

    max_companies = getattr(settings, 'max_companies_per_request', 5)

    if idx in selected_set:
        selected_set.remove(idx)
    else:
        if len(selected_set) >= max_companies:
            await callback.answer(f"Максимум {max_companies} компаний", show_alert=True)
            return
        selected_set.add(idx)

    await state.update_data(selected_companies=sorted(selected_set))
    await safe_edit_text(callback, "✅ Выберите компании:", reply_markup=kb_companies_multi(selected_set, max_companies))
    await callback.answer()


@router.callback_query(F.data.startswith("act:cmpall:"), RequestFSM.companies)
async def cb_company_all(callback: CallbackQuery, state: FSMContext, settings) -> None:
    max_companies = getattr(settings, 'max_companies_per_request', 5)
    selected_set = set(range(min(len(COMPANIES), max_companies)))
    await state.update_data(selected_companies=sorted(selected_set))
    await safe_edit_text(callback, "✅ Выберите компании:", reply_markup=kb_companies_multi(selected_set, max_companies))
    await callback.answer()


@router.callback_query(F.data.startswith("act:cmpnone:"), RequestFSM.companies)
async def cb_company_none(callback: CallbackQuery, state: FSMContext, settings) -> None:
    max_companies = getattr(settings, 'max_companies_per_request', 5)
    await state.update_data(selected_companies=[])
    await safe_edit_text(callback, "✅ Выберите компании:", reply_markup=kb_companies_multi(set(), max_companies))
    await callback.answer()


@router.callback_query(F.data.startswith("act:cmpdone:"), RequestFSM.companies)
async def cb_company_done(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    selected: List[int] = data.get("selected_companies", [])
    if not selected:
        await callback.answer("Сначала выберите хотя бы одну компанию", show_alert=True)
        return

    companies = [COMPANIES[i] for i in selected]
    await state.update_data(companies=companies)
    await state.set_state(RequestFSM.purpose)

    await safe_edit_text(
        callback,
        "🎯 <b>Введите цель</b> (зачем нужна флешка/ЭЦП):\n\n"
        "Например: Подписание договора, отчётность, банк-клиент и т.д.",
        reply_markup=None
    )
    await callback.answer()


# -------------------------
# Purpose
# -------------------------
@router.message(RequestFSM.purpose)
async def msg_purpose(message: Message, state: FSMContext, db: Database, settings) -> None:
    purpose = (message.text or "").strip()
    max_length = getattr(settings, 'max_purpose_length', 500)
    if not purpose:
        await message.answer("Цель не может быть пустой. Напишите цель.")
        return
    if len(purpose) > max_length:
        await message.answer(f"Слишком длинно. Максимум {max_length} символов.")
        return

    data = await state.get_data()
    companies: List[str] = data.get("companies") or []

    if not companies:
        await state.clear()
        await message.answer("Ошибка состояния. Начните заново.", reply_markup=main_menu_kb())
        return

    missing_companies = [c for c in companies if c not in COMPANY_TOKEN_MAP]
    if missing_companies:
        log.error("Missing token mapping for companies: %s", missing_companies)
        await state.clear()
        await message.answer(
            "Ошибка конфигурации: для части компаний не настроены токены. Сообщите администратору.",
            reply_markup=main_menu_kb(),
        )
        return

    items = [(c, COMPANY_TOKEN_MAP[c]) for c in companies]

    from_user = message.from_user
    fallback_username = ""
    if from_user:
        fallback_username = from_user.full_name or from_user.username or ""

    try:
        request_id = await db.create_request_multi(
            tg_id=message.from_user.id,
            username=(await db.get_user_full_name(message.from_user.id))
            or fallback_username,
            items=items,
            purpose=purpose,
            comment=None,
        )
    except RuntimeError as e:
        err = str(e)
        if err.startswith("TOKEN_NOT_AVAILABLE:"):
            token_id = err.split(":", 1)[1].strip()
            company = next((c for c, t in items if t == token_id), "Неизвестная компания")
            joined = await db.join_waitlist(message.from_user.id, token_id, company)
            await state.clear()
            await message.answer(
                "⛔ <b>Токен сейчас занят.</b>\n\n"
                f"Компания: <b>{company}</b>\n"
                f"Токен: <code>{token_id}</code>\n\n"
                + ("✅ Вы добавлены в очередь и получите уведомление, когда токен освободится."
                   if joined else "ℹ️ Вы уже в очереди на этот токен. Уведомим, когда он освободится."),
                reply_markup=main_menu_kb(),
            )
            return

        log.error("create_request_multi failed: %s", e)
        await state.clear()
        await message.answer("Ошибка создания заявки. Попробуйте ещё раз.", reply_markup=main_menu_kb())
        return
    except Exception as e:
        log.error("create_request_multi failed: %s", e)
        await state.clear()
        await message.answer("Ошибка создания заявки. Попробуйте ещё раз.", reply_markup=main_menu_kb())
        return

    await state.clear()
    await message.answer(
        f"✅ <b>Заявка создана</b> (# {request_id}).\n"
        "Ожидайте решения директора.",
        reply_markup=main_menu_kb()
    )

    # Notify director
    try:
        req = await db.get_request(request_id)
        if req:
            req_items = await db.get_request_items(request_id)
            await message.bot.send_message(
                chat_id=settings.director_tg_id,
                text="🧑‍💼 <b>Новая заявка на согласование</b>\n\n" + request_card_text(req, req_items),
                reply_markup=kb_director_decision(req.id),
            )
            await safe_append_journal(
                settings=settings,
                request_row=req,
                action="REQUESTED",
                actor_tg_id=message.from_user.id,
                request_items=req_items,
            )
    except Exception as e:
        log.warning("Failed to notify director: %s: %s", type(e).__name__, e)


# -------------------------
# Director callbacks
# -------------------------
@router.callback_query(F.data.startswith("act:apr:"))
async def cb_director_approve(callback: CallbackQuery, db: Database, settings) -> None:
    if not is_director(callback.from_user.id, settings):
        await callback.answer("Только директор может это делать.", show_alert=True)
        return

    try:
        action, value = unpack_cb(callback.data)
        if action != "apr":
            return
        request_id = int(value)
    except Exception:
        await callback.answer("Ошибка данных", show_alert=True)
        return

    try:
        req = await db.director_decide(request_id, director_tg_id=callback.from_user.id, approve=True)
        if not req:
            await callback.answer("Заявка не найдена", show_alert=True)
            return
    except RuntimeError as e:
        if "INVALID_STATUS" in str(e):
            await callback.answer("Заявка уже обработана или имеет неверный статус", show_alert=True)
            return
        log.error("Error in director_decide: %s", e)
        await callback.answer("Ошибка при обработке", show_alert=True)
        return
    except Exception as e:
        log.error("Error in director_decide: %s", e)
        await callback.answer("Ошибка при обработке", show_alert=True)
        return

    items = await db.get_request_items(request_id)

    await callback.answer("✅ Одобрено")
    try:
        await callback.message.edit_text("✅ <b>Заявка одобрена</b>\n\n" + request_card_text(req, items))
    except Exception:
        pass

    try:
        await callback.bot.send_message(
            chat_id=req.tg_id,
            text="🎉 <b>Ваша заявка одобрена директором!</b>\n\n" + request_card_text(req, items)
        )
    except Exception as e:
        log.warning("Failed to notify user: %s: %s", type(e).__name__, e)

    try:
        await callback.bot.send_message(
            chat_id=settings.officer_tg_id,
            text="🟢 <b>Новая заявка на выдачу</b>\n\n" + request_card_text(req, items),
            reply_markup=kb_officer_actions(req.id, req.status),
        )
    except Exception as e:
        log.warning("Failed to notify officer: %s: %s", type(e).__name__, e)

    await safe_append_journal(
        settings=settings,
        request_row=req,
        action="APPROVED",
        actor_tg_id=callback.from_user.id,
        request_items=items,
    )


@router.callback_query(F.data.startswith("act:rej:"))
async def cb_director_reject(callback: CallbackQuery, db: Database, settings) -> None:
    if not is_director(callback.from_user.id, settings):
        await callback.answer("Только директор может это делать.", show_alert=True)
        return

    try:
        action, value = unpack_cb(callback.data)
        if action != "rej":
            return
        request_id = int(value)
    except Exception:
        await callback.answer("Ошибка данных", show_alert=True)
        return

    try:
        req = await db.director_decide(request_id, director_tg_id=callback.from_user.id, approve=False)
        if not req:
            await callback.answer("Заявка не найдена", show_alert=True)
            return
    except RuntimeError as e:
        if "INVALID_STATUS" in str(e):
            await callback.answer("Заявка уже обработана или имеет неверный статус", show_alert=True)
            return
        log.error("Error in director_decide: %s", e)
        await callback.answer("Ошибка при обработке", show_alert=True)
        return
    except Exception as e:
        log.error("Error in director_decide: %s", e)
        await callback.answer("Ошибка при обработке", show_alert=True)
        return

    items = await db.get_request_items(request_id)

    await callback.answer("❌ Отклонено")
    try:
        await callback.message.edit_text("❌ <b>Заявка отклонена</b>\n\n" + request_card_text(req, items))
    except Exception:
        pass

    try:
        await callback.bot.send_message(
            chat_id=req.tg_id,
            text="😔 <b>Ваша заявка отклонена директором</b>\n\n" + request_card_text(req, items)
        )
    except Exception as e:
        log.warning("Failed to notify user: %s: %s", type(e).__name__, e)

    await safe_append_journal(
        settings=settings,
        request_row=req,
        action="REJECTED",
        actor_tg_id=callback.from_user.id,
        request_items=items,
    )

    await notify_waiters_for_tokens(callback.bot, db, [it.get("token_id") for it in items])


# -------------------------
# Officer callbacks
# -------------------------
@router.callback_query(F.data.startswith("act:iss:"))
async def cb_officer_issued(callback: CallbackQuery, db: Database, settings) -> None:
    if not is_officer(callback.from_user.id, settings):
        await callback.answer("Только уполномоченный может это делать.", show_alert=True)
        return

    try:
        action, value = unpack_cb(callback.data)
        if action != "iss":
            return
        request_id = int(value)
    except Exception:
        await callback.answer("Ошибка данных", show_alert=True)
        return

    try:
        req = await db.officer_issue(request_id, officer_tg_id=callback.from_user.id)
        if not req:
            await callback.answer("Заявка не найдена", show_alert=True)
            return
    except RuntimeError as e:
        if "INVALID_STATUS" in str(e):
            await callback.answer("Неверный статус заявки", show_alert=True)
            return
        log.error("Error in officer_issue: %s", e)
        await callback.answer("Ошибка при обработке", show_alert=True)
        return
    except Exception as e:
        log.error("Error in officer_issue: %s", e)
        await callback.answer("Ошибка при обработке", show_alert=True)
        return

    items = await db.get_request_items(request_id)

    await callback.answer("📦 Выдано")
    try:
        await callback.message.edit_text("📦 <b>Токены выданы</b>\n\n" + request_card_text(req, items))
    except Exception:
        pass

    try:
        await callback.bot.send_message(
            chat_id=req.tg_id,
            text="📦 <b>Вам выдали токены</b>\n\n" + request_card_text(req, items)
        )
    except Exception as e:
        log.warning("Failed to notify user: %s: %s", type(e).__name__, e)

    await safe_append_journal(
        settings=settings,
        request_row=req,
        action="ISSUED",
        actor_tg_id=callback.from_user.id,
        request_items=items,
    )


@router.callback_query(F.data.startswith("act:ret:"))
async def cb_officer_returned(callback: CallbackQuery, db: Database, settings) -> None:
    if not is_officer(callback.from_user.id, settings):
        await callback.answer("Только уполномоченный может это делать.", show_alert=True)
        return

    try:
        action, value = unpack_cb(callback.data)
        if action != "ret":
            return
        request_id = int(value)
    except Exception:
        await callback.answer("Ошибка данных", show_alert=True)
        return

    try:
        req = await db.officer_return(request_id, officer_tg_id=callback.from_user.id)
        if not req:
            await callback.answer("Заявка не найдена", show_alert=True)
            return
    except RuntimeError as e:
        if "INVALID_STATUS" in str(e):
            await callback.answer("Неверный статус заявки", show_alert=True)
            return
        log.error("Error in officer_return: %s", e)
        await callback.answer("Ошибка при обработке", show_alert=True)
        return
    except Exception as e:
        log.error("Error in officer_return: %s", e)
        await callback.answer("Ошибка при обработке", show_alert=True)
        return

    items = await db.get_request_items(request_id)

    await callback.answer("✅ Принято")
    try:
        await callback.message.edit_text("✅ <b>Токены возвращены</b>\n\n" + request_card_text(req, items))
    except Exception:
        pass

    try:
        await callback.bot.send_message(
            chat_id=req.tg_id,
            text="✅ <b>Токены приняты (возврат оформлен)</b>\n\n" + request_card_text(req, items)
        )
    except Exception as e:
        log.warning("Failed to notify user: %s: %s", type(e).__name__, e)

    await safe_append_journal(
        settings=settings,
        request_row=req,
        action="RETURNED",
        actor_tg_id=callback.from_user.id,
        request_items=items,
    )

    await notify_waiters_for_tokens(callback.bot, db, [it.get("token_id") for it in items])


# -------------------------
# PIN + Admin
# -------------------------
@router.message(Command("pin"))
async def cmd_pin(message: Message, db: Database, settings) -> None:
    if not settings.bot_pin:
        await message.answer("PIN не настроен.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /pin 1234")
        return

    code = parts[1].strip()
    if code == str(settings.bot_pin).strip():
        await db.set_authed(message.from_user.id)
        await message.answer("✅ Авторизация выполнена.")
    else:
        await message.answer("❌ Неверный PIN.")


@router.message(Command("admin"))
async def cmd_admin(message: Message, settings) -> None:
    uid = message.from_user.id
    if not is_superadmin(uid, settings):
        await message.answer("⛔ Нет доступа.")
        return
    await message.answer("🛠 <b>Админ-панель</b>", reply_markup=kb_admin_menu())


@router.message(Command("admindel"))
async def cmd_admindel(message: Message, db: Database, settings) -> None:
    uid = message.from_user.id
    if not is_superadmin(uid, settings):
        await message.answer("⛔ Нет доступа.")
        return

    parts = (message.text or "").strip().split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await message.answer(
            "Использование: <code>/admindel ID_ЗАЯВКИ</code>\n"
            "Например: <code>/admindel 123</code>"
        )
        return

    request_id = int(parts[1].strip())
    deleted = await db.delete_request_by_admin(request_id=request_id, actor_tg_id=uid)
    if not deleted:
        await message.answer(f"Заявка #{request_id} не найдена.")
        return

    await message.answer(
        f"🗑 Заявка <b>#{request_id}</b> удалена.\n"
        "Связанные токены возвращены в состояние available."
    )


@router.callback_query(F.data.startswith("adm:"))
async def cb_admin(call: CallbackQuery, db: Database, settings) -> None:
    uid = call.from_user.id
    if not is_superadmin(uid, settings):
        await call.answer("Нет доступа", show_alert=True)
        return

    data = (call.data or "").strip()

    if data == "adm:menu":
        await safe_edit_text(call, "🛠 <b>Админ-панель</b>", reply_markup=kb_admin_menu())
        await call.answer()
        return

    if data == "adm:stats":
        stats = await db.get_statistics()
        await safe_edit_text(call, format_statistics(stats), reply_markup=kb_back_to_admin())
        await call.answer()
        return

    if data == "adm:tokens":
        tokens = await db.list_all_tokens()
        await safe_edit_text(call, format_token_list(tokens), reply_markup=kb_back_to_admin())
        await call.answer()
        return

    if data == "adm:authed":
        users = await db.list_authed_users(limit=100)
        if not users:
            await safe_edit_text(call, "Нет авторизованных пользователей.", reply_markup=kb_back_to_admin())
            await call.answer()
            return

        text = ["👥 <b>Авторизованные пользователи</b>", ""]
        for row in users:
            text.append(
                f"• tg_id: <code>{row['tg_id']}</code> — "
                f"{row.get('authed_at') or 'неизвестно'}"
            )

        await safe_edit_text(call, "\n".join(text), reply_markup=kb_back_to_admin())
        await call.answer()
        return

    if data == "adm:pending":
        rows = await db.list_requests_by_status("REQUESTED", limit=20)
        if not rows:
            await safe_edit_text(call, "Нет заявок на согласовании.", reply_markup=kb_back_to_admin())
            await call.answer()
            return

        text = ["🧑‍💼 <b>На согласовании</b>\n"]
        for r in rows:
            items = await db.get_request_items(r.id)
            text.append(request_card_text(r, items))
            text.append("—" * 20)

        await safe_edit_text(call, "\n".join(text), reply_markup=kb_back_to_admin())
        await call.answer()
        return

    if data == "adm:approved":
        rows = await db.list_requests_by_status("APPROVED", limit=20)
        if not rows:
            await safe_edit_text(call, "Нет одобренных заявок.", reply_markup=kb_back_to_admin())
            await call.answer()
            return
        text = ["✅ <b>Одобрено</b>\n"]
        for r in rows:
            items = await db.get_request_items(r.id)
            text.append(request_card_text(r, items))
            text.append("—" * 20)
        await safe_edit_text(call, "\n".join(text), reply_markup=kb_back_to_admin())
        await call.answer()
        return

    if data == "adm:issued":
        rows = await db.list_requests_by_status("ISSUED", limit=20)
        if not rows:
            await safe_edit_text(call, "Нет выданных (на руках) заявок.", reply_markup=kb_back_to_admin())
            await call.answer()
            return
        text = ["📦 <b>Выдано (на руках)</b>\n"]
        for r in rows:
            items = await db.get_request_items(r.id)
            text.append(request_card_text(r, items))
            text.append("—" * 20)
        await safe_edit_text(call, "\n".join(text), reply_markup=kb_back_to_admin())
        await call.answer()
        return

    if data == "adm:active":
        rows_a = await db.list_requests_by_status("APPROVED", limit=20)
        rows_i = await db.list_requests_by_status("ISSUED", limit=20)
        rows = rows_a + rows_i
        if not rows:
            await safe_edit_text(call, "Нет активных заявок.", reply_markup=kb_back_to_admin())
            await call.answer()
            return
        text = ["🟢 <b>Активные</b>\n"]
        for r in rows:
            items = await db.get_request_items(r.id)
            text.append(request_card_text(r, items))
            text.append("—" * 20)
        await safe_edit_text(call, "\n".join(text), reply_markup=kb_back_to_admin())
        await call.answer()
        return

    if data == "adm:last20":
        rows = await db.list_last_requests(limit=20)
        text = ["🕒 <b>Последние 20 заявок</b>\n"]
        for r in rows:
            items = await db.get_request_items(r.id)
            text.append(request_card_text(r, items))
            text.append("—" * 20)
        await safe_edit_text(call, "\n".join(text), reply_markup=kb_back_to_admin())
        await call.answer()
        return

    if data.startswith("adm:over:"):
        try:
            sec = int(data.split(":")[-1])
        except Exception:
            sec = 1800
        rows = await db.pending_over_seconds(sec)
        if not rows:
            await safe_edit_text(call, "Нет просроченных заявок.", reply_markup=kb_back_to_admin())
            await call.answer()
            return
        text = [f"⏱ <b>Просроченные заявки</b> (>{sec} сек)\n"]
        for r in rows:
            items = await db.get_request_items(r.id)
            text.append(request_card_text(r, items))
            text.append("—" * 20)
        await safe_edit_text(call, "\n".join(text), reply_markup=kb_back_to_admin())
        await call.answer()
        return

    if data == "adm:delete_help":
        rows = await db.list_last_requests(limit=20)
        lines = [
            "🗑 <b>Удаление заявки (superadmin)</b>",
            "",
            "Команда: <code>/admindel ID_ЗАЯВКИ</code>",
            "",
            "Последние ID:",
        ]
        if rows:
            lines.extend([f"• #{r.id} — {r.status}" for r in rows])
        else:
            lines.append("(заявок пока нет)")

        await safe_edit_text(call, "\n".join(lines), reply_markup=kb_back_to_admin())
        await call.answer()
        return

    if data == "adm:cleanup":
        deleted = await db.cleanup_old_data(days=90)
        await safe_edit_text(call, f"🧹 Удалено записей: {deleted}", reply_markup=kb_back_to_admin())
        await call.answer()
        return

    if data == "adm:webdav":
        ok, msg = await webdav_healthcheck(
            settings.nc_webdav_url,
            settings.nc_user,
            settings.nc_app_password,
            settings.journal_path
        )
        await safe_edit_text(call, f"🔄 WebDAV: {'✅' if ok else '❌'}\n{msg}", reply_markup=kb_back_to_admin())
        await call.answer()
        return

    await call.answer("Неизвестная команда", show_alert=True)
