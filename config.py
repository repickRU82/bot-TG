from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional
import os

from dotenv import load_dotenv

# -------------------------
# Load .env (SAFE)
# -------------------------
# В проде .env может отсутствовать — это нормально
load_dotenv(override=False)


# -------------------------
# Companies / Tokens
# -------------------------
COMPANIES: List[str] = [
    "ООО Кустос",
    "ООО Поле",
    "ООО ТЭК",
    "ООО Агровита",
    "ООО ТДФ",
    "ООО КЭК",
    "ООО Сиваш Грейн",
    "ООО Деметра Агро",
    "Красногвардейский Элеватор",
    "ИП Тишков ИП",
    "ИП Ганага М.В",
    "ИП Ганага Г.И",
    "КФХ Аграрное",
    "ООО Мир Агро",
    "ИП Малецкий",
    "ОТЭ",
    "Клуб СБ Фрегат",
]

COMPANY_TOKEN_MAP: Dict[str, str] = {
    "ООО Кустос": "KEY-01",
    "ООО Поле": "KEY-02",
    "ООО ТЭК": "KEY-03",
    "ООО Агровита": "KEY-04",
    "ООО ТДФ": "KEY-05",
    "ООО КЭК": "KEY-06",
    "ООО Сиваш Грейн": "KEY-07",
    "ООО Деметра Агро": "KEY-08",
    "Красногвардейский Элеватор": "KEY-09",
    "ИП Тишков ИП": "KEY-10",
    "ИП Ганага М.В": "KEY-11",
    "ИП Ганага Г.И": "KEY-12",
    "КФХ Аграрное": "KEY-13",
    "ООО Мир Агро": "KEY-14",
    "ИП Малецкий": "KEY-15",
    "ОТЭ": "KEY-16",
    "Клуб СБ Фрегат": "KEY-17",
}


# -------------------------
# Status constants
# -------------------------
STATUS_REQUESTED = "REQUESTED"
STATUS_APPROVED = "APPROVED"
STATUS_REJECTED = "REJECTED"
STATUS_ISSUED = "ISSUED"
STATUS_RETURNED = "RETURNED"

TOKEN_AVAILABLE = "available"
TOKEN_ISSUED = "issued"
TOKEN_RESERVED = "reserved"


# -------------------------
# Settings dataclass
# -------------------------
@dataclass(frozen=True)
class Settings:
    bot_token: str
    director_tg_id: int
    officer_tg_id: int

    # Auth
    bot_pin: Optional[str]
    superadmin_ids: List[int]

    # Reminders
    remind_after_minutes: int
    remind_repeat_minutes: int
    remind_check_seconds: int

    # Nextcloud WebDAV
    nc_webdav_url: str
    nc_user: str
    nc_app_password: str
    journal_path: str

    # Database
    db_path: Path

    # Limits
    max_companies_per_request: int
    max_purpose_length: int
    max_comment_length: int


# -------------------------
# ENV helpers
# -------------------------
def _get_env_str(name: str) -> str:
    val = os.getenv(name)
    if not val or not str(val).strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return str(val).strip()


def _get_env_int(name: str) -> int:
    raw = _get_env_str(name)
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"Environment variable {name} must be int, got: {raw!r}")


def _get_env_int_list(name: str) -> List[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    out: List[int] = []
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out


def _get_int_default(name: str, default: int, *, min_v: int = 0, max_v: int = 10_000) -> int:
    raw = os.getenv(name)
    if not raw or not raw.strip():
        return default
    try:
        val = int(raw.strip())
        return max(min_v, min(val, max_v))
    except ValueError:
        return default


# -------------------------
# Loader
# -------------------------
def load_settings() -> Settings:
    db_path = Path(os.getenv("DB_PATH", "bot.db")).expanduser().resolve()

    bot_pin = os.getenv("BOT_PIN")
    bot_pin = bot_pin.strip() if bot_pin and bot_pin.strip() else None

    journal_path = _get_env_str("JOURNAL_PATH").strip()

    return Settings(
        bot_token=_get_env_str("BOT_TOKEN"),
        director_tg_id=_get_env_int("DIRECTOR_TG_ID"),
        officer_tg_id=_get_env_int("OFFICER_TG_ID"),

        bot_pin=bot_pin,
        superadmin_ids=_get_env_int_list("SUPERADMIN_IDS"),

        remind_after_minutes=_get_int_default("REMIND_AFTER_MINUTES", 30, min_v=1, max_v=1440),
        remind_repeat_minutes=_get_int_default("REMIND_REPEAT_MINUTES", 30, min_v=1, max_v=1440),
        remind_check_seconds=_get_int_default("REMIND_CHECK_SECONDS", 60, min_v=10, max_v=3600),

        nc_webdav_url=_get_env_str("NC_WEBDAV_URL"),
        nc_user=_get_env_str("NC_USER"),
        nc_app_password=_get_env_str("NC_APP_PASSWORD"),
        journal_path=journal_path,

        db_path=db_path,

        max_companies_per_request=_get_int_default("MAX_COMPANIES_PER_REQUEST", 5, min_v=1, max_v=20),
        max_purpose_length=_get_int_default("MAX_PURPOSE_LENGTH", 500, min_v=50, max_v=2000),
        max_comment_length=_get_int_default("MAX_COMMENT_LENGTH", 300, min_v=0, max_v=2000),
    )
