# db.py
from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite

from config import (
    TOKEN_AVAILABLE,
    TOKEN_RESERVED,
    TOKEN_ISSUED,
    STATUS_REQUESTED,
    STATUS_APPROVED,
    STATUS_REJECTED,
    STATUS_ISSUED,
    STATUS_RETURNED,
)

log = logging.getLogger(__name__)


@dataclass
class RequestRow:
    id: int
    tg_id: int
    username: Optional[str]
    company: str
    token_id: str
    purpose: str
    comment: Optional[str]
    status: str
    requested_at: Optional[str]
    remind_sent_at: Optional[str]
    approved_by: Optional[int]
    approved_at: Optional[str]
    issued_by: Optional[int]
    issued_at: Optional[str]
    returned_by: Optional[int]
    returned_at: Optional[str]


class Database:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    async def _configure(self, db: aiosqlite.Connection) -> None:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.execute("PRAGMA foreign_keys=ON;")
        await db.execute("PRAGMA busy_timeout=5000;")

    async def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)

            await db.executescript(
                """
                CREATE TABLE IF NOT EXISTS tokens(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  token_id TEXT UNIQUE NOT NULL,
                  description TEXT,
                  status TEXT DEFAULT 'available'
                );

                CREATE TABLE IF NOT EXISTS requests(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  tg_id INTEGER NOT NULL,
                  username TEXT,
                  company TEXT NOT NULL,
                  token_id TEXT NOT NULL,
                  purpose TEXT NOT NULL,
                  comment TEXT,
                  status TEXT DEFAULT 'REQUESTED',
                  requested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                  remind_sent_at DATETIME,
                  approved_by INTEGER,
                  approved_at DATETIME,
                  issued_by INTEGER,
                  issued_at DATETIME,
                  returned_by INTEGER,
                  returned_at DATETIME
                );

                CREATE TABLE IF NOT EXISTS audit_log(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                  request_id INTEGER,
                  actor_tg_id INTEGER,
                  action TEXT,
                  payload TEXT
                );

                CREATE TABLE IF NOT EXISTS request_items(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  request_id INTEGER NOT NULL,
                  company TEXT NOT NULL,
                  token_id TEXT NOT NULL,
                  FOREIGN KEY(request_id) REFERENCES requests(id) ON DELETE CASCADE,
                  UNIQUE(request_id, token_id)
                );

                CREATE TABLE IF NOT EXISTS bot_auth(
                  tg_id INTEGER PRIMARY KEY,
                  authed_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS user_profiles(
                  tg_id INTEGER PRIMARY KEY,
                  full_name TEXT NOT NULL,
                  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_tokens_status ON tokens(status);
                CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status);
                CREATE INDEX IF NOT EXISTS idx_requests_tg_id ON requests(tg_id);
                CREATE INDEX IF NOT EXISTS idx_requests_requested_at ON requests(requested_at);
                CREATE INDEX IF NOT EXISTS idx_request_items_request_id ON request_items(request_id);
                CREATE INDEX IF NOT EXISTS idx_audit_request_id ON audit_log(request_id);
                """
            )

            # Мягкие миграции (без молчаливого pass)
            await self._ensure_column(db, "requests", "requested_at", "DATETIME DEFAULT CURRENT_TIMESTAMP")
            await self._ensure_column(db, "requests", "remind_sent_at", "DATETIME")

            await self._seed_tokens_if_empty(db)
            await db.commit()

    async def _ensure_table(self, db: aiosqlite.Connection, name: str, ddl: str) -> None:
        await db.execute(ddl)

    async def _ensure_column(self, db: aiosqlite.Connection, table: str, column: str, ddl_tail: str) -> None:
        try:
            cur = await db.execute(f"PRAGMA table_info({table});")
            cols = [r["name"] for r in await cur.fetchall()]
            if column not in cols:
                await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_tail};")
        except Exception as e:
            log.warning("Migration ensure_column failed: %s.%s (%s)", table, column, e)

    async def _seed_tokens_if_empty(self, db: aiosqlite.Connection) -> None:
        cur = await db.execute("SELECT COUNT(*) AS c FROM tokens;")
        row = await cur.fetchone()
        if row and int(row["c"]) > 0:
            return

        from config import COMPANY_TOKEN_MAP, COMPANIES

        examples: List[Tuple[str, str, str]] = []
        for company in COMPANIES:
            token_id = COMPANY_TOKEN_MAP.get(company)
            if token_id:
                examples.append((token_id, f"Токен для {company}", TOKEN_AVAILABLE))

        if examples:
            await db.executemany(
                "INSERT OR IGNORE INTO tokens(token_id, description, status) VALUES(?, ?, ?);",
                examples,
            )

    # -------------------------
    # Internal TX helpers (НЕ открывают новые connect)
    # -------------------------
    async def _get_request_tx(self, db: aiosqlite.Connection, request_id: int) -> Optional[RequestRow]:
        cur = await db.execute("SELECT * FROM requests WHERE id=?;", (request_id,))
        row = await cur.fetchone()
        return RequestRow(**dict(row)) if row else None

    async def _get_request_items_tx(self, db: aiosqlite.Connection, request_id: int) -> List[Dict[str, Any]]:
        cur = await db.execute(
            "SELECT company, token_id FROM request_items WHERE request_id=? ORDER BY company;",
            (request_id,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def _add_audit_log_tx(
        self, db: aiosqlite.Connection, request_id: int, actor_tg_id: int, action: str, payload: Dict[str, Any]
    ) -> None:
        await db.execute(
            "INSERT INTO audit_log(request_id, actor_tg_id, action, payload) VALUES(?, ?, ?, ?);",
            (request_id, actor_tg_id, action, json.dumps(payload, ensure_ascii=False)),
        )


    # -------------------------
    # User profile (FIO)
    # -------------------------
    async def get_user_full_name(self, tg_id: int) -> Optional[str]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute("SELECT full_name FROM user_profiles WHERE tg_id=?;", (tg_id,))
            row = await cur.fetchone()
            if not row:
                return None
            name = str(row["full_name"] or "").strip()
            return name or None

    async def set_user_full_name(self, tg_id: int, full_name: str) -> None:
        value = str(full_name or "").strip()
        if not value:
            raise ValueError("full_name is empty")
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            await db.execute(
                "INSERT INTO user_profiles(tg_id, full_name, updated_at) VALUES(?, ?, CURRENT_TIMESTAMP) "
                "ON CONFLICT(tg_id) DO UPDATE SET full_name=excluded.full_name, updated_at=CURRENT_TIMESTAMP;",
                (tg_id, value),
            )
            await db.execute(
                "UPDATE requests SET username=? WHERE tg_id=? AND (username IS NULL OR TRIM(username)='');",
                (value, tg_id),
            )
            await db.commit()

    # -------------------------
    # PIN auth
    # -------------------------
    async def is_authed(self, tg_id: int) -> bool:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute("SELECT 1 FROM bot_auth WHERE tg_id=?;", (tg_id,))
            return (await cur.fetchone()) is not None

    async def set_authed(self, tg_id: int) -> None:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            await db.execute(
                "INSERT INTO bot_auth(tg_id, authed_at) VALUES(?, CURRENT_TIMESTAMP) "
                "ON CONFLICT(tg_id) DO UPDATE SET authed_at=CURRENT_TIMESTAMP;",
                (tg_id,),
            )
            await db.commit()

    async def revoke_auth(self, tg_id: int) -> None:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            await db.execute("DELETE FROM bot_auth WHERE tg_id=?;", (tg_id,))
            await db.commit()

    async def list_authed_users(self, limit: int = 100) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute(
                "SELECT tg_id, authed_at FROM bot_auth ORDER BY authed_at DESC LIMIT ?;",
                (limit,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    # -------------------------
    # Tokens
    # -------------------------
    async def get_token(self, token_id: str) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute(
                "SELECT token_id, description, status FROM tokens WHERE token_id=?;",
                (token_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def set_token_status(self, token_id: str, status: str) -> bool:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute(
                "UPDATE tokens SET status=? WHERE token_id=?;",
                (status, token_id),
            )
            await db.commit()
            return cur.rowcount > 0

    async def list_available_tokens(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute(
                "SELECT token_id, description, status FROM tokens WHERE status=? ORDER BY token_id;",
                (TOKEN_AVAILABLE,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def list_all_tokens(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute("SELECT token_id, description, status FROM tokens ORDER BY token_id;")
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    # -------------------------
    # Requests / Items
    # -------------------------
    async def create_request_multi(
        self,
        tg_id: int,
        username: str,
        items: List[Tuple[str, str]],  # [(company, token_id)]
        purpose: str,
        comment: Optional[str],
    ) -> int:
        """Создаёт одну заявку и несколько позиций request_items. Резервирует токены атомарно."""
        uniq: Dict[str, str] = {}
        for company, token_id in items:
            uniq[token_id] = company
        items_u = [(c, t) for t, c in uniq.items()]  # [(company, token_id)]

        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            try:
                await db.execute("BEGIN IMMEDIATE;")

                # Проверяем и резервируем токены (idempotent)
                for company, token_id in items_u:
                    cur = await db.execute("SELECT status FROM tokens WHERE token_id=?;", (token_id,))
                    row = await cur.fetchone()
                    if not row:
                        raise RuntimeError(f"TOKEN_NOT_FOUND:{token_id}")
                    if row["status"] != TOKEN_AVAILABLE:
                        raise RuntimeError(f"TOKEN_NOT_AVAILABLE:{token_id}")

                # Создаём заявку
                cur = await db.execute(
                    """
                    INSERT INTO requests(tg_id, username, company, token_id, purpose, comment, status)
                    VALUES(?, ?, ?, ?, ?, ?, ?);
                    """,
                    (tg_id, username, "MULTI", "MULTI", purpose, comment, STATUS_REQUESTED),
                )
                request_id = int(cur.lastrowid)

                # Items
                for company, token_id in items_u:
                    await db.execute(
                        "INSERT INTO request_items(request_id, company, token_id) VALUES(?, ?, ?);",
                        (request_id, company, token_id),
                    )

                # Reserve tokens строго из available -> reserved
                for company, token_id in items_u:
                    cur2 = await db.execute(
                        "UPDATE tokens SET status=? WHERE token_id=? AND status=?;",
                        (TOKEN_RESERVED, token_id, TOKEN_AVAILABLE),
                    )
                    if cur2.rowcount != 1:
                        raise RuntimeError(f"TOKEN_RESERVE_FAILED:{token_id}")

                await self._add_audit_log_tx(
                    db=db,
                    request_id=request_id,
                    actor_tg_id=tg_id,
                    action="REQUESTED",
                    payload={"items": items_u, "purpose": purpose, "comment": comment},
                )

                await db.commit()
                return request_id

            except sqlite3.Error as e:
                await db.execute("ROLLBACK;")
                raise RuntimeError(f"DB_ERROR:{e}") from e
            except Exception:
                await db.execute("ROLLBACK;")
                raise

    async def get_request(self, request_id: int) -> Optional[RequestRow]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            return await self._get_request_tx(db, request_id)

    async def get_request_items(self, request_id: int) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            return await self._get_request_items_tx(db, request_id)

    async def list_requests_by_tg(self, tg_id: int, limit: int = 50) -> List[RequestRow]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute(
                "SELECT * FROM requests WHERE tg_id=? ORDER BY id DESC LIMIT ?;",
                (tg_id, limit),
            )
            rows = await cur.fetchall()
            return [RequestRow(**dict(r)) for r in rows]

    async def list_requests_by_user(self, tg_id: int, limit: int = 50) -> List[RequestRow]:
        return await self.list_requests_by_tg(tg_id, limit)

    async def list_requests_by_status(self, status: str, limit: int = 50) -> List[RequestRow]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute(
                "SELECT * FROM requests WHERE status=? ORDER BY id DESC LIMIT ?;",
                (status, limit),
            )
            rows = await cur.fetchall()
            return [RequestRow(**dict(r)) for r in rows]

    async def list_pending_for_director(self, limit: int = 50) -> List[RequestRow]:
        return await self.list_requests_by_status(STATUS_REQUESTED, limit)

    async def list_active_for_officer(self, limit: int = 50) -> List[RequestRow]:
        return await self.list_requests_by_status(STATUS_APPROVED, limit)

    async def list_last_requests(self, limit: int = 20) -> List[RequestRow]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute("SELECT * FROM requests ORDER BY id DESC LIMIT ?;", (limit,))
            rows = await cur.fetchall()
            return [RequestRow(**dict(r)) for r in rows]

    async def counts_by_status(self) -> Dict[str, int]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute("SELECT status, COUNT(*) AS c FROM requests GROUP BY status;")
            rows = await cur.fetchall()
            return {r["status"]: int(r["c"]) for r in rows}

    async def pending_over_seconds(self, seconds: int) -> List[RequestRow]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute(
                """
                SELECT * FROM requests
                WHERE status=? AND requested_at <= DATETIME('now', ?)
                ORDER BY requested_at ASC;
                """,
                (STATUS_REQUESTED, f"-{int(seconds)} seconds"),
            )
            rows = await cur.fetchall()
            return [RequestRow(**dict(r)) for r in rows]

    async def pending_for_remind(self, after_minutes: int, repeat_minutes: int) -> List[RequestRow]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            cur = await db.execute(
                """
                SELECT * FROM requests
                WHERE status=? 
                  AND requested_at <= DATETIME('now', ?)
                  AND (
                        remind_sent_at IS NULL OR
                        remind_sent_at <= DATETIME('now', ?)
                  )
                ORDER BY requested_at ASC;
                """,
                (
                    STATUS_REQUESTED,
                    f"-{int(after_minutes)} minutes",
                    f"-{int(repeat_minutes)} minutes",
                ),
            )
            rows = await cur.fetchall()
            return [RequestRow(**dict(r)) for r in rows]

    async def mark_reminded(self, request_ids: List[int]) -> None:
        if not request_ids:
            return
        placeholders = ",".join(["?"] * len(request_ids))
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            await db.execute(
                f"UPDATE requests SET remind_sent_at=CURRENT_TIMESTAMP WHERE id IN ({placeholders});",
                request_ids,
            )
            await db.commit()

    # -------------------------
    # Status transitions (director/officer) — безопасно и атомарно
    # -------------------------
    async def director_decide(self, request_id: int, director_tg_id: int, approve: bool) -> Optional[RequestRow]:
        new_status = STATUS_APPROVED if approve else STATUS_REJECTED

        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            try:
                await db.execute("BEGIN IMMEDIATE;")

                cur = await db.execute("SELECT status FROM requests WHERE id=?;", (request_id,))
                row = await cur.fetchone()
                if not row:
                    await db.execute("ROLLBACK;")
                    return None
                if row["status"] != STATUS_REQUESTED:
                    await db.execute("ROLLBACK;")
                    raise RuntimeError("INVALID_STATUS")

                # Переход REQUESTED -> APPROVED/REJECTED (защита от повторных кликов)
                cur2 = await db.execute(
                    "UPDATE requests SET status=?, approved_by=?, approved_at=CURRENT_TIMESTAMP "
                    "WHERE id=? AND status=?;",
                    (new_status, director_tg_id, request_id, STATUS_REQUESTED),
                )
                if cur2.rowcount != 1:
                    raise RuntimeError("RACE_LOST")

                # Токены: все должны быть reserved; если отказ — вернуть в available
                items = await self._get_request_items_tx(db, request_id)
                want = TOKEN_RESERVED if approve else TOKEN_AVAILABLE
                for it in items:
                    cur3 = await db.execute(
                        "UPDATE tokens SET status=? WHERE token_id=? AND status=?;",
                        (want, it["token_id"], TOKEN_RESERVED),
                    )
                    if cur3.rowcount != 1:
                        raise RuntimeError(f"TOKEN_STATUS_MISMATCH:{it['token_id']}")

                await self._add_audit_log_tx(
                    db=db,
                    request_id=request_id,
                    actor_tg_id=director_tg_id,
                    action=new_status,
                    payload={"approved": approve},
                )

                await db.commit()
                return await self._get_request_tx(db, request_id)

            except Exception:
                await db.execute("ROLLBACK;")
                raise

    async def officer_issue(self, request_id: int, officer_tg_id: int) -> Optional[RequestRow]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            try:
                await db.execute("BEGIN IMMEDIATE;")

                cur = await db.execute("SELECT status FROM requests WHERE id=?;", (request_id,))
                row = await cur.fetchone()
                if not row:
                    await db.execute("ROLLBACK;")
                    return None
                if row["status"] != STATUS_APPROVED:
                    await db.execute("ROLLBACK;")
                    raise RuntimeError("INVALID_STATUS")

                cur2 = await db.execute(
                    "UPDATE requests SET status=?, issued_by=?, issued_at=CURRENT_TIMESTAMP "
                    "WHERE id=? AND status=?;",
                    (STATUS_ISSUED, officer_tg_id, request_id, STATUS_APPROVED),
                )
                if cur2.rowcount != 1:
                    raise RuntimeError("RACE_LOST")

                items = await self._get_request_items_tx(db, request_id)
                for it in items:
                    cur3 = await db.execute(
                        "UPDATE tokens SET status=? WHERE token_id=? AND status=?;",
                        (TOKEN_ISSUED, it["token_id"], TOKEN_RESERVED),
                    )
                    if cur3.rowcount != 1:
                        raise RuntimeError(f"TOKEN_STATUS_MISMATCH:{it['token_id']}")

                await self._add_audit_log_tx(
                    db=db,
                    request_id=request_id,
                    actor_tg_id=officer_tg_id,
                    action="ISSUED",
                    payload={},
                )

                await db.commit()
                return await self._get_request_tx(db, request_id)

            except Exception:
                await db.execute("ROLLBACK;")
                raise

    async def officer_return(self, request_id: int, officer_tg_id: int) -> Optional[RequestRow]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            try:
                await db.execute("BEGIN IMMEDIATE;")

                cur = await db.execute("SELECT status FROM requests WHERE id=?;", (request_id,))
                row = await cur.fetchone()
                if not row:
                    await db.execute("ROLLBACK;")
                    return None
                if row["status"] != STATUS_ISSUED:
                    await db.execute("ROLLBACK;")
                    raise RuntimeError("INVALID_STATUS")

                cur2 = await db.execute(
                    "UPDATE requests SET status=?, returned_by=?, returned_at=CURRENT_TIMESTAMP "
                    "WHERE id=? AND status=?;",
                    (STATUS_RETURNED, officer_tg_id, request_id, STATUS_ISSUED),
                )
                if cur2.rowcount != 1:
                    raise RuntimeError("RACE_LOST")

                items = await self._get_request_items_tx(db, request_id)
                for it in items:
                    cur3 = await db.execute(
                        "UPDATE tokens SET status=? WHERE token_id=? AND status=?;",
                        (TOKEN_AVAILABLE, it["token_id"], TOKEN_ISSUED),
                    )
                    if cur3.rowcount != 1:
                        raise RuntimeError(f"TOKEN_STATUS_MISMATCH:{it['token_id']}")

                await self._add_audit_log_tx(
                    db=db,
                    request_id=request_id,
                    actor_tg_id=officer_tg_id,
                    action="RETURNED",
                    payload={},
                )

                await db.commit()
                return await self._get_request_tx(db, request_id)

            except Exception:
                await db.execute("ROLLBACK;")
                raise

    # -------------------------
    # Audit
    # -------------------------
    async def add_audit_log(self, request_id: int, actor_tg_id: int, action: str, payload: Dict[str, Any]) -> None:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            await db.execute(
                "INSERT INTO audit_log(request_id, actor_tg_id, action, payload) VALUES(?, ?, ?, ?);",
                (request_id, actor_tg_id, action, json.dumps(payload, ensure_ascii=False)),
            )
            await db.commit()

    async def get_audit_logs(self, request_id: Optional[int] = None, limit: int = 50) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)
            if request_id is not None:
                cur = await db.execute(
                    "SELECT * FROM audit_log WHERE request_id=? ORDER BY ts DESC LIMIT ?;",
                    (request_id, limit),
                )
            else:
                cur = await db.execute("SELECT * FROM audit_log ORDER BY ts DESC LIMIT ?;", (limit,))
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    # -------------------------
    # Admin functions
    # -------------------------
    async def get_statistics(self) -> Dict[str, Any]:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)

            cur = await db.execute(
                "SELECT COUNT(*) as total, "
                "SUM(CASE WHEN status='REQUESTED' THEN 1 ELSE 0 END) as pending, "
                "SUM(CASE WHEN status='APPROVED' THEN 1 ELSE 0 END) as approved, "
                "SUM(CASE WHEN status='ISSUED' THEN 1 ELSE 0 END) as issued, "
                "SUM(CASE WHEN status='RETURNED' THEN 1 ELSE 0 END) as returned, "
                "SUM(CASE WHEN status='REJECTED' THEN 1 ELSE 0 END) as rejected "
                "FROM requests;"
            )
            req_stats = dict(await cur.fetchone())

            cur = await db.execute("SELECT status, COUNT(*) as count FROM tokens GROUP BY status;")
            token_stats = {row["status"]: row["count"] for row in await cur.fetchall()}

            cur = await db.execute("SELECT COUNT(DISTINCT tg_id) as users FROM requests;")
            users_count = (await cur.fetchone())["users"]

            cur = await db.execute("SELECT COUNT(*) as authed FROM bot_auth;")
            authed_count = (await cur.fetchone())["authed"]

            return {
                "requests": req_stats,
                "tokens": token_stats,
                "users_count": users_count,
                "authed_count": authed_count,
            }

    async def cleanup_old_data(self, days: int = 90) -> int:
        async with aiosqlite.connect(self.db_path.as_posix()) as db:
            await self._configure(db)

            cur = await db.execute(
                "SELECT id FROM requests WHERE status IN ('RETURNED','REJECTED') "
                "AND requested_at <= DATETIME('now', ?);",
                (f"-{days} days",),
            )
            old_ids = [row["id"] for row in await cur.fetchall()]
            if not old_ids:
                return 0

            placeholders = ",".join(["?"] * len(old_ids))
            await db.execute("BEGIN IMMEDIATE;")
            await db.execute(f"DELETE FROM request_items WHERE request_id IN ({placeholders});", old_ids)
            await db.execute(f"DELETE FROM audit_log WHERE request_id IN ({placeholders});", old_ids)
            await db.execute(f"DELETE FROM requests WHERE id IN ({placeholders});", old_ids)
            await db.commit()
            return len(old_ids)
