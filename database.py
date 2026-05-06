import sqlite3
import os
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "calisth.db"))


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS tokens (
                user_id TEXT PRIMARY KEY,
                token   TEXT NOT NULL,
                username TEXT,
                connected_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS vips (
                user_id     TEXT PRIMARY KEY,
                type        TEXT DEFAULT 'trial',
                redeemed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                expires_at  TEXT,
                status      TEXT DEFAULT 'active'
            );
            CREATE TABLE IF NOT EXISTS farm_tasks (
                user_id    TEXT PRIMARY KEY,
                guild_id   TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                status     TEXT DEFAULT 'active',
                started_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS farm_stats (
                user_id      TEXT PRIMARY KEY,
                username     TEXT,
                total_seconds INTEGER DEFAULT 0,
                last_updated TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS whitelist (
                owner_id        TEXT,
                target_id       TEXT,
                target_username TEXT,
                PRIMARY KEY (owner_id, target_id)
            );
            CREATE TABLE IF NOT EXISTS rich_presence (
                user_id    TEXT PRIMARY KEY,
                app_name   TEXT DEFAULT '1533',
                details    TEXT,
                state_text TEXT,
                image_url  TEXT,
                image_text TEXT,
                app_id     TEXT,
                enabled    INTEGER DEFAULT 1,
                started_at INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS tickets (
                channel_id  TEXT PRIMARY KEY,
                user_id     TEXT NOT NULL,
                tipo        TEXT NOT NULL,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS ticket_ratings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id  TEXT,
                user_id     TEXT,
                rating      INTEGER,
                tipo        TEXT,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS auto_ticket (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    TEXT NOT NULL,
                message     TEXT NOT NULL,
                enabled     INTEGER DEFAULT 1,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS sub_owners (
                user_id TEXT PRIMARY KEY,
                added_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """)


# ── TOKENS ──────────────────────────────────────────────────────────────────

def save_token(user_id: str, token: str, username: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO tokens (user_id, token, username) VALUES (?,?,?)",
            (user_id, token, username),
        )

def get_token(user_id: str) -> Optional[str]:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT token FROM tokens WHERE user_id=?", (user_id,)).fetchone()
    return row[0] if row else None

def get_user_info(user_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            "SELECT username, connected_at FROM tokens WHERE user_id=?", (user_id,)
        ).fetchone()

def remove_token(user_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM tokens WHERE user_id=?", (user_id,))

def count_tokens() -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()
    return row[0] if row else 0

# ── VIP ─────────────────────────────────────────────────────────────────────

def _migrate_vips():
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute("ALTER TABLE vips ADD COLUMN status TEXT DEFAULT 'active'")
        except Exception:
            pass
        conn.execute("UPDATE vips SET status='active' WHERE status IS NULL")

def has_redeemed(user_id: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("SELECT 1 FROM vips WHERE user_id=?", (user_id,)).fetchone() is not None

def is_vip_active(user_id: str) -> bool:
    """Verifica se o usuário tem VIP ativo (não expirado)."""
    _migrate_vips()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT expires_at FROM vips WHERE user_id=? AND status='active'", (user_id,)
        ).fetchone()
    if not row:
        return False
    expires_at = row[0]
    if expires_at is None:
        return True  # VIP permanente
    return datetime.utcnow().isoformat() < expires_at

def redeem_vip(user_id: str, days: int = 7) -> bool:
    if has_redeemed(user_id):
        return False
    expires = (datetime.utcnow() + timedelta(days=days)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO vips (user_id, expires_at, status) VALUES (?,?,'active')",
            (user_id, expires),
        )
    return True

def grant_vip(user_id: str, days: int) -> str:
    """Concede VIP a um usuário (mesmo que já tenha resgatado). Retorna 'created' ou 'extended'."""
    _migrate_vips()
    now = datetime.utcnow()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT expires_at, status FROM vips WHERE user_id=?", (user_id,)
        ).fetchone()
        if row:
            # Estende a partir de agora ou da expiração atual, o que for maior
            current_exp = row[0]
            if current_exp and current_exp > now.isoformat():
                base = datetime.fromisoformat(current_exp)
            else:
                base = now
            new_exp = (base + timedelta(days=days)).isoformat()
            conn.execute(
                "UPDATE vips SET expires_at=?, status='active' WHERE user_id=?",
                (new_exp, user_id),
            )
            return "extended"
        else:
            expires = (now + timedelta(days=days)).isoformat()
            conn.execute(
                "INSERT INTO vips (user_id, expires_at, status) VALUES (?,?,'active')",
                (user_id, expires),
            )
            return "created"

def revoke_vip(user_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE vips SET status='expired' WHERE user_id=?", (user_id,))

def get_expired_vips() -> list:
    _migrate_vips()
    now = datetime.utcnow().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT user_id FROM vips WHERE status='active' AND expires_at IS NOT NULL AND expires_at <= ?",
            (now,),
        ).fetchall()
    return [r[0] for r in rows]

def mark_vip_expired(user_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE vips SET status='expired' WHERE user_id=?", (user_id,))

# ── FARM TASKS ───────────────────────────────────────────────────────────────

def set_farm_task(user_id: str, guild_id: str, channel_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO farm_tasks (user_id, guild_id, channel_id, status, started_at) VALUES (?,?,?,'active',CURRENT_TIMESTAMP)",
            (user_id, guild_id, channel_id),
        )

def stop_farm_task(user_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE farm_tasks SET status='stopped' WHERE user_id=?", (user_id,))

def get_farm_task(user_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            "SELECT guild_id, channel_id, status FROM farm_tasks WHERE user_id=?", (user_id,)
        ).fetchone()

def get_active_tasks():
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            """SELECT ft.user_id, ft.guild_id, ft.channel_id, t.token
               FROM farm_tasks ft JOIN tokens t ON ft.user_id = t.user_id
               WHERE ft.status = 'active'"""
        ).fetchall()

# ── FARM STATS (ranking) ─────────────────────────────────────────────────────

def add_farm_seconds(user_id: str, username: str, seconds: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT INTO farm_stats (user_id, username, total_seconds)
               VALUES (?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                   total_seconds = total_seconds + excluded.total_seconds,
                   username = excluded.username,
                   last_updated = CURRENT_TIMESTAMP""",
            (user_id, username, seconds),
        )

def get_ranking(limit: int = 10):
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            "SELECT user_id, username, total_seconds FROM farm_stats ORDER BY total_seconds DESC LIMIT ?",
            (limit,),
        ).fetchall()

# ── WHITELIST ────────────────────────────────────────────────────────────────

def add_whitelist(owner_id: str, target_id: str, target_username: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO whitelist VALUES (?,?,?)", (owner_id, target_id, target_username)
        )

def remove_whitelist(owner_id: str, target_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM whitelist WHERE owner_id=? AND target_id=?", (owner_id, target_id))

def get_whitelist(owner_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            "SELECT target_id, target_username FROM whitelist WHERE owner_id=?", (owner_id,)
        ).fetchall()

# ── RICH PRESENCE ────────────────────────────────────────────────────────────

def save_presence(user_id: str, app_name: str, details: str, state_text: str,
                  image_url: str, image_text: str, app_id: str = "", started_at: int = 0):
    import time as _time
    if started_at == 0:
        started_at = int(_time.time() * 1000)
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute("ALTER TABLE rich_presence ADD COLUMN started_at INTEGER DEFAULT 0")
        except Exception:
            pass
        conn.execute(
            """INSERT OR REPLACE INTO rich_presence
               (user_id, app_name, details, state_text, image_url, image_text, app_id, enabled, started_at)
               VALUES (?,?,?,?,?,?,?,1,?)""",
            (user_id, app_name, details, state_text, image_url, image_text, app_id, started_at),
        )

def get_presence(user_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute("ALTER TABLE rich_presence ADD COLUMN started_at INTEGER DEFAULT 0")
        except Exception:
            pass
        return conn.execute(
            "SELECT app_name, details, state_text, image_url, image_text, app_id, enabled, started_at FROM rich_presence WHERE user_id=?",
            (user_id,),
        ).fetchone()

def disable_presence(user_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE rich_presence SET enabled=0 WHERE user_id=?", (user_id,))

def get_all_active_presences():
    """Retorna lista de (user_id,) de todos usuários com presence ativa."""
    with sqlite3.connect(DB_PATH) as conn:
        try:
            return conn.execute(
                "SELECT user_id FROM rich_presence WHERE enabled=1"
            ).fetchall()
        except Exception:
            return []

# ── TICKETS ──────────────────────────────────────────────────────────────────

def save_ticket(channel_id: str, user_id: str, tipo: str):
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tickets (
                    channel_id  TEXT PRIMARY KEY,
                    user_id     TEXT NOT NULL,
                    tipo        TEXT NOT NULL,
                    created_at  TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception:
            pass
        conn.execute(
            "INSERT OR REPLACE INTO tickets (channel_id, user_id, tipo) VALUES (?,?,?)",
            (channel_id, user_id, tipo),
        )

def get_ticket(channel_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        try:
            return conn.execute(
                "SELECT user_id, tipo, created_at FROM tickets WHERE channel_id=?", (channel_id,)
            ).fetchone()
        except Exception:
            return None

def delete_ticket(channel_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute("DELETE FROM tickets WHERE channel_id=?", (channel_id,))
        except Exception:
            pass

def get_ticket_by_user(user_id: str):
    """Retorna o ticket aberto de um usuário, ou None se não houver."""
    with sqlite3.connect(DB_PATH) as conn:
        try:
            return conn.execute(
                "SELECT channel_id, tipo FROM tickets WHERE user_id=?", (user_id,)
            ).fetchone()
        except Exception:
            return None

def save_rating(channel_id: str, user_id: str, rating: int, tipo: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO ticket_ratings (channel_id, user_id, rating, tipo) VALUES (?,?,?,?)",
            (channel_id, user_id, rating, tipo),
        )

# ── AUTO TICKET ──────────────────────────────────────────────────────────────

def save_auto_ticket(guild_id: str, message: str, enabled: int = 1):
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS auto_ticket (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id TEXT NOT NULL,
                    message TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception:
            pass
        # Upsert por guild_id
        existing = conn.execute("SELECT id FROM auto_ticket WHERE guild_id=?", (guild_id,)).fetchone()
        if existing:
            conn.execute(
                "UPDATE auto_ticket SET message=?, enabled=?, created_at=CURRENT_TIMESTAMP WHERE guild_id=?",
                (message, enabled, guild_id),
            )
        else:
            conn.execute(
                "INSERT INTO auto_ticket (guild_id, message, enabled) VALUES (?,?,?)",
                (guild_id, message, enabled),
            )

def get_auto_ticket(guild_id: str):
    """Retorna (guild_id, message, enabled) ou None."""
    with sqlite3.connect(DB_PATH) as conn:
        try:
            return conn.execute(
                "SELECT guild_id, message, enabled FROM auto_ticket WHERE guild_id=?", (guild_id,)
            ).fetchone()
        except Exception:
            return None

def set_auto_ticket_enabled(guild_id: str, enabled: int):
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute(
                "UPDATE auto_ticket SET enabled=? WHERE guild_id=?", (enabled, guild_id)
            )
        except Exception:
            pass

def delete_auto_ticket(guild_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute("DELETE FROM auto_ticket WHERE guild_id=?", (guild_id,))
        except Exception:
            pass

# ── SUB OWNERS ───────────────────────────────────────────────────────────────

def add_sub_owner(user_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sub_owners (
                    user_id TEXT PRIMARY KEY,
                    added_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception:
            pass
        conn.execute(
            "INSERT OR IGNORE INTO sub_owners (user_id) VALUES (?)", (user_id,)
        )

def remove_sub_owner(user_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute("DELETE FROM sub_owners WHERE user_id=?", (user_id,))
        except Exception:
            pass

def is_sub_owner(user_id: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        try:
            return conn.execute(
                "SELECT 1 FROM sub_owners WHERE user_id=?", (user_id,)
            ).fetchone() is not None
        except Exception:
            return False

def get_all_sub_owners() -> list:
    with sqlite3.connect(DB_PATH) as conn:
        try:
            return [r[0] for r in conn.execute("SELECT user_id FROM sub_owners").fetchall()]
        except Exception:
            return []
