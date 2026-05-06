from __future__ import annotations

import asyncio
import base64
import json
import os
import sqlite3
import time
from typing import Optional

import aiohttp
import discord
import websockets
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

from database import (
    add_farm_seconds, add_whitelist, count_tokens, disable_presence,
    get_active_tasks, get_expired_vips, get_farm_task, get_presence, get_ranking,
    get_token, get_user_info, get_whitelist,
    has_redeemed, init_db, mark_vip_expired, redeem_vip, remove_token, remove_whitelist,
    save_presence, save_token, set_farm_task, stop_farm_task, DB_PATH,
    save_ticket, get_ticket, get_ticket_by_user, delete_ticket, save_rating,
    get_all_active_presences,
    # Novos
    is_vip_active, grant_vip, revoke_vip,
    save_auto_ticket, get_auto_ticket, set_auto_ticket_enabled, delete_auto_ticket,
    add_sub_owner, remove_sub_owner, is_sub_owner, get_all_sub_owners,
)

# ─── OWNER / CONFIG ──────────────────────────────────────────────────────────

OWNER_ID = os.environ.get("OWNER_ID", "").strip()
if not OWNER_ID:
    raise SystemExit("OWNER_ID não configurado! Defina no .env ou como variável de ambiente.")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN não configurado! Defina no .env ou como variável de ambiente.")

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")


def load_cfg() -> dict:
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}


def save_cfg(data: dict):
    with open(CONFIG_PATH, "w") as f:
        json.dump(data, f, indent=2)


def is_owner(interaction: discord.Interaction) -> bool:
    return str(interaction.user.id) == OWNER_ID


def is_sub_owner_interaction(interaction: discord.Interaction) -> bool:
    return is_sub_owner(str(interaction.user.id))


def is_privileged(interaction: discord.Interaction) -> bool:
    """True se for o dono do bot ou um sub dono."""
    return is_owner(interaction) or is_sub_owner_interaction(interaction)


def has_vip(interaction: discord.Interaction) -> bool:
    """True se for owner, sub dono ou tiver VIP ativo."""
    uid = str(interaction.user.id)
    if str(uid) == OWNER_ID or is_sub_owner(uid):
        return True
    cfg = load_cfg()
    vip_role_id = cfg.get("vip_role_id")
    if vip_role_id and interaction.guild:
        vip_role = interaction.guild.get_role(int(vip_role_id))
        if vip_role and vip_role in interaction.user.roles:
            return True
    return is_vip_active(uid)


intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ─── ACTIVE TASKS ────────────────────────────────────────────────────────────

CL_TASKS: dict[str, asyncio.Task] = {}
SESSIONS: dict[str, asyncio.Task] = {}
PRESENCE_SESSIONS: dict[str, asyncio.Task] = {}
SESSION_WS: dict[str, any] = {}

# ─── DISCORD API HELPERS ─────────────────────────────────────────────────────

API = "https://discord.com/api/v9"


def _hdr(token: str) -> dict:
    return {"Authorization": token, "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/json"}


def _super_props() -> str:
    props = {
        "os": "Windows",
        "browser": "Chrome",
        "device": "",
        "system_locale": "pt-BR",
        "browser_user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "browser_version": "124.0.0.0",
        "os_version": "10",
        "referrer": "",
        "referring_domain": "",
        "referrer_current": "",
        "referring_domain_current": "",
        "release_channel": "stable",
        "client_build_number": 335601,
        "client_event_source": None,
    }
    return base64.b64encode(json.dumps(props).encode()).decode()


def _hdr_user(token: str) -> dict:
    return {
        "Authorization": token,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Origin": "https://discord.com",
        "Referer": "https://discord.com/channels/@me",
        "X-Discord-Locale": "pt-BR",
        "X-Discord-Timezone": "America/Sao_Paulo",
        "X-Super-Properties": _super_props(),
    }


async def api_get(s: aiohttp.ClientSession, url: str, token: str):
    while True:
        async with s.get(url, headers=_hdr(token)) as r:
            if r.status == 429:
                d = await r.json()
                await asyncio.sleep(d.get("retry_after", 1) + 0.1)
                continue
            return await r.json() if r.status == 200 else None


async def api_delete(s: aiohttp.ClientSession, url: str, token: str) -> int:
    while True:
        async with s.delete(url, headers=_hdr(token)) as r:
            if r.status == 429:
                d = await r.json()
                await asyncio.sleep(d.get("retry_after", 1) + 0.1)
                continue
            return r.status


async def api_post(s: aiohttp.ClientSession, url: str, token: str, data: dict):
    while True:
        async with s.post(url, headers=_hdr(token), json=data) as r:
            if r.status == 429:
                d = await r.json()
                await asyncio.sleep(d.get("retry_after", 1) + 0.1)
                continue
            return await r.json() if r.status in (200, 201) else None


async def api_put(s: aiohttp.ClientSession, url: str, token: str, data: dict):
    while True:
        async with s.put(url, headers=_hdr(token), json=data) as r:
            if r.status == 429:
                d = await r.json()
                await asyncio.sleep(d.get("retry_after", 1) + 0.1)
                continue
            return await r.json() if r.status in (200, 201) else None


async def validate_token(token: str) -> Optional[dict]:
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{API}/users/@me", headers=_hdr(token)) as r:
            return await r.json() if r.status == 200 else None


async def get_channel_guild(token: str, channel_id: str) -> Optional[str]:
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{API}/channels/{channel_id}", headers=_hdr(token)) as r:
            if r.status == 200:
                return (await r.json()).get("guild_id")
    return None


async def set_hypesquad(token: str, house_id: int) -> bool:
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"{API}/hypesquad/online",
            headers=_hdr_user(token),
            json={"house_id": house_id},
        ) as r:
            print(f"[HYPESQUAD] set house={house_id} status={r.status}")
            return r.status in (200, 201, 204)


async def leave_hypesquad(token: str) -> bool:
    async with aiohttp.ClientSession() as s:
        async with s.delete(
            f"{API}/hypesquad/online",
            headers=_hdr_user(token),
        ) as r:
            print(f"[HYPESQUAD] leave status={r.status}")
            return r.status in (200, 204)


# ─── CL ACTIONS ──────────────────────────────────────────────────────────────

async def _delete_channel_messages(s, channel_id, user_id, token, whitelist_ids):
    count = 0
    last_id = None
    while True:
        url = f"{API}/channels/{channel_id}/messages?limit=100"
        if last_id:
            url += f"&before={last_id}"
        msgs = await api_get(s, url, token)
        if not msgs:
            break
        mine = [m for m in msgs if m["author"]["id"] == user_id
                and m["author"]["id"] not in whitelist_ids]
        for m in mine:
            st = await api_delete(s, f"{API}/channels/{channel_id}/messages/{m['id']}", token)
            if st in (200, 204):
                count += 1
            await asyncio.sleep(0.5)
        if msgs:
            last_id = msgs[-1]["id"]
        if len(msgs) < 100:
            break
    return count


async def cl_dm(discord_uid, token, target_user_id, whitelist_ids):
    async with aiohttp.ClientSession() as s:
        ch = await api_post(s, f"{API}/users/@me/channels", token, {"recipient_id": target_user_id})
        if not ch:
            return 0
        return await _delete_channel_messages(s, ch["id"], discord_uid, token, whitelist_ids)


async def cl_servidor(discord_uid, token, guild_id, whitelist_ids):
    async with aiohttp.ClientSession() as s:
        channels = await api_get(s, f"{API}/guilds/{guild_id}/channels", token)
        if not channels:
            return 0
        count = 0
        for ch in channels:
            if ch.get("type") in (0, 5, 11, 12):
                try:
                    count += await _delete_channel_messages(s, ch["id"], discord_uid, token, whitelist_ids)
                except Exception:
                    pass
        return count


async def cl_amigos(discord_uid, token, whitelist_ids):
    async with aiohttp.ClientSession() as s:
        rels = await api_get(s, f"{API}/users/@me/relationships", token)
        if not rels:
            return 0
        count = 0
        for r in rels:
            if r.get("type") == 1 and r["id"] not in whitelist_ids:
                ch = await api_post(s, f"{API}/users/@me/channels", token, {"recipient_id": r["id"]})
                if ch:
                    count += await _delete_channel_messages(s, ch["id"], discord_uid, token, whitelist_ids)
        return count


async def cl_dms(discord_uid, token, whitelist_ids):
    async with aiohttp.ClientSession() as s:
        channels = await api_get(s, f"{API}/users/@me/channels", token)
        if not channels:
            return 0
        count = 0
        for ch in channels:
            if ch.get("type") == 1:
                other = next((u["id"] for u in ch.get("recipients", []) if u["id"] != discord_uid), None)
                if other and other in whitelist_ids:
                    continue
                count += await _delete_channel_messages(s, ch["id"], discord_uid, token, whitelist_ids)
        return count


async def fechar_conversas(token):
    async with aiohttp.ClientSession() as s:
        channels = await api_get(s, f"{API}/users/@me/channels", token)
        if not channels:
            return 0
        count = 0
        for ch in channels:
            if ch.get("type") in (1, 3):
                st = await api_delete(s, f"{API}/channels/{ch['id']}", token)
                if st in (200, 204):
                    count += 1
                await asyncio.sleep(0.3)
        return count


async def remover_amigos(token, whitelist_ids):
    async with aiohttp.ClientSession() as s:
        # Usar header de usuário (com X-Super-Properties) para evitar 401/403
        async with s.get(f"{API}/users/@me/relationships", headers=_hdr_user(token)) as resp:
            if resp.status == 429:
                d = await resp.json()
                await asyncio.sleep(d.get("retry_after", 1) + 0.1)
            if resp.status != 200:
                return 0
            rels = await resp.json()
        if not rels:
            return 0
        count = 0
        for r in rels:
            # type 1 = amigo, type 3 = bloqueado (ignorar)
            if r.get("type") == 1 and r.get("id") not in whitelist_ids:
                rid = r.get("id") or r.get("user", {}).get("id")
                if not rid:
                    continue
                while True:
                    async with s.delete(
                        f"{API}/users/@me/relationships/{rid}",
                        headers=_hdr_user(token),
                    ) as dr:
                        if dr.status == 429:
                            d = await dr.json()
                            await asyncio.sleep(d.get("retry_after", 1) + 0.1)
                            continue
                        if dr.status in (200, 204):
                            count += 1
                        break
                await asyncio.sleep(0.5)
        return count


async def sair_servidores(token, whitelist_ids):
    async with aiohttp.ClientSession() as s:
        guilds = await api_get(s, f"{API}/users/@me/guilds", token)
        if not guilds:
            return 0
        count = 0
        for g in guilds:
            if g["id"] not in whitelist_ids:
                st = await api_delete(s, f"{API}/users/@me/guilds/{g['id']}", token)
                if st in (200, 204):
                    count += 1
                await asyncio.sleep(0.5)
        return count


# ─── GATEWAY HELPERS ─────────────────────────────────────────────────────────

GW_URL = "wss://gateway.discord.gg/?v=9&encoding=json"
GW_MAX = 20 * 1024 * 1024

# Mobile: aparece com ícone de celular, mas Discord bloqueia Rich Presence
IDENTIFY_PAYLOAD = lambda token: {
    "op": 2,
    "d": {
        "token": token,
        "intents": 0,
        "properties": {"$os": "android", "$browser": "Discord Android", "$device": "Android"},
        "presence": {"status": "online", "since": 0, "activities": [], "afk": False},
        "compress": False,
    },
}

# Desktop: necessário para Rich Presence (imagens) funcionar
IDENTIFY_PAYLOAD_DESKTOP = lambda token: {
    "op": 2,
    "d": {
        "token": token,
        "intents": 0,
        "properties": {"$os": "windows", "$browser": "Discord Client", "$device": "discord"},
        "presence": {"status": "online", "since": 0, "activities": [], "afk": False},
        "compress": False,
    },
}

PRESENCE_GUILD_ID = 1499649431707062343

DEFAULT_PRESENCE = {
    "name": "1533",
    "details": "1533 - /painel ♥",
    "state": ".gg/1533",
}

DEFAULT_PRESENCE_IMAGE: Optional[str] = None
PRESENCE_APP_ID: Optional[str] = None

# ─── EMOJIS ──────────────────────────────────────────────────────────────────

EMOJI_VOZ       = "<:a_voz:1499650096793649292>"
EMOJI_LIXEIRA   = "<:a_lixeira:1499650050735870022>"
EMOJI_INFO      = "<:a_info:1499650312250720266>"
EMOJI_CHECK     = "<:a_check:1499650333419376702>"
EMOJI_CANCEL    = "<:a_cancel:1499650323084480723>"
EMOJI_REMOVE    = "<:usuarioremovido:1499650072944709672>"
EMOJI_ADD       = "<:a_usuario:1499650040271077386>"
EMOJI_ON        = "<:c_ligado:1499650354868912168>"
EMOJI_OFF       = "<:c_desligado:1499650365602267236>"
EMOJI_ARROW_R   = "<:setadireita:1499650387227967570>"
EMOJI_ARROW_L   = "<:setaesquerda:1499650397583835188>"
EMOJI_BACK      = "<:a_voltar:1499650269993238569>"
EMOJI_HAMMER    = "<:a_moderessao:1499650248975323197>"
EMOJI_BRAVERY   = "<:677453houseofbravery:1499650408069599364>"
EMOJI_BALANCE   = "<:642689houseofbalance:1499650418681319526>"
EMOJI_BRILLIANCE = "<:271697houseofbrilliance:1499650429422927982>"
EMOJI_CONTA     = "<:a_conta:1499650117907517482>"
EMOJI_PROTECAO  = "<:a_protecao:1499650085183815710>"
EMOJI_DISCORD   = "<:a_discord:1499650227651739691>"
EMOJI_SETA      = "<:b_seta:1499871879450722304>"
EMOJI_PONTO     = "<:b_ponto:1499871863277617223>"


# ─── LOG HELPERS ─────────────────────────────────────────────────────────────

async def _get_log_channel():
    cfg = load_cfg()
    ch_id = cfg.get("logs_channel_id")
    if not ch_id:
        return None
    return bot.get_channel(int(ch_id))


async def send_log(title: str, description: str, emoji: str = ""):
    ch = await _get_log_channel()
    if not ch:
        return
    em = discord.Embed(
        title=f"{emoji}  {title}" if emoji else title,
        description=description,
        color=0x5DADE2,
    )
    em.set_footer(text="1533 © Todos os direitos reservados")
    try:
        await ch.send(embed=em)
    except Exception:
        pass


async def send_log_cl(action, user_str, uid, count, alvo=None, elapsed=None):
    import time as _t
    ch = await _get_log_channel()
    if not ch:
        return
    icon_map = {
        "CL DM": EMOJI_LIXEIRA, "CL Servidor": EMOJI_LIXEIRA,
        "CL Amigos": EMOJI_LIXEIRA, "CL DMs": EMOJI_LIXEIRA,
        "Fechar Conversas": EMOJI_LIXEIRA, "Remover Amigos": EMOJI_REMOVE,
        "Sair Servidores": EMOJI_CANCEL,
    }
    label_map = {
        "CL DM": "Mensagens apagadas", "CL Servidor": "Mensagens apagadas",
        "CL Amigos": "Mensagens apagadas", "CL DMs": "Mensagens apagadas",
        "Fechar Conversas": "Conversas fechadas", "Remover Amigos": "Amigos removidos",
        "Sair Servidores": "Servidores saídos",
    }
    icon = icon_map.get(action, EMOJI_LIXEIRA)
    label = label_map.get(action, "Total")
    ts = int(_t.time())
    em = discord.Embed(title=f"{icon}  {action}", color=0x5DADE2)
    em.add_field(name="Usuário", value=f"**{user_str}**\n`{uid}`", inline=True)
    em.add_field(name=label, value=f"`{count}`", inline=True)
    if alvo:
        em.add_field(name="Alvo", value=f"`{alvo}`", inline=True)
    if elapsed is not None:
        h, m, s = int(elapsed)//3600, (int(elapsed)%3600)//60, int(elapsed)%60
        em.add_field(name="Duração", value=f"`{h}h {m:02d}m {s:02d}s`", inline=True)
    em.add_field(name="Horário", value=f"<t:{ts}:F>", inline=False)
    em.set_footer(text="1533 © Todos os direitos reservados")
    try:
        await ch.send(embed=em)
    except Exception:
        pass


async def send_log_farm(event, user_str, user_id, channel_id, elapsed=None):
    import time as _t
    ch = await _get_log_channel()
    if not ch:
        return
    ts = int(_t.time())
    icon = EMOJI_ON if event == "Iniciado" else EMOJI_OFF
    em = discord.Embed(title=f"{icon}  Farm {event}", color=0x5DADE2)
    em.add_field(name="Usuário", value=f"**{user_str}**\n`{user_id}`", inline=True)
    em.add_field(name="Canal", value=f"`{channel_id}`", inline=True)
    if elapsed is not None:
        h, m, s = elapsed//3600, (elapsed%3600)//60, elapsed%60
        em.add_field(name="Tempo na call", value=f"`{h}h {m:02d}m {s:02d}s`", inline=True)
    em.add_field(name="Horário", value=f"<t:{ts}:F>", inline=False)
    em.set_footer(text="1533 © Todos os direitos reservados")
    try:
        await ch.send(embed=em)
    except Exception:
        pass


def build_presence_op3(user_id: str, farm_start_ms: Optional[int] = None) -> dict:
    p = get_presence(user_id)
    now_ms = farm_start_ms or int(time.time() * 1000)
    if p and not p[6]:
        return {"op": 3, "d": {"since": None, "activities": [], "status": "online", "afk": False}}
    if p and p[6]:
        app_name, details, state_text, image_url, image_text, app_id, _, started_at = p
        activity: dict = {
            "name": app_name or DEFAULT_PRESENCE["name"],
            "type": 0,
            "details": details or DEFAULT_PRESENCE["details"],
            "state": state_text or DEFAULT_PRESENCE["state"],
            "timestamps": {"start": started_at if started_at else now_ms},
        }
        if image_url and image_url.strip():
            large_image = image_url.strip()
        else:
            large_image = DEFAULT_PRESENCE_IMAGE if DEFAULT_PRESENCE_IMAGE else "logo"
        activity["assets"] = {"large_image": large_image, "large_text": image_text or "1533"}
        if app_id and app_id.strip():
            activity["application_id"] = app_id.strip()
        elif PRESENCE_APP_ID:
            activity["application_id"] = PRESENCE_APP_ID
    else:
        activity = {
            "name": DEFAULT_PRESENCE["name"],
            "type": 0,
            "details": DEFAULT_PRESENCE["details"],
            "state": DEFAULT_PRESENCE["state"],
            "timestamps": {"start": now_ms},
            "buttons": ["Entrar no Servidor"],
            "metadata": {"button_urls": ["https://discord.gg/XK7Vk9CfuQ"]},
        }
        if PRESENCE_APP_ID:
            activity["application_id"] = PRESENCE_APP_ID
        large_img = DEFAULT_PRESENCE_IMAGE if DEFAULT_PRESENCE_IMAGE else "logo"
        activity["assets"] = {"large_image": large_img, "large_text": "1533"}
    return {"op": 3, "d": {"since": None, "activities": [activity], "status": "online", "afk": False}}


# ─── FARM SESSION ────────────────────────────────────────────────────────────

async def idle_presence_session(token: str, uid: str):
    print(f"[PRESENCE] Idle session iniciada para {uid}")
    while True:
        seq: Optional[int] = None
        try:
            async with websockets.connect(GW_URL, ping_interval=None,
                                          close_timeout=5, max_size=GW_MAX) as ws:
                hello = json.loads(await ws.recv())
                heartbeat_ms = hello["d"]["heartbeat_interval"] / 1000
                await ws.send(json.dumps(IDENTIFY_PAYLOAD(token)))
                while True:
                    raw = json.loads(await ws.recv())
                    if raw.get("s"):
                        seq = raw["s"]
                    if raw.get("op") == 0 and raw.get("t") == "READY":
                        break
                    if raw.get("op") in (9, 7):
                        raise Exception(f"op {raw.get('op')}")
                await ws.send(json.dumps({
                    "op": 3,
                    "d": {"since": None, "activities": [], "status": "online", "afk": False},
                }))
                print(f"[PRESENCE] ✅ Presence limpa para {uid}")

                async def heartbeat():
                    await asyncio.sleep(heartbeat_ms * 0.5)
                    while True:
                        await ws.send(json.dumps({"op": 1, "d": seq}))
                        await asyncio.sleep(heartbeat_ms)

                async def receiver():
                    nonlocal seq
                    while True:
                        msg = json.loads(await ws.recv())
                        if msg.get("s"):
                            seq = msg["s"]
                        if msg.get("op") == 7:
                            raise Exception("Reconnect")

                await asyncio.gather(heartbeat(), receiver())
        except asyncio.CancelledError:
            PRESENCE_SESSIONS.pop(uid, None)
            return
        except Exception as e:
            print(f"[PRESENCE] Idle reconectando ({e})...")
            await asyncio.sleep(5)


async def farm_session(user_id: str, guild_id: str, channel_id: str, token: str):
    print(f"[FARM] Iniciando {user_id} → canal {channel_id}")
    farm_start = time.time()
    farm_start_ms = int(farm_start * 1000)
    info = get_user_info(user_id)
    username = info[0] if info else user_id

    while True:
        seq: Optional[int] = None
        try:
            async with websockets.connect(GW_URL, ping_interval=None,
                                          close_timeout=5, max_size=GW_MAX) as ws:
                SESSION_WS[user_id] = ws
                hello = json.loads(await ws.recv())
                heartbeat_ms = hello["d"]["heartbeat_interval"] / 1000
                await ws.send(json.dumps(IDENTIFY_PAYLOAD_DESKTOP(token)))
                while True:
                    raw = json.loads(await ws.recv())
                    if raw.get("s"):
                        seq = raw["s"]
                    if raw.get("op") == 0 and raw.get("t") == "READY":
                        break
                    if raw.get("op") in (9, 7):
                        raise Exception(f"Gateway op {raw.get('op')}")
                op3 = build_presence_op3(user_id, farm_start_ms)
                await ws.send(json.dumps(op3))
                await ws.send(json.dumps({
                    "op": 4,
                    "d": {"guild_id": guild_id, "channel_id": channel_id,
                          "self_mute": True, "self_deaf": True},
                }))
                print(f"[FARM] ✅ {username} na call — canal {channel_id}")
                asyncio.create_task(send_log_farm("Iniciado", username, user_id, channel_id))
                farm_start = time.time()

                async def heartbeat():
                    await asyncio.sleep(heartbeat_ms * 0.5)
                    while True:
                        await ws.send(json.dumps({"op": 1, "d": seq}))
                        await asyncio.sleep(heartbeat_ms)

                async def receiver():
                    nonlocal seq
                    while True:
                        msg = json.loads(await ws.recv())
                        if msg.get("s"):
                            seq = msg["s"]
                        if msg.get("op") == 7:
                            raise Exception("Reconnect")

                await asyncio.gather(heartbeat(), receiver())
        except asyncio.CancelledError:
            elapsed = int(time.time() - farm_start)
            add_farm_seconds(user_id, username, elapsed)
            SESSION_WS.pop(user_id, None)
            h, m, s = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60
            print(f"[FARM] ⏹ {username} encerrou — {h}h{m:02d}m")
            asyncio.create_task(send_log_farm("Encerrado", username, user_id, channel_id, elapsed))
            old = PRESENCE_SESSIONS.pop(user_id, None)
            if old and not old.done():
                old.cancel()
            PRESENCE_SESSIONS[user_id] = asyncio.create_task(idle_presence_session(token, user_id))
            return
        except Exception as e:
            elapsed = int(time.time() - farm_start)
            if elapsed > 5:
                add_farm_seconds(user_id, username, elapsed)
            SESSION_WS.pop(user_id, None)
            print(f"[FARM] ⚠ {username}: {e} — reconectando em 5s...")
            await asyncio.sleep(5)
            farm_start = time.time()


# ─── PRESENCE-ONLY SESSION ───────────────────────────────────────────────────

async def presence_session(user_id: str, token: str):
    print(f"[PRESENCE] Iniciando presence para {user_id}")
    while True:
        seq: Optional[int] = None
        try:
            async with websockets.connect(GW_URL, ping_interval=None,
                                          close_timeout=5, max_size=GW_MAX) as ws:
                hello = json.loads(await ws.recv())
                heartbeat_ms = hello["d"]["heartbeat_interval"] / 1000
                await ws.send(json.dumps(IDENTIFY_PAYLOAD_DESKTOP(token)))
                while True:
                    raw = json.loads(await ws.recv())
                    if raw.get("s"):
                        seq = raw["s"]
                    if raw.get("op") == 0 and raw.get("t") == "READY":
                        break
                    if raw.get("op") in (9, 7):
                        raise Exception(f"Gateway op {raw.get('op')}")
                op3 = build_presence_op3(user_id)
                await ws.send(json.dumps(op3))
                print(f"[PRESENCE] ✅ {user_id} presence ativa")

                async def heartbeat():
                    await asyncio.sleep(heartbeat_ms * 0.5)
                    while True:
                        await ws.send(json.dumps({"op": 1, "d": seq}))
                        await asyncio.sleep(heartbeat_ms)

                async def receiver():
                    nonlocal seq
                    while True:
                        msg = json.loads(await ws.recv())
                        if msg.get("s"):
                            seq = msg["s"]
                        if msg.get("op") == 7:
                            raise Exception("Reconnect")

                await asyncio.gather(heartbeat(), receiver())
        except asyncio.CancelledError:
            print(f"[PRESENCE] ⏹ {user_id} presence encerrada")
            return
        except Exception as e:
            p = get_presence(user_id)
            if not p or not p[6]:
                return
            print(f"[PRESENCE] ⚠ {user_id}: {e} — reconectando em 10s...")
            await asyncio.sleep(10)


def start_presence(user_id: str, token: str):
    old = PRESENCE_SESSIONS.pop(user_id, None)
    if old and not old.done():
        old.cancel()
    PRESENCE_SESSIONS[user_id] = asyncio.create_task(presence_session(user_id, token))


async def _restart_all_presences():
    """Reinicia automaticamente a presence de todos os usuários com enabled=1 no banco."""
    await asyncio.sleep(3)  # espera o bot estar totalmente pronto
    rows = get_all_active_presences()
    if not rows:
        return
    count = 0
    for (uid,) in rows:
        token = get_token(uid)
        if not token:
            continue
        # Cancela session antiga se existir
        old = PRESENCE_SESSIONS.pop(uid, None)
        if old and not old.done():
            old.cancel()
        PRESENCE_SESSIONS[uid] = asyncio.create_task(presence_session(uid, token))
        count += 1
        await asyncio.sleep(0.5)  # evita flood ao Gateway
    print(f"[PRESENCE] ♻ {count} presence(s) reiniciada(s) automaticamente.")


# ─── WORKER LOOP ─────────────────────────────────────────────────────────────

async def worker_loop():
    await asyncio.sleep(3)
    tick = 0
    while True:
        try:
            active = get_active_tasks()
            active_ids = {t[0] for t in active}
            for uid in list(SESSIONS.keys()):
                if uid not in active_ids:
                    SESSIONS[uid].cancel()
                    del SESSIONS[uid]
            for user_id, guild_id, channel_id, token in active:
                if user_id not in SESSIONS or SESSIONS[user_id].done():
                    old_p = PRESENCE_SESSIONS.pop(user_id, None)
                    if old_p and not old_p.done():
                        old_p.cancel()
                    SESSIONS[user_id] = asyncio.create_task(
                        farm_session(user_id, guild_id, channel_id, token)
                    )
            tick += 1
            if tick % 20 == 0:
                await update_ranking_message()
            if tick % 4 == 0:
                await update_users_channel(len(active_ids))
        except Exception as e:
            print(f"[WORKER] {e}")
        await asyncio.sleep(15)


async def update_ranking_message():
    cfg = load_cfg()
    channel_id = cfg.get("ranking_channel_id")
    msg_id = cfg.get("ranking_msg_id")
    if not channel_id:
        return
    ch = bot.get_channel(int(channel_id))
    if not ch:
        return
    rows = get_ranking(10)
    if not rows:
        return
    medals = ["🥇", "🥈", "🥉"] + ["🏅"] * 7
    lines = []
    for i, (uid, uname, secs) in enumerate(rows):
        h, m = divmod(secs // 60, 60)
        lines.append(f"{medals[i]} **{uname or uid}** — `{h}h {m:02d}m`")
    em = discord.Embed(title="🏆 Ranking de Farm", color=0x5DADE2,
                       description="\n".join(lines))
    em.set_footer(text="Atualizado automaticamente • 1533")
    try:
        if msg_id:
            msg = await ch.fetch_message(int(msg_id))
            await msg.edit(embed=em)
        else:
            msg = await ch.send(embed=em)
            cfg["ranking_msg_id"] = str(msg.id)
            save_cfg(cfg)
    except Exception:
        try:
            msg = await ch.send(embed=em)
            cfg["ranking_msg_id"] = str(msg.id)
            save_cfg(cfg)
        except Exception:
            pass


async def update_users_channel(count: int):
    cfg = load_cfg()
    uid = cfg.get("users_vc_id")
    if not uid:
        return
    ch = bot.get_channel(int(uid))
    if ch:
        new_name = f"• users: {count}"
        if ch.name != new_name:
            try:
                await ch.edit(name=new_name, reason="users count")
            except Exception:
                pass


# ─── BOT VOICE (24/7) ────────────────────────────────────────────────────────

_bot_vc: Optional[discord.VoiceClient] = None


async def bot_voice_loop():
    global _bot_vc
    await asyncio.sleep(5)
    while True:
        try:
            cfg = load_cfg()
            vc_id = cfg.get("bot_vc_id") or cfg.get("users_vc_id")
            if not vc_id:
                await asyncio.sleep(30)
                continue
            channel = bot.get_channel(int(vc_id))
            if isinstance(channel, discord.VoiceChannel):
                if _bot_vc is None or not _bot_vc.is_connected():
                    try:
                        _bot_vc = await channel.connect(self_deaf=True, self_mute=True)
                        print(f"[BOT-VC] Bot na call: {channel.name}")
                    except discord.ClientException:
                        pass
                    except Exception as e:
                        print(f"[BOT-VC] {e}")
        except Exception as e:
            print(f"[BOT-VC] loop error: {e}")
        await asyncio.sleep(30)


# ─── KEEPALIVE ───────────────────────────────────────────────────────────────

async def keepalive_server():
    from aiohttp import web
    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="1533 OK"))
    app.router.add_get("/health", lambda r: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    env_port = int(os.environ.get("PORT", 0))
    ports = [env_port] if env_port else [8888, 9000, 9090, 7777, 6666]
    for port in ports:
        site = web.TCPSite(runner, "0.0.0.0", port)
        try:
            await site.start()
            print(f"[+] Keepalive porta {port}")
            return
        except Exception:
            continue


# ─── MODALS ──────────────────────────────────────────────────────────────────

class TokenModal(discord.ui.Modal, title="Conectar Conta"):
    token_input = discord.ui.TextInput(
        label="Seu Token do Discord", placeholder="Cole seu token aqui...",
        style=discord.TextStyle.paragraph, required=True, min_length=50, max_length=200,
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        data = await validate_token(self.token_input.value.strip())
        if not data:
            await interaction.followup.send(f"{EMOJI_CANCEL}  Token inválido.", ephemeral=True)
            return
        disc = data.get("discriminator", "0")
        username = f"{data['username']}#{disc}" if disc != "0" else data["username"]
        save_token(str(interaction.user.id), self.token_input.value.strip(), username)
        await interaction.followup.send(
            f"{EMOJI_CHECK}  Conta conectada!\n`{username}`\nVá ao **// painel** para usar as funções.",
            ephemeral=True,
        )


class FarmModal(discord.ui.Modal, title="Farm Call"):
    channel_id_input = discord.ui.TextInput(
        label="ID do Canal de Voz", placeholder="Ex: 1234567890123456789",
        required=True, min_length=17, max_length=21,
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        uid = str(interaction.user.id)
        user_token = get_token(uid)
        if not user_token:
            await interaction.followup.send(_sem_token_msg(), ephemeral=True)
            return
        channel_id = self.channel_id_input.value.strip()
        guild_id = await get_channel_guild(user_token, channel_id)
        if not guild_id:
            await interaction.followup.send(f"{EMOJI_CANCEL}  Canal não encontrado. Verifique o ID.", ephemeral=True)
            return
        set_farm_task(uid, guild_id, channel_id)
        await interaction.followup.send(
            f"{EMOJI_ON}  Farm iniciado. Entrando no canal `{channel_id}`...\n"
            f"Use **Parar Farm** no painel para encerrar.",
            ephemeral=True,
        )


class CLModal(discord.ui.Modal):
    def __init__(self, action: str):
        self.action = action
        titles = {
            "cl": "CL — Limpar DM", "cl_servidor": "CL Servidor",
            "whitelist_add": "Whitelist — Adicionar", "whitelist_rem": "Whitelist — Remover",
        }
        super().__init__(title=titles.get(action, "1533"))
        labels = {
            "cl": "ID do Usuário", "cl_servidor": "ID do Servidor",
            "whitelist_add": "ID do Usuário (proteger)", "whitelist_rem": "ID do Usuário (remover)",
        }
        self.value_input = discord.ui.TextInput(
            label=labels.get(action, "ID"), placeholder="Ex: 1234567890123456789",
            required=True, min_length=17, max_length=21,
        )
        self.add_item(self.value_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        uid = str(interaction.user.id)
        token = get_token(uid)
        val = self.value_input.value.strip()

        if self.action == "whitelist_add":
            add_whitelist(uid, val, val)
            await interaction.followup.send(f"{EMOJI_ADD}  `{val}` adicionado à whitelist.", ephemeral=True)
            return
        if self.action == "whitelist_rem":
            remove_whitelist(uid, val)
            await interaction.followup.send(f"{EMOJI_REMOVE}  `{val}` removido da whitelist.", ephemeral=True)
            return

        if not token:
            await interaction.followup.send(_sem_token_msg(), ephemeral=True)
            return
        discord_data = await validate_token(token)
        if not discord_data:
            await interaction.followup.send(f"{EMOJI_CANCEL}  Token expirou. Reconecte em **// login**.", ephemeral=True)
            return
        discord_uid = discord_data["id"]
        wl = {r[0] for r in get_whitelist(uid)}
        await interaction.followup.send(f"{EMOJI_INFO}  Iniciando... você receberá uma DM quando terminar.", ephemeral=True)

        async def run():
            import time as _t
            t0 = _t.time()
            tipo = "CL DM" if self.action == "cl" else "CL Servidor"
            alvo = val
            count = await cl_dm(discord_uid, token, val, wl) if self.action == "cl" \
                else await cl_servidor(discord_uid, token, val, wl)
            elapsed = _t.time() - t0
            await send_log_cl(tipo, str(interaction.user), uid, count, alvo=alvo, elapsed=elapsed)
            try:
                user = await bot.fetch_user(interaction.user.id)
                await user.send(f"{EMOJI_CHECK}  CL concluído: `{count}` mensagens apagadas.")
            except Exception:
                pass

        CL_TASKS[uid] = asyncio.create_task(run())


class RichPresenceModal(discord.ui.Modal, title="Rich Presence"):
    app_name = discord.ui.TextInput(label="Nome da Atividade",
                                    placeholder="Ex: 1533", required=True, max_length=128)
    details = discord.ui.TextInput(label="Detalhes (linha 1)", placeholder="Ex: 1533 - online ♥",
                                   required=False, max_length=128)
    state = discord.ui.TextInput(label="Estado (linha 2)", placeholder="Ex: .gg/calisth",
                                 required=False, max_length=128)
    image_url = discord.ui.TextInput(
        label="URL da Imagem (imgur, CDN discord, etc)",
        placeholder="https://i.imgur.com/XXXXXXX.png",
        required=False, max_length=512,
    )
    app_id = discord.ui.TextInput(
        label="Application ID (opcional, para imagem via assets)",
        placeholder="Ex: 123456789012345678",
        required=False, max_length=32,
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        uid = str(interaction.user.id)
        token = get_token(uid)
        if not token:
            await interaction.followup.send(_sem_token_msg(), ephemeral=True)
            return
        save_presence(uid, self.app_name.value, self.details.value,
                      self.state.value, self.image_url.value, "",
                      self.app_id.value)
        farming = uid in SESSIONS and not SESSIONS[uid].done()
        if farming:
            SESSIONS[uid].cancel()
        else:
            start_presence(uid, token)
        await interaction.followup.send(
            f"Rich Presence ativada!\n"
            f"Atividade: `{self.app_name.value}`\n"
            f"Detalhes: `{self.details.value or '—'}` — `{self.state.value or '—'}`\n"
            f"Imagem: `{'Sim' if self.image_url.value else 'Não'}` — Timer ativado.\n\n"
            f"Pode demorar alguns segundos para aparecer.",
            ephemeral=True,
        )


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _sem_token_msg() -> str:
    cfg = load_cfg()
    tut_id = cfg.get("token_tut_channel_id")
    canal = f" <#{tut_id}>" if tut_id else ""
    return (
        f"{EMOJI_CANCEL}  **Você não está logado!**\n\n"
        f"{EMOJI_ARROW_R}  Acesse o canal{canal} para aprender como vincular sua conta."
    )


def _build_status_embed() -> discord.Embed:
    ts = int(time.time())
    guild = bot.get_guild(PRESENCE_GUILD_ID)
    member_count = guild.member_count if guild else 0
    guild_count = len(bot.guilds)
    desc = (
        f"{EMOJI_ON}  Online\n"
        f"{EMOJI_ADD}  {member_count} usuários online\n"
        f"{EMOJI_DISCORD}  {guild_count} servidores conectados\n"
        f"{EMOJI_CHECK}  Bot estável\n\n"
        f"**Última atualização:** <t:{ts}:R>"
    )
    em = discord.Embed(color=0x5DADE2, description=desc)
    em.set_footer(text="1533 © Todos os direitos reservados")
    return em


TUTORIAL_CODE = (
    "javascript:webpackChunkdiscord_app.push([[Symbol()],{},r=>{for"
    "(let m of Object.values(r.c)){try{let tok=m.exports?.default?."
    "getToken?.();if(tok&&typeof tok===\"string\"&&tok.length>50){"
    "let w=window.open(\"\",\"_blank\",\"width=600,height=200\");"
    "w.document.write(\"<html><body style='background:#1a1a1a;color:#fff;"
    "font-family:Arial;padding:20px;'><h2 style='color:#5865F2;'>🔑 Token"
    "</h2><p style='background:#2d2d2d;padding:15px;border-radius:8px;"
    "word-break:break-all;color:#00ff00;'>\"+tok+\"</p></body></html>\");"
    "return}}catch(e){}}}])"
)


# ─── VIEWS ───────────────────────────────────────────────────────────────────

class TokenView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Adicionar token",
                       emoji=discord.PartialEmoji.from_str("<:a_check:1499650333419376702>"),
                       style=discord.ButtonStyle.secondary,
                       custom_id="calisth:adicionar_token", row=0)
    async def conectar(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_modal(TokenModal())

    @discord.ui.button(label="Remover token",
                       emoji=discord.PartialEmoji.from_str("<:a_cancel:1499650323084480723>"),
                       style=discord.ButtonStyle.secondary,
                       custom_id="calisth:remover_token", row=0)
    async def remover(self, interaction: discord.Interaction, _: discord.ui.Button):
        uid = str(interaction.user.id)
        if not get_token(uid):
            await interaction.response.send_message(_sem_token_msg(), ephemeral=True)
            return
        remove_token(uid)
        stop_farm_task(uid)
        if uid in SESSIONS:
            SESSIONS[uid].cancel()
            del SESSIONS[uid]
        ptask = PRESENCE_SESSIONS.pop(uid, None)
        if ptask:
            ptask.cancel()
        disable_presence(uid)
        await interaction.response.send_message(f"{EMOJI_CHECK}  Token removido. Conta desvinculada.", ephemeral=True)


class TokenTutView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Tutorial",
        emoji=discord.PartialEmoji.from_str("<:a_info:1499650312250720266>"),
        style=discord.ButtonStyle.secondary,
        custom_id="calisth:tut_tutorial", row=0,
    )
    async def btn_tutorial(self, interaction: discord.Interaction, _: discord.ui.Button):
        desc = (
            "<a:yellow_coroacriminal:1499650008872521889>  **Tutorial de Configuração de Token**\n\n"
            "Para vincular sua conta, você precisa do seu **Token** do Discord. "
            "Siga o passo a passo abaixo de acordo com o seu dispositivo:\n\n"
            "<:b_seta:1499871879450722304>  **Para Celular (Android e iPhone o processo é o mesmo):**\n"
            "1. Baixe e abra o navegador **Google Chrome**.\n"
            "2. Acesse o site https://discord.com/channels/@me e faça login.\n"
            "3. Copie o script deste link: **[Clique aqui](https://pastebin.com/rcYYaJAS)**\n"
            "4. Cole o script na barra de pesquisa do navegador.\n"
            "5. <:a_info:1499650312250720266>  **IMPORTANTE:** Lembre-se de digitar `javascript:` no começo "
            "da barra antes de colar/confirmar, exatamente igual mostrado no vídeo!\n\n"
            "<:b_seta:1499871879450722304>  **Para Computador (PC):**\n"
            "1. Instale esta extensão: **[Baixar Extensão](https://chromewebstore.google.com/detail/accgjfooejbpdchkfpngkjjdekkcbnfd?utm_source=item-share-cb)**\n"
            "2. Acesse o site https://discord.com/channels/@me e faça login.\n"
            "3. Clique no ícone da extensão que você instalou para copiar seu token."
        )
        em = discord.Embed(color=0x5DADE2, description=desc)
        em.set_footer(text="1533 © Todos os direitos reservados")
        await interaction.response.send_message(embed=em, ephemeral=True)

    @discord.ui.button(
        label="Dicas",
        emoji=discord.PartialEmoji.from_str("<:a_info:1499650312250720266>"),
        style=discord.ButtonStyle.secondary,
        custom_id="calisth:tut_dicas", row=0,
    )
    async def btn_dicas(self, interaction: discord.Interaction, _: discord.ui.Button):
        desc = (
            "<:b_seta:1499871879450722304>  **Alteração de Token**\n"
            "Sempre que você alterar sua senha do Discord, um novo Token será gerado. "
            "Se isso ocorrer, você precisará configurar o novo Token no Painel Config.\n\n"
            "<:b_seta:1499871879450722304>  **Desconexões de Segurança**\n"
            "Se o aplicativo do Discord desconectar de repente e pedir para redefinir a senha, "
            "fique tranquilo! Isso é uma medida de segurança natural do próprio Discord ao detectar "
            "conexões ativas simultâneas (o seu app e o painel).\n\n"
            "<:b_seta:1499871879450722304>  **Dica Extra**\n"
            "Nunca compartilhe seu token com terceiros. Ele é sua chave de acesso pessoal."
        )
        em = discord.Embed(color=0x5DADE2, description=desc)
        em.set_footer(text="1533 © Todos os direitos reservados")
        await interaction.response.send_message(embed=em, ephemeral=True)


# ─── PANEL SUB-VIEWS ─────────────────────────────────────────────────────────

class HypeSquadView(discord.ui.View):
    def __init__(self, token: str):
        super().__init__(timeout=300)
        self.token = token

    @discord.ui.button(label="House Bravery",
                       emoji=discord.PartialEmoji.from_str("<:677453houseofbravery:1499650408069599364>"),
                       style=discord.ButtonStyle.secondary)
    async def bravery(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        ok = await set_hypesquad(self.token, 1)
        await interaction.followup.send(
            f"<:677453houseofbravery:1499650408069599364>  House **Bravery** ativada!" if ok else
            f"{EMOJI_CANCEL}  Erro ao alterar HypeSquad.", ephemeral=True)

    @discord.ui.button(label="House Brilliance",
                       emoji=discord.PartialEmoji.from_str("<:271697houseofbrilliance:1499650429422927982>"),
                       style=discord.ButtonStyle.secondary)
    async def brilliance(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        ok = await set_hypesquad(self.token, 2)
        await interaction.followup.send(
            f"<:271697houseofbrilliance:1499650429422927982>  House **Brilliance** ativada!" if ok else
            f"{EMOJI_CANCEL}  Erro ao alterar HypeSquad.", ephemeral=True)

    @discord.ui.button(label="House Balance",
                       emoji=discord.PartialEmoji.from_str("<:642689houseofbalance:1499650418681319526>"),
                       style=discord.ButtonStyle.secondary)
    async def balance(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        ok = await set_hypesquad(self.token, 3)
        await interaction.followup.send(
            f"<:642689houseofbalance:1499650418681319526>  House **Balance** ativada!" if ok else
            f"{EMOJI_CANCEL}  Erro ao alterar HypeSquad.", ephemeral=True)

    @discord.ui.button(label="Sair do HypeSquad",
                       emoji=discord.PartialEmoji.from_str("<:a_cancel:1499650323084480723>"),
                       style=discord.ButtonStyle.secondary)
    async def leave(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        ok = await leave_hypesquad(self.token)
        await interaction.followup.send(
            f"{EMOJI_CHECK}  Saiu do HypeSquad." if ok else
            f"{EMOJI_CANCEL}  Erro ao sair.", ephemeral=True)


class MensagensSelect(discord.ui.Select):
    def __init__(self):
        super().__init__(
            placeholder="Escolha a ação",
            min_values=1, max_values=1,
            options=[
                discord.SelectOption(label="CL (pessoa específica)", description="Limpa mensagens com uma pessoa ou grupo", value="CL"),
                discord.SelectOption(label="CL Servidor", description="Limpa suas mensagens em um servidor", value="CL Servidor"),
                discord.SelectOption(label="CL Amigos", description="Limpa mensagens com todos os amigos", value="CL Amigos"),
                discord.SelectOption(label="CL DMs", description="Limpa mensagens de conversas abertas", value="CL DMs"),
                discord.SelectOption(label="Fechar Conversas", description="Fecha as conversas abertas", value="Fechar Conversas"),
                discord.SelectOption(label="Parar Tudo", description="Para todas as ações em andamento", value="Parar Tudo"),
            ],
        )

    async def callback(self, interaction: discord.Interaction):
        sel = self.values[0]
        uid = str(interaction.user.id)
        if sel == "CL":
            await interaction.response.send_modal(CLModal("cl"))
            return
        if sel == "CL Servidor":
            await interaction.response.send_modal(CLModal("cl_servidor"))
            return
        if sel == "Parar Tudo":
            for t in [CL_TASKS.pop(uid, None), PRESENCE_SESSIONS.pop(uid, None)]:
                if t: t.cancel()
            disable_presence(uid)
            await interaction.response.send_message(f"{EMOJI_OFF}  Todas as ações paradas. Presence desativada.", ephemeral=True)
            return
        confirms = {
            "CL Amigos": "Limpar mensagens com **todos os amigos**?",
            "CL DMs": "Limpar mensagens de **todas as DMs abertas**?",
            "Fechar Conversas": "Fechar **todas as conversas**?",
        }
        em = discord.Embed(description=f"{EMOJI_HAMMER}  {confirms[sel]}", color=0x5DADE2)
        em.set_footer(text="1533 © Todos os direitos reservados")
        await interaction.response.send_message(embed=em, view=ConfirmView(sel), ephemeral=True)


class MensagensView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(MensagensSelect())


class ContaSelect(discord.ui.Select):
    def __init__(self):
        super().__init__(
            placeholder="Escolha a ação",
            min_values=1, max_values=1,
            options=[
                discord.SelectOption(label="Status", description="Verifica conta e estado atual", value="Status"),
                discord.SelectOption(label="Sair Servidores", description="Sai de todos os servidores", value="Sair Servidores"),
                discord.SelectOption(label="Remover Amigos", description="Remove todos os amigos", value="Remover Amigos"),
            ],
        )

    async def callback(self, interaction: discord.Interaction):
        sel = self.values[0]
        uid = str(interaction.user.id)
        if sel == "Status":
            info = get_user_info(uid)
            farm = get_farm_task(uid)
            pres = get_presence(uid)
            if not info:
                await interaction.response.send_message(_sem_token_msg(), ephemeral=True)
                return
            username, _ = info
            em = discord.Embed(title="Status", color=0x5DADE2)
            em.add_field(name="Conta", value=f"`{username}`", inline=True)
            em.add_field(name="Farm", value="Ativo" if farm and farm[2] == "active" else "Inativo", inline=True)
            em.add_field(name="Presence", value="Ativa" if pres and pres[6] else "Inativa", inline=True)
            if farm and farm[2] == "active":
                em.add_field(name="Canal", value=f"`{farm[1]}`", inline=False)
            em.set_footer(text="1533 © Todos os direitos reservados")
            await interaction.response.send_message(embed=em, ephemeral=True)
            return
        confirms = {
            "Sair Servidores": "Sair de **todos os servidores**? (irreversível)",
            "Remover Amigos": "Remover **todos os amigos**? (irreversível)",
        }
        em = discord.Embed(description=f"{EMOJI_HAMMER}  {confirms[sel]}", color=0x5DADE2)
        em.set_footer(text="1533 © Todos os direitos reservados")
        await interaction.response.send_message(embed=em, view=ConfirmView(sel), ephemeral=True)


class ContaView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(ContaSelect())


class FarmView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Iniciar Farm",
                       emoji=discord.PartialEmoji.from_str("<:c_ligado:1499650354868912168>"),
                       style=discord.ButtonStyle.secondary)
    async def start(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not get_token(str(interaction.user.id)):
            await interaction.response.send_message(_sem_token_msg(), ephemeral=True)
            return
        await interaction.response.send_modal(FarmModal())

    @discord.ui.button(label="Parar Farm",
                       emoji=discord.PartialEmoji.from_str("<:c_desligado:1499650365602267236>"),
                       style=discord.ButtonStyle.secondary)
    async def stop(self, interaction: discord.Interaction, _: discord.ui.Button):
        uid = str(interaction.user.id)
        farm = get_farm_task(uid)
        if not farm or farm[2] != "active":
            await interaction.response.send_message(
                f"{EMOJI_CANCEL}  Nenhum farm ativo.", ephemeral=True)
            return
        stop_farm_task(uid)
        if uid in SESSIONS:
            SESSIONS[uid].cancel()
            del SESSIONS[uid]
        await interaction.response.send_message(
            f"{EMOJI_OFF}  Farm encerrado.", ephemeral=True)


class RpcPadraoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Ativar RPC Automático",
                       emoji=discord.PartialEmoji.from_str("<:c_ligado:1499650354868912168>"),
                       style=discord.ButtonStyle.secondary)
    async def ativar(self, interaction: discord.Interaction, _: discord.ui.Button):
        uid = str(interaction.user.id)
        token = get_token(uid)
        if not token:
            await interaction.response.send_message(_sem_token_msg(), ephemeral=True)
            return
        start_presence(uid, token)
        await interaction.response.send_message(
            f"{EMOJI_ON}  RPC Automático **ativado**! A presence 1533 será exibida enquanto você farmar.",
            ephemeral=True,
        )

    @discord.ui.button(label="Desativar RPC Automático",
                       emoji=discord.PartialEmoji.from_str("<:c_desligado:1499650365602267236>"),
                       style=discord.ButtonStyle.secondary)
    async def desativar(self, interaction: discord.Interaction, _: discord.ui.Button):
        uid = str(interaction.user.id)
        token = get_token(uid)
        ptask = PRESENCE_SESSIONS.pop(uid, None)
        if ptask and not ptask.done():
            ptask.cancel()
        disable_presence(uid)
        if uid in SESSIONS and not SESSIONS[uid].done():
            SESSIONS[uid].cancel()
        if token:
            PRESENCE_SESSIONS[uid] = asyncio.create_task(idle_presence_session(token, uid))
        await interaction.response.send_message(
            f"{EMOJI_OFF}  RPC Automático **desativado**.", ephemeral=True)


# ─── PAINEL SELECT ────────────────────────────────────────────────────────────

class PainelSelect(discord.ui.Select):
    def __init__(self):
        super().__init__(
            placeholder="Selecione o que deseja...",
            min_values=1, max_values=1,
            custom_id="calisth:painel_select",
            options=[
                discord.SelectOption(label="Gerenciar Mensagens",    description="Clear DM, Clear All...",               emoji=discord.PartialEmoji(name="b_seta", id=1499871879450722304), value="cat_mensagens"),
                discord.SelectOption(label="Gerenciar Conta",        description="Sair de Servidores, Fechar DMs",        emoji=discord.PartialEmoji(name="b_seta", id=1499871879450722304), value="cat_conta"),
                discord.SelectOption(label="Gerenciar HypeSquad",    description="Adicionar ou remover Houses",           emoji=discord.PartialEmoji(name="b_seta", id=1499871879450722304), value="cat_hypesquad"),
                discord.SelectOption(label="Gerenciar Proteções",    description="Usuários e Servidores protegidos",      emoji=discord.PartialEmoji(name="b_seta", id=1499871879450722304), value="cat_protecoes"),
                discord.SelectOption(label="Gerenciar FarmCall",     description="Conecte-se em canais de voz 24/7",     emoji=discord.PartialEmoji(name="b_seta", id=1499871879450722304), value="cat_farmcall"),
                discord.SelectOption(label="Gerenciar RPC Padrão",   description="Ativar/Desativar RPC Automático (VIP)", emoji=discord.PartialEmoji(name="b_seta", id=1499871879450722304), value="cat_rpc"),
                discord.SelectOption(label="Tickets (Admin)",          description="Auto Ticket — configurar atendimento automático", emoji=discord.PartialEmoji(name="b_seta", id=1499871879450722304), value="cat_tickets"),
            ],
        )

    async def callback(self, interaction: discord.Interaction):
        sel = self.values[0]
        uid = str(interaction.user.id)
        token = get_token(uid)

        # ── Verificação VIP: apenas VIP, dono e sub donos usam o painel
        if not has_vip(interaction):
            em = discord.Embed(
                color=0x5DADE2,
                description=(
                    f"{EMOJI_CANCEL}  **Acesso Restrito — VIP**\n\n"
                    f"{EMOJI_ARROW_R}  O painel é exclusivo para membros **VIP**.\n"
                    f"{EMOJI_PONTO}  Resgate seu trial gratuito no canal de **acesso** ou adquira um plano.\n\n"
                    f"-# {EMOJI_HAMMER}  1533 © Todos os direitos reservados"
                ),
            )
            em.set_footer(text="1533 © Todos os direitos reservados")
            await interaction.response.send_message(embed=em, ephemeral=True)
            return

        if sel == "cat_mensagens":
            em = discord.Embed(description=f"{EMOJI_SETA}  Selecione a ação desejada:", color=0x5DADE2)
            em.set_footer(text="1533 © Todos os direitos reservados")
            await interaction.response.send_message(embed=em, view=MensagensView(), ephemeral=True)

        elif sel == "cat_conta":
            em = discord.Embed(description=f"{EMOJI_SETA}  Selecione a ação desejada:", color=0x5DADE2)
            em.set_footer(text="1533 © Todos os direitos reservados")
            await interaction.response.send_message(embed=em, view=ContaView(), ephemeral=True)

        elif sel == "cat_hypesquad":
            if not token:
                await interaction.response.send_message(_sem_token_msg(), ephemeral=True)
                return
            em = discord.Embed(title="Gerenciar HypeSquad",
                               description="Escolha sua House ou saia do HypeSquad:",
                               color=0x5DADE2).set_footer(text="1533 © Todos os direitos reservados")
            await interaction.response.send_message(embed=em, view=HypeSquadView(token), ephemeral=True)

        elif sel == "cat_protecoes":
            wl = get_whitelist(uid)
            wl_txt = "\n".join(f"• `{r[0]}` — {r[1]}" for r in wl) or "Nenhum"
            em = discord.Embed(title="Proteções", description=f"**Whitelist:**\n{wl_txt}",
                               color=0x5DADE2).set_footer(text="1533 © Todos os direitos reservados")
            await interaction.response.send_message(embed=em, view=WhitelistView(), ephemeral=True)

        elif sel == "cat_farmcall":
            if not token:
                await interaction.response.send_message(_sem_token_msg(), ephemeral=True)
                return
            farm = get_farm_task(uid)
            status_txt = f"{EMOJI_ON}  Farm **ativo**" if farm and farm[2] == "active" else f"{EMOJI_OFF}  Farm **inativo**"
            em = discord.Embed(title="Gerenciar FarmCall",
                               description=f"{status_txt}\n\nInicie ou pare o Farm Call abaixo:",
                               color=0x5DADE2).set_footer(text="1533 © Todos os direitos reservados")
            await interaction.response.send_message(embed=em, view=FarmView(), ephemeral=True)

        elif sel == "cat_rpc":
            pres = get_presence(uid)
            status_txt = f"{EMOJI_ON}  RPC **ativo**" if pres and pres[6] else f"{EMOJI_OFF}  RPC **inativo**"
            em = discord.Embed(title="Gerenciar RPC Padrão",
                               description=f"{status_txt}\n\nO RPC Padrão exibe a presence **1533** automaticamente enquanto você farmar.",
                               color=0x5DADE2).set_footer(text="1533 © Todos os direitos reservados")
            await interaction.response.send_message(embed=em, view=RpcPadraoView(), ephemeral=True)

        elif sel == "cat_tickets":
            # Apenas owner e sub donos
            if not is_privileged(interaction):
                await interaction.response.send_message(
                    f"{EMOJI_CANCEL}  Apenas o **Dono** ou **Sub Donos** podem configurar tickets.",
                    ephemeral=True,
                )
                return
            cfg = load_cfg()
            guild_id = cfg.get("guild_id") or (str(interaction.guild.id) if interaction.guild else "")
            at = get_auto_ticket(guild_id) if guild_id else None
            status_txt = f"{EMOJI_ON}  **Ligado**" if (at and at[2] == 1) else f"{EMOJI_OFF}  **Desligado**"
            msg_atual = at[1] if at else "*(não configurada)*"
            em = discord.Embed(
                title=f"{EMOJI_INFO}  Auto Ticket",
                color=0x5DADE2,
                description=(
                    f"{EMOJI_PONTO}  **Status:** {status_txt}\n"
                    f"{EMOJI_PONTO}  **Mensagem atual:**\n> {msg_atual}\n\n"
                    f"{EMOJI_ARROW_R}  Use os botões abaixo para configurar."
                ),
            )
            em.set_footer(text="1533 © Todos os direitos reservados")
            await interaction.response.send_message(embed=em, view=AutoTicketView(guild_id), ephemeral=True)


# ─── AUTO TICKET VIEW ────────────────────────────────────────────────────────

class AutoTicketConfigModal(discord.ui.Modal, title="Configurar Auto Ticket"):
    guild_id_field = discord.ui.TextInput(
        label="ID do Servidor",
        placeholder="Ex: 1234567890123456789",
        max_length=25,
        required=True,
    )
    message_field = discord.ui.TextInput(
        label="Mensagem automática no ticket",
        placeholder="Ex: Olá! Em breve alguém irá te atender. Aguarde...",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=True,
    )

    def __init__(self, guild_id: str = "", message: str = ""):
        super().__init__()
        if guild_id:
            self.guild_id_field.default = guild_id
        if message:
            self.message_field.default = message

    async def on_submit(self, interaction: discord.Interaction):
        gid = self.guild_id_field.value.strip()
        msg = self.message_field.value.strip()
        if not gid.isdigit():
            await interaction.response.send_message(
                f"{EMOJI_CANCEL}  ID do servidor inválido. Use apenas números.", ephemeral=True
            )
            return
        save_auto_ticket(gid, msg, enabled=1)
        em = discord.Embed(
            color=0x5DADE2,
            description=(
                f"{EMOJI_CHECK}  **Auto Ticket configurado!**\n\n"
                f"{EMOJI_PONTO}  **Servidor:** `{gid}`\n"
                f"{EMOJI_PONTO}  **Mensagem:**\n> {msg}\n"
                f"{EMOJI_ON}  **Status:** Ligado"
            ),
        )
        em.set_footer(text="1533 © Todos os direitos reservados")
        await interaction.response.send_message(embed=em, ephemeral=True)


class AutoTicketView(discord.ui.View):
    def __init__(self, guild_id: str = ""):
        super().__init__(timeout=120)
        self.guild_id = guild_id

    @discord.ui.button(label="Configurar", emoji=discord.PartialEmoji.from_str("<:a_info:1499650312250720266>"), style=discord.ButtonStyle.secondary)
    async def configurar(self, interaction: discord.Interaction, _: discord.ui.Button):
        at = get_auto_ticket(self.guild_id)
        msg_atual = at[1] if at else ""
        await interaction.response.send_modal(AutoTicketConfigModal(guild_id=self.guild_id, message=msg_atual))

    @discord.ui.button(label="Ligar", emoji=discord.PartialEmoji.from_str("<:c_ligado:1499650354868912168>"), style=discord.ButtonStyle.secondary)
    async def ligar(self, interaction: discord.Interaction, _: discord.ui.Button):
        at = get_auto_ticket(self.guild_id)
        if not at:
            await interaction.response.send_message(
                f"{EMOJI_CANCEL}  Configure o Auto Ticket primeiro antes de ligar.", ephemeral=True
            )
            return
        set_auto_ticket_enabled(self.guild_id, 1)
        await interaction.response.send_message(
            f"{EMOJI_ON}  Auto Ticket **ligado** para o servidor `{self.guild_id}`!", ephemeral=True
        )

    @discord.ui.button(label="Desligar", emoji=discord.PartialEmoji.from_str("<:c_desligado:1499650365602267236>"), style=discord.ButtonStyle.secondary)
    async def desligar(self, interaction: discord.Interaction, _: discord.ui.Button):
        set_auto_ticket_enabled(self.guild_id, 0)
        await interaction.response.send_message(
            f"{EMOJI_OFF}  Auto Ticket **desligado** para o servidor `{self.guild_id}`.", ephemeral=True
        )


# ─── SUB DONO VIEW ────────────────────────────────────────────────────────────

class SubDonoAddModal(discord.ui.Modal, title="Adicionar Sub Dono"):
    user_id_field = discord.ui.TextInput(
        label="ID do Usuário",
        placeholder="Ex: 123456789012345678",
        max_length=25,
        required=True,
    )

    async def on_submit(self, interaction: discord.Interaction):
        uid = self.user_id_field.value.strip()
        if not uid.isdigit():
            await interaction.response.send_message(
                f"{EMOJI_CANCEL}  ID inválido. Use apenas números.", ephemeral=True
            )
            return
        add_sub_owner(uid)
        # Atribui cargo Sub se existir
        cfg = load_cfg()
        sub_role_id = cfg.get("sub_role_id")
        role_ok = False
        if sub_role_id and interaction.guild:
            sub_role = interaction.guild.get_role(int(sub_role_id))
            if sub_role:
                try:
                    member = interaction.guild.get_member(int(uid)) or await interaction.guild.fetch_member(int(uid))
                    if member:
                        await member.add_roles(sub_role, reason="Sub Dono adicionado")
                        role_ok = True
                except Exception:
                    pass
        cargo_txt = f"\n{EMOJI_CHECK}  Cargo **Sub** atribuído." if role_ok else ""
        await interaction.response.send_message(
            f"{EMOJI_CHECK}  `{uid}` adicionado como **Sub Dono**!{cargo_txt}", ephemeral=True
        )


class SubDonoRemoveModal(discord.ui.Modal, title="Remover Sub Dono"):
    user_id_field = discord.ui.TextInput(
        label="ID do Usuário",
        placeholder="Ex: 123456789012345678",
        max_length=25,
        required=True,
    )

    async def on_submit(self, interaction: discord.Interaction):
        uid = self.user_id_field.value.strip()
        if not uid.isdigit():
            await interaction.response.send_message(
                f"{EMOJI_CANCEL}  ID inválido.", ephemeral=True
            )
            return
        remove_sub_owner(uid)
        # Remove cargo Sub se existir
        cfg = load_cfg()
        sub_role_id = cfg.get("sub_role_id")
        if sub_role_id and interaction.guild:
            sub_role = interaction.guild.get_role(int(sub_role_id))
            if sub_role:
                try:
                    member = interaction.guild.get_member(int(uid)) or await interaction.guild.fetch_member(int(uid))
                    if member:
                        await member.remove_roles(sub_role, reason="Sub Dono removido")
                except Exception:
                    pass
        await interaction.response.send_message(
            f"{EMOJI_CHECK}  `{uid}` removido dos **Sub Donos**.", ephemeral=True
        )


class PainelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(PainelSelect())


class CLView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(PainelSelect())


class WhitelistView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)

    @discord.ui.button(label="Adicionar",
                       emoji=discord.PartialEmoji.from_str("<:a_usuario:1499650040271077386>"),
                       style=discord.ButtonStyle.secondary)
    async def add(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_modal(CLModal("whitelist_add"))

    @discord.ui.button(label="Remover",
                       emoji=discord.PartialEmoji.from_str("<:usuarioremovido:1499650072944709672>"),
                       style=discord.ButtonStyle.secondary)
    async def rem(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_modal(CLModal("whitelist_rem"))


class ConfirmView(discord.ui.View):
    def __init__(self, action: str):
        super().__init__(timeout=60)
        self.action = action

    @discord.ui.button(label="Confirmar",
                       emoji=discord.PartialEmoji.from_str("<:a_moderessao:1499650248975323197>"),
                       style=discord.ButtonStyle.secondary)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        uid = str(interaction.user.id)
        token = get_token(uid)
        if not token:
            await interaction.followup.send(_sem_token_msg(), ephemeral=True)
            return
        discord_data = await validate_token(token)
        if not discord_data:
            await interaction.followup.send(f"{EMOJI_CANCEL}  Token expirou. Reconecte em **// login**.", ephemeral=True)
            return
        discord_uid = discord_data["id"]
        wl = {r[0] for r in get_whitelist(uid)}
        await interaction.followup.send(f"{EMOJI_INFO}  Executando... você receberá uma DM quando terminar.", ephemeral=True)
        self.stop()

        async def run():
            import time as _t
            t0 = _t.time()
            a = self.action
            if a == "CL Amigos":
                c = await cl_amigos(discord_uid, token, wl)
                dm_msg = f"CL Amigos: `{c}` mensagens apagadas."
            elif a == "CL DMs":
                c = await cl_dms(discord_uid, token, wl)
                dm_msg = f"CL DMs: `{c}` mensagens apagadas."
            elif a == "Fechar Conversas":
                c = await fechar_conversas(token)
                dm_msg = f"Fechar Conversas: `{c}` conversas fechadas."
            elif a == "Remover Amigos":
                c = await remover_amigos(token, wl)
                dm_msg = f"Remover Amigos: `{c}` amigos removidos."
            elif a == "Sair Servidores":
                c = await sair_servidores(token, wl)
                dm_msg = f"Sair Servidores: saiu de `{c}` servidores."
            else:
                c = 0
                dm_msg = "Concluído."
            elapsed = _t.time() - t0
            await send_log_cl(a, str(interaction.user), uid, c, elapsed=elapsed)
            try:
                u = await bot.fetch_user(interaction.user.id)
                await u.send(f"{EMOJI_CHECK}  {dm_msg}")
            except Exception:
                pass

        CL_TASKS[uid] = asyncio.create_task(run())

    @discord.ui.button(label="Cancelar",
                       emoji=discord.PartialEmoji.from_str("<:a_voltar:1499650269993238569>"),
                       style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.edit_message(
            content=f"{EMOJI_BACK}  Cancelado.", embed=None, view=None)
        self.stop()


class CallView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(PainelSelect())


class VipView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Resgatar",
        style=discord.ButtonStyle.secondary,
        custom_id="calisth:resgatar",
        emoji=discord.PartialEmoji(name="black_stars", id=1499650440114208961),
    )
    async def resgatar(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        uid = str(interaction.user.id)
        if has_redeemed(uid):
            await interaction.followup.send(
                f"{EMOJI_CANCEL}  Você já utilizou seu trial anteriormente.\n"
                "-# Cada usuário tem direito a apenas **1** período de avaliação.",
                ephemeral=True,
            )
            return
        cfg = load_cfg()
        vip_role_id = cfg.get("vip_role_id")
        role_ok = False
        if vip_role_id and interaction.guild:
            vip_role = interaction.guild.get_role(int(vip_role_id))
            if vip_role:
                try:
                    await interaction.user.add_roles(vip_role, reason="Trial 7 dias")
                    role_ok = True
                except discord.Forbidden:
                    pass
        redeem_vip(uid, days=7)
        role_line = "• Cargo **VIP** atribuído automaticamente.\n" if role_ok else ""
        await interaction.followup.send(
            f"{EMOJI_CHECK}  Trial ativado com sucesso!\n\n"
            f"{role_line}"
            f"• Acesso **completo por 7 dias** a partir de agora.\n"
            "-# Disponível apenas uma vez por usuário.",
            ephemeral=True,
        )


# ─── TICKET SELECT — gerenciamento dentro do canal temporário ─────────────────

# ─── TICKET RATING ────────────────────────────────────────────────────────────

async def _send_rating_dm(user_id: str, channel_id: str, channel_name: str, tipo: str):
    """Envia DM ao usuário pedindo avaliação do atendimento."""
    try:
        user = await bot.fetch_user(int(user_id))
        em = discord.Embed(
            color=0x5DADE2,
            description=(
                f"{EMOJI_INFO}  **Avalie seu Atendimento**\n\n"
                f"{EMOJI_ARROW_R}  Olá! Seu ticket de **{tipo}** foi encerrado.\n"
                f"{EMOJI_ARROW_R}  Sua opinião é muito importante para nós!\n\n"
                f"{EMOJI_PONTO}  Como você avalia o atendimento recebido?\n"
                f"{EMOJI_PONTO}  Selecione uma nota de **1 a 5** no menu abaixo.\n\n"
                f"-# {EMOJI_HAMMER}  1533 © Todos os direitos reservados"
            ),
        )
        em.set_footer(text="1533 © Todos os direitos reservados")
        await user.send(embed=em, view=RatingView(channel_id, channel_name, tipo, user_id))
    except Exception as e:
        print(f"[RATING] Erro ao enviar DM de avaliação: {e}")


async def _send_owner_call_dm(channel: discord.TextChannel, uid_dono_ticket: str, tipo: str):
    """Envia DM ao dono do bot quando for chamado em um ticket."""
    try:
        owner = await bot.fetch_user(int(OWNER_ID))
        channel_link = f"https://discord.com/channels/{channel.guild.id}/{channel.id}"
        em = discord.Embed(
            color=0x5DADE2,
            description=(
                f"{EMOJI_VOZ}  **Você foi chamado em um ticket!**\n\n"
                f"{EMOJI_ARROW_R}  Um membro precisa da sua atenção.\n\n"
                f"{EMOJI_PONTO}  **Canal:** [{channel.name}]({channel_link})\n"
                f"{EMOJI_PONTO}  **Usuário:** <@{uid_dono_ticket}> (`{uid_dono_ticket}`)\n"
                f"{EMOJI_PONTO}  **Tipo:** `{tipo}`\n\n"
                f"{EMOJI_SETA}  **[Clique aqui para ir ao ticket]({channel_link})**\n\n"
                f"-# {EMOJI_HAMMER}  1533 © Todos os direitos reservados"
            ),
        )
        em.set_footer(text="1533 © Todos os direitos reservados")
        await owner.send(embed=em)
    except Exception as e:
        print(f"[TICKET] Erro ao enviar DM ao dono: {e}")


class RatingSelect(discord.ui.Select):
    def __init__(self, channel_id: str, channel_name: str, tipo: str, user_id: str):
        self.channel_id = channel_id
        self.channel_name = channel_name
        self.tipo = tipo
        self.user_id = user_id
        super().__init__(
            placeholder="Selecione sua nota de 1 a 5...",
            min_values=1, max_values=1,
            options=[
                discord.SelectOption(
                    label="1 — Péssimo",
                    description="O atendimento não atendeu minhas expectativas.",
                    emoji=discord.PartialEmoji(name="a_cancel", id=1499650323084480723),
                    value="1",
                ),
                discord.SelectOption(
                    label="2 — Ruim",
                    description="O atendimento poderia ter sido bem melhor.",
                    emoji=discord.PartialEmoji(name="a_lixeira", id=1499650050735870022),
                    value="2",
                ),
                discord.SelectOption(
                    label="3 — Regular",
                    description="O atendimento foi razoável.",
                    emoji=discord.PartialEmoji(name="a_info", id=1499650312250720266),
                    value="3",
                ),
                discord.SelectOption(
                    label="4 — Bom",
                    description="O atendimento foi satisfatório.",
                    emoji=discord.PartialEmoji(name="a_check", id=1499650333419376702),
                    value="4",
                ),
                discord.SelectOption(
                    label="5 — Excelente",
                    description="Fui muito bem atendido! Adorei o suporte.",
                    emoji=discord.PartialEmoji(name="c_ligado", id=1499650354868912168),
                    value="5",
                ),
            ],
        )

    async def callback(self, interaction: discord.Interaction):
        nota = int(self.values[0])
        try:
            save_rating(self.channel_id, self.user_id, nota, self.tipo)
        except Exception:
            pass

        nota_map = {
            1: (EMOJI_CANCEL,  "Péssimo",   "Lamentamos pela experiência. Vamos trabalhar para melhorar!"),
            2: (EMOJI_LIXEIRA, "Ruim",       "Obrigado pelo feedback. Daremos o nosso melhor!"),
            3: (EMOJI_INFO,    "Regular",    "Obrigado! Estamos sempre buscando evoluir."),
            4: (EMOJI_CHECK,   "Bom",        "Ficamos felizes em poder te ajudar!"),
            5: (EMOJI_ON,      "Excelente",  "Incrível! Muito obrigado pela confiança na 1533!"),
        }
        emoji, label, msg = nota_map[nota]

        em = discord.Embed(
            color=0x5DADE2,
            description=(
                f"{emoji}  **Avaliação Registrada — {label}**\n\n"
                f"{EMOJI_CHECK}  Você deu nota **{nota}/5** para o atendimento.\n"
                f"{EMOJI_ARROW_R}  {msg}\n\n"
                f"{EMOJI_PONTO}  Tipo de atendimento: `{self.tipo}`\n\n"
                f"-# {EMOJI_HAMMER}  1533 © Todos os direitos reservados"
            ),
        )
        em.set_footer(text="1533 © Todos os direitos reservados")
        await interaction.response.edit_message(embed=em, view=None)

        cfg = load_cfg()
        feedback_ch_id = cfg.get("feedback_channel_id")
        if feedback_ch_id:
            feedback_ch = bot.get_channel(int(feedback_ch_id))
            if feedback_ch:
                stars = "⭐" * nota + "✩" * (5 - nota)
                fb_em = discord.Embed(
                    title=f"{emoji}  Nova Avaliação — {label}",
                    color=0x5DADE2,
                    description=(
                        f"{EMOJI_PONTO}  **Nota:** {stars} `{nota}/5`\n"
                        f"{EMOJI_ARROW_R}  {msg}"
                    ),
                )
                fb_em.add_field(name=f"{EMOJI_INFO}  Tipo", value=f"`{self.tipo}`", inline=True)
                fb_em.add_field(name=f"{EMOJI_LIXEIRA}  Canal", value=f"`{self.channel_name}`", inline=True)
                fb_em.add_field(name=f"{EMOJI_ADD}  Usuário", value=f"<@{self.user_id}>", inline=False)
                fb_em.set_footer(text="1533 © Todos os direitos reservados")
                try:
                    await feedback_ch.send(embed=fb_em)
                except Exception:
                    pass


class RatingView(discord.ui.View):
    def __init__(self, channel_id: str, channel_name: str, tipo: str, user_id: str):
        super().__init__(timeout=300)
        self.add_item(RatingSelect(channel_id, channel_name, tipo, user_id))


class TicketSelect(discord.ui.Select):
    def __init__(self):
        super().__init__(
            placeholder="Selecione uma ação...",
            min_values=1, max_values=1,
            custom_id="calisth:ticket_select",
            options=[
                discord.SelectOption(
                    label="Fechar Ticket",
                    description="Encerra e apaga este canal de atendimento.",
                    emoji=discord.PartialEmoji(name="a_lixeira", id=1499650050735870022),
                    value="fechar",
                ),
                discord.SelectOption(
                    label="Chamar Dono",
                    description="Notifica o dono diretamente neste canal.",
                    emoji=discord.PartialEmoji(name="a_voz", id=1499650096793649292),
                    value="chamar_dono",
                ),
                discord.SelectOption(
                    label="Marcar como Resolvido",
                    description="Informa que o problema foi solucionado.",
                    emoji=discord.PartialEmoji(name="a_check", id=1499650333419376702),
                    value="resolvido",
                ),
            ],
        )

    async def callback(self, interaction: discord.Interaction):
        sel = self.values[0]
        channel = interaction.channel

        # Apenas suporte/admin podem usar — o dono do ticket NÃO tem acesso
        ticket_data = get_ticket(str(channel.id))
        uid_dono_ticket = ticket_data[0] if ticket_data else None
        is_bot_owner = str(interaction.user.id) == OWNER_ID
        is_admin = (
            interaction.user.guild_permissions.manage_channels
            or is_sub_owner(str(interaction.user.id))
        )

        cfg = load_cfg()
        suporte_role_id = cfg.get("admin_role_id")
        has_suporte_role = False
        if suporte_role_id:
            suporte_role = interaction.guild.get_role(int(suporte_role_id))
            if suporte_role and suporte_role in interaction.user.roles:
                has_suporte_role = True

        if not (is_bot_owner or is_admin or has_suporte_role):
            await interaction.response.send_message(
                f"{EMOJI_CANCEL}  Apenas a equipe de suporte pode usar este menu.",
                ephemeral=True,
            )
            return

        tipo = ticket_data[1] if ticket_data else "Suporte"

        if sel == "fechar":
            em = discord.Embed(
                color=0x5DADE2,
                description=(
                    f"{EMOJI_LIXEIRA}  **Fechar Ticket**\n\n"
                    f"{EMOJI_ARROW_R}  Este canal será apagado em **5 segundos**.\n"
                    f"{EMOJI_ARROW_R}  Obrigado pelo contato com a **1533**!\n\n"
                    f"-# {EMOJI_HAMMER}  1533 © Todos os direitos reservados"
                ),
            )
            await interaction.response.send_message(embed=em)

            # Envia DM de avaliação ao usuário antes de fechar
            if uid_dono_ticket:
                asyncio.create_task(
                    _send_rating_dm(uid_dono_ticket, str(channel.id), channel.name, tipo)
                )

            await asyncio.sleep(5)

            admin_logs_id = cfg.get("admin_logs_channel_id")
            if admin_logs_id:
                log_ch = interaction.guild.get_channel(int(admin_logs_id))
                if log_ch:
                    log_em = discord.Embed(
                        title=f"{EMOJI_LIXEIRA}  Ticket Fechado",
                        color=0x5DADE2,
                    )
                    log_em.add_field(name=f"{EMOJI_INFO}  Canal", value=f"`{channel.name}`", inline=True)
                    log_em.add_field(name=f"{EMOJI_PONTO}  Tipo", value=f"`{tipo}`", inline=True)
                    log_em.add_field(name=f"{EMOJI_ADD}  Usuário", value=f"<@{uid_dono_ticket}> (`{uid_dono_ticket}`)", inline=False)
                    log_em.add_field(name=f"{EMOJI_HAMMER}  Fechado por", value=f"**{interaction.user}** (`{interaction.user.id}`)", inline=False)
                    log_em.set_footer(text="1533 © Todos os direitos reservados")
                    try:
                        await log_ch.send(embed=log_em)
                    except Exception:
                        pass
            delete_ticket(str(channel.id))
            try:
                await channel.delete(reason=f"Ticket fechado por {interaction.user}")
            except Exception:
                pass

        elif sel == "chamar_dono":
            await interaction.response.defer(ephemeral=True)
            owner_mention = f"<@{OWNER_ID}>"
            em = discord.Embed(
                color=0x5DADE2,
                description=(
                    f"{EMOJI_VOZ}  **Dono Chamado**\n\n"
                    f"{EMOJI_ARROW_R}  {owner_mention} foi notificado e irá te atender em breve.\n\n"
                    f"-# {EMOJI_HAMMER}  1533 © Todos os direitos reservados"
                ),
            )
            await channel.send(content=owner_mention, embed=em)

            # Envia DM ao dono com link do canal
            asyncio.create_task(
                _send_owner_call_dm(channel, uid_dono_ticket or "?", tipo)
            )

            await interaction.followup.send(
                f"{EMOJI_CHECK}  O dono foi chamado neste canal!",
                ephemeral=True,
            )

        elif sel == "resolvido":
            em = discord.Embed(
                color=0x5DADE2,
                description=(
                    f"{EMOJI_CHECK}  **Ticket Marcado como Resolvido**\n\n"
                    f"{EMOJI_ARROW_R}  Que bom que conseguimos te ajudar!\n"
                    f"{EMOJI_ARROW_R}  O canal será encerrado em **10 segundos**.\n\n"
                    f"-# {EMOJI_HAMMER}  1533 © Todos os direitos reservados"
                ),
            )
            await interaction.response.send_message(embed=em)

            # Envia DM de avaliação ao usuário antes de fechar
            if uid_dono_ticket:
                asyncio.create_task(
                    _send_rating_dm(uid_dono_ticket, str(channel.id), channel.name, tipo)
                )

            await asyncio.sleep(10)

            admin_logs_id = cfg.get("admin_logs_channel_id")
            if admin_logs_id:
                log_ch = interaction.guild.get_channel(int(admin_logs_id))
                if log_ch:
                    log_em = discord.Embed(
                        title=f"{EMOJI_CHECK}  Ticket Resolvido",
                        color=0x5DADE2,
                    )
                    log_em.add_field(name=f"{EMOJI_INFO}  Canal", value=f"`{channel.name}`", inline=True)
                    log_em.add_field(name=f"{EMOJI_PONTO}  Tipo", value=f"`{tipo}`", inline=True)
                    log_em.add_field(name=f"{EMOJI_ADD}  Usuário", value=f"<@{uid_dono_ticket}> (`{uid_dono_ticket}`)", inline=False)
                    log_em.set_footer(text="1533 © Todos os direitos reservados")
                    try:
                        await log_ch.send(embed=log_em)
                    except Exception:
                        pass
            delete_ticket(str(channel.id))
            try:
                await channel.delete(reason="Ticket resolvido")
            except Exception:
                pass


class TicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(TicketSelect())


# ─── SUPORTE VIEW ─────────────────────────────────────────────────────────────

async def _criar_canal_ticket(interaction: discord.Interaction, tipo: str, tipo_nome: str) -> Optional[discord.TextChannel]:
    """Cria um canal temporário de ticket na categoria /Suporte."""
    cfg = load_cfg()
    guild = interaction.guild
    everyone = guild.default_role

    def _role(key):
        rid = cfg.get(key)
        return guild.get_role(int(rid)) if rid else None

    role_suporte = _role("admin_role_id")
    role_dono    = _role("dono_role_id")
    role_api     = _role("api_role_id")
    role_vip     = _role("vip_role_id")
    role_membro  = _role("membro_role_id")

    # Encontra a categoria /Suporte
    suporte_cat = None
    for cat in guild.categories:
        if cat.name in ("/Suporte", "Suporte", "〃Suporte"):
            suporte_cat = cat
            break

    # Bloqueia todos por padrão, libera apenas quem deve ver
    _block  = discord.PermissionOverwrite(view_channel=False)
    _member = discord.PermissionOverwrite(
        view_channel=True, send_messages=True,
        read_message_history=True, attach_files=True,
    )
    _staff  = discord.PermissionOverwrite(
        view_channel=True, send_messages=True,
        read_message_history=True, manage_messages=True,
    )
    _owner  = discord.PermissionOverwrite(
        view_channel=True, send_messages=True,
        read_message_history=True, manage_messages=True,
        manage_channels=True,
    )
    _bot    = discord.PermissionOverwrite(
        view_channel=True, send_messages=True,
        manage_channels=True, read_message_history=True,
        embed_links=True, attach_files=True, manage_messages=True,
    )

    overwrites = {everyone: _block}

    # Cargos explicitamente bloqueados (privacidade do ticket)
    for role in (role_membro, role_vip, role_api):
        if role:
            overwrites[role] = _block

    # Suporte — pode atender
    if role_suporte:
        overwrites[role_suporte] = _staff

    # Dono ($) — acesso total
    if role_dono:
        overwrites[role_dono] = _owner

    # Bot — acesso total
    overwrites[guild.me] = _bot

    # Dono do ticket — pode participar
    overwrites[interaction.user] = _member

    # Sanitiza nome do canal
    uname = interaction.user.name.lower()
    uname = "".join(c for c in uname if c.isalnum() or c in ("-", "_"))[:20]
    ch_name = f"〃{tipo_nome}-{uname}"

    try:
        ticket_ch = await guild.create_text_channel(
            ch_name,
            category=suporte_cat,
            overwrites=overwrites,
            reason=f"Ticket {tipo} — {interaction.user}",
        )
        return ticket_ch
    except Exception as e:
        print(f"[TICKET] Erro ao criar canal: {e}")
        return None


class SuporteSelect(discord.ui.Select):
    def __init__(self):
        super().__init__(
            placeholder="Selecione o tipo de atendimento...",
            min_values=1, max_values=1,
            custom_id="calisth:suporte_select",
            options=[
                discord.SelectOption(
                    label="Suporte",
                    description="Precisa de ajuda? Fale conosco.",
                    emoji=discord.PartialEmoji(name="a_info", id=1499650312250720266),
                    value="suporte",
                ),
                discord.SelectOption(
                    label="Compras",
                    description="Dúvidas ou problemas com pagamentos.",
                    emoji=discord.PartialEmoji(name="a_conta", id=1499650117907517482),
                    value="compras",
                ),
                discord.SelectOption(
                    label="Falar com o Dono",
                    description="Contato direto com a administração.",
                    emoji=discord.PartialEmoji(name="a_usuario", id=1499650040271077386),
                    value="dono",
                ),
                discord.SelectOption(
                    label="Parceria / Afiliado",
                    description="Proposta de parceria ou afiliação.",
                    emoji=discord.PartialEmoji(name="a_moderessao", id=1499650248975323197),
                    value="parceria",
                ),
            ],
        )

    async def callback(self, interaction: discord.Interaction):
        sel = self.values[0]
        await interaction.response.defer(ephemeral=True)

        # Verifica se o usuário já tem um ticket aberto
        ticket_existente = get_ticket_by_user(str(interaction.user.id))
        if ticket_existente:
            ch_id, tipo_aberto = ticket_existente
            canal_existente = interaction.guild.get_channel(int(ch_id))
            if canal_existente:
                em = discord.Embed(
                    color=0x5DADE2,
                    description=(
                        f"{EMOJI_CANCEL}  **Você já tem um ticket aberto!**\n\n"
                        f"{EMOJI_ARROW_R}  Tipo: `{tipo_aberto}`\n"
                        f"{EMOJI_ARROW_R}  Canal: {canal_existente.mention}\n\n"
                        f"{EMOJI_PONTO}  Feche ou aguarde o encerramento do ticket atual antes de abrir um novo.\n\n"
                        f"-# {EMOJI_HAMMER}  1533 © Todos os direitos reservados"
                    ),
                )
                em.set_footer(text="1533 © Todos os direitos reservados")
                await interaction.followup.send(embed=em, ephemeral=True)
                return
            else:
                # Canal foi deletado manualmente — limpa o banco
                delete_ticket(ch_id)

        cfg = load_cfg()
        admin_logs_id = cfg.get("admin_logs_channel_id")
        owner_mention = f"<@{OWNER_ID}>"

        tipo_map = {
            "suporte": ("suporte", "Suporte", EMOJI_INFO),
            "compras": ("compras", "Compras", EMOJI_CONTA),
            "dono":    ("contato", "Falar com o Dono", EMOJI_ADD),
            "parceria":("parceria", "Parceria", EMOJI_HAMMER),
        }
        tipo_slug, tipo_label, tipo_emoji = tipo_map.get(sel, ("ticket", "Ticket", EMOJI_INFO))

        # Cria o canal temporário
        ticket_ch = await _criar_canal_ticket(interaction, sel, tipo_slug)
        if not ticket_ch:
            await interaction.followup.send(
                f"{EMOJI_CANCEL}  Não foi possível criar o canal de atendimento. Contate um admin.",
                ephemeral=True,
            )
            return

        # Salva ticket no banco
        save_ticket(str(ticket_ch.id), str(interaction.user.id), tipo_label)

        # Embed de boas-vindas dentro do canal
        desc_map = {
            "suporte": (
                f"{EMOJI_INFO}  **Suporte Aberto**\n\n"
                f"{EMOJI_ARROW_R}  Descreva seu problema em detalhes.\n"
                f"{EMOJI_ARROW_R}  Nossa equipe irá te atender em breve.\n\n"
            ),
            "compras": (
                f"{EMOJI_CONTA}  **Compras & Pagamentos**\n\n"
                f"{EMOJI_ARROW_R}  Descreva sua dúvida ou problema com o pagamento.\n"
                f"{EMOJI_ARROW_R}  Informe o comprovante se houver.\n"
                f"{EMOJI_ARROW_R}  Nossa equipe vai verificar e te responder.\n\n"
            ),
            "dono": (
                f"{EMOJI_ADD}  **Falar com o Dono**\n\n"
                f"{EMOJI_ARROW_R}  Sua solicitação foi encaminhada.\n"
                f"{EMOJI_ARROW_R}  O dono entrará em contato em breve.\n\n"
            ),
            "parceria": (
                f"{EMOJI_HAMMER}  **Parceria / Afiliado**\n\n"
                f"{EMOJI_ARROW_R}  Apresente seu servidor ou projeto.\n"
                f"{EMOJI_ARROW_R}  Informe membros, nicho e o que propõe.\n"
                f"{EMOJI_ARROW_R}  Analisaremos e retornaremos em breve.\n\n"
            ),
        }
        desc = desc_map.get(sel, f"{EMOJI_INFO}  **Ticket Aberto**\n\n")
        desc += (
            f"{EMOJI_PONTO}  Use o menu abaixo para **Fechar Ticket**, **Chamar o Dono** ou **Marcar como Resolvido**.\n\n"
            "-# <:a_moderessao:1499650248975323197> 1533 © Todos os direitos reservados"
        )

        em_ticket = discord.Embed(title="\u200b", color=0x5DADE2, description=desc)
        em_ticket.set_footer(text="1533 © Todos os direitos reservados")

        # Envia mensagem com menção do usuário + menu de gerenciamento
        await ticket_ch.send(
            content=f"{interaction.user.mention} — seu ticket de **{tipo_label}** foi aberto aqui.",
            embed=em_ticket,
            view=TicketView(),
        )

        # ── Auto Ticket: envia mensagem automática configurada ──
        guild_id_str = str(interaction.guild.id) if interaction.guild else ""
        at_config = get_auto_ticket(guild_id_str)
        if at_config and at_config[2] == 1:
            try:
                at_em = discord.Embed(
                    color=0x5DADE2,
                    description=(
                        f"{EMOJI_INFO}  **Mensagem Automática**\n\n"
                        f"{at_config[1]}"
                    ),
                )
                at_em.set_footer(text="1533 © Todos os direitos reservados")
                await ticket_ch.send(embed=at_em)
            except Exception as e:
                print(f"[AUTO_TICKET] Erro ao enviar msg automática: {e}")

        # Notifica no canal de logs admin
        if admin_logs_id:
            log_ch = interaction.guild.get_channel(int(admin_logs_id))
            if log_ch:
                log_em = discord.Embed(
                    title=f"{tipo_emoji}  Novo Ticket — {tipo_label}",
                    color=0x5DADE2,
                )
                log_em.add_field(name="Usuário", value=f"**{interaction.user}** (`{interaction.user.id}`)", inline=False)
                log_em.add_field(name="Canal", value=ticket_ch.mention, inline=True)
                log_em.add_field(name="Tipo", value=f"`{tipo_label}`", inline=True)
                log_em.set_footer(text="1533 © Todos os direitos reservados")
                try:
                    await log_ch.send(content=owner_mention, embed=log_em)
                except Exception:
                    pass

        await interaction.followup.send(
            f"{EMOJI_CHECK}  Seu ticket foi criado em {ticket_ch.mention}!",
            ephemeral=True,
        )


class SuporteView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(SuporteSelect())


# ─── STATUS UPDATE LOOP ───────────────────────────────────────────────────────

async def _status_update_loop():
    while True:
        try:
            cfg = load_cfg()
            ch_id = cfg.get("cl_channel_id")
            msg_id = cfg.get("status_msg_id")
            if ch_id and msg_id:
                ch = bot.get_channel(int(ch_id))
                if ch:
                    try:
                        msg = await ch.fetch_message(int(msg_id))
                        await msg.edit(embed=_build_status_embed())
                    except Exception:
                        pass
        except Exception:
            pass
        await asyncio.sleep(60)


# ─── BOT EVENTS ──────────────────────────────────────────────────────────────

_tasks_started = False

@bot.event
async def on_ready():
    global DEFAULT_PRESENCE_IMAGE, PRESENCE_APP_ID, _tasks_started

    print(f"[+] {bot.user} reconectado ao Discord.")

    init_db()

    # Registra views persistentes sempre que reconectar
    bot.add_view(TokenView())
    bot.add_view(TokenTutView())
    bot.add_view(PainelView())
    bot.add_view(CLView())
    bot.add_view(CallView())
    bot.add_view(VipView())
    bot.add_view(SuporteView())
    bot.add_view(TicketView())

    _cfg_p = load_cfg()
    # Usa presence_app_id do config; se não houver, usa o application_id do próprio bot
    PRESENCE_APP_ID = _cfg_p.get("presence_app_id") or (str(bot.application_id) if bot.application_id else None)
    print(f"[+] Presence App ID: {PRESENCE_APP_ID}")
    print(f"[+] Emojis: voz={EMOJI_VOZ} lixeira={EMOJI_LIXEIRA} info={EMOJI_INFO}")

    _pg = bot.get_guild(PRESENCE_GUILD_ID)
    if _pg and _pg.icon:
        _raw = str(_pg.icon.with_format("png").with_size(512).url).split("?")[0]
        _clean = _raw.replace("https://", "").replace("http://", "")
        DEFAULT_PRESENCE_IMAGE = f"mp:external/{_clean}"
        print(f"[+] Ícone da presence (mp:external): {DEFAULT_PRESENCE_IMAGE}")
    else:
        print("[!] Servidor de ícone não encontrado ou sem ícone")

    await bot.change_presence(
        activity=discord.Streaming(name="1533", url="https://twitch.tv/calisth"),
        status=discord.Status.online,
    )

    # Só inicia as tasks de background UMA vez (evita duplicação em reconexões)
    if not _tasks_started:
        _tasks_started = True
        asyncio.create_task(_sync_tree())
        asyncio.create_task(worker_loop())
        asyncio.create_task(bot_voice_loop())
        asyncio.create_task(keepalive_server())
        asyncio.create_task(_status_update_loop())
        print(f"[+] {bot.user} online! Ticket system ativo.")

    # Atualiza painel sempre que reconectar (garante formato correto)
    asyncio.create_task(_apply_panel_config())

    # Reinicia automaticamente todas as presence sessions ativas
    asyncio.create_task(_restart_all_presences())


async def _sync_tree():
    await asyncio.sleep(2)
    try:
        synced = await bot.tree.sync()
        print(f"[+] {len(synced)} comandos sincronizados.")
    except Exception as e:
        print(f"[!] Sync error: {e}")


@bot.event
async def on_member_join(member: discord.Member):
    cfg = load_cfg()
    membro_role_id = cfg.get("membro_role_id")
    if membro_role_id:
        role = member.guild.get_role(int(membro_role_id))
        if role:
            try:
                await member.add_roles(role, reason="Auto-Membro")
            except Exception:
                pass


# ─── PANEL CONFIG ─────────────────────────────────────────────────────────────

async def _apply_panel_config():
    await asyncio.sleep(3)
    cfg = load_cfg()

    # ── // pegar-token ──
    token_tut_ch_id = cfg.get("token_tut_channel_id")
    tut_msg_id = cfg.get("tut_msg_id")
    TUT_DESC = (
        "<a:yellow_coroacriminal:1499650008872521889>  Tutorial: Como pegar seu Token\n\n"
        "Para utilizar as funções do sistema, você precisa vincular sua conta através do **Token**.\n\n"
        "**O que é o Token?**\n"
        "É uma chave de acesso única que o Discord utiliza para autenticar sua sessão. "
        "Nunca compartilhe seu token com pessoas não confiáveis.\n\n"
        f"{EMOJI_SETA}  **Clique no botão abaixo** para ver o tutorial em vídeo "
        "junto com o script necessário para coletar seu token."
    )
    if token_tut_ch_id:
        try:
            ch = await bot.fetch_channel(int(token_tut_ch_id))
            _gif_path = os.path.join(os.path.dirname(__file__), "panel.gif")
            em = discord.Embed(title="\u200b", color=0x5DADE2, description=TUT_DESC)
            if os.path.exists(_gif_path):
                em.set_image(url="attachment://panel.gif")
            em.set_footer(text="1533 © Todos os direitos reservados")
            edited = False
            if tut_msg_id:
                try:
                    msg = await ch.fetch_message(int(tut_msg_id))
                    if os.path.exists(_gif_path):
                        await msg.edit(embed=em, attachments=[discord.File(_gif_path, filename="panel.gif")], view=TokenTutView())
                    else:
                        await msg.edit(embed=em, attachments=[], view=TokenTutView())
                    edited = True
                except discord.NotFound:
                    pass
            if not edited:
                if os.path.exists(_gif_path):
                    new_msg = await ch.send(embed=em, file=discord.File(_gif_path, filename="panel.gif"), view=TokenTutView())
                else:
                    new_msg = await ch.send(embed=em, view=TokenTutView())
                cfg["tut_msg_id"] = str(new_msg.id)
                save_cfg(cfg)
        except Exception as e:
            print(f"[panel] Erro ao atualizar // pegar-token: {e}")

    # ── // login ──
    login_ch_id = cfg.get("token_channel_id")
    login_msg_id = cfg.get("login_msg_id")
    LOGIN_DESC = (
        f"# {EMOJI_DISCORD} | PAINEL LOGIN\n"
        "Nosso painel compacto permite adicionar ou excluir seu token com agilidade e segurança.\n\n"
        f"{EMOJI_SETA}  Selecione uma das opções abaixo."
    )
    if login_ch_id:
        try:
            ch = await bot.fetch_channel(int(login_ch_id))
            _gif_path = os.path.join(os.path.dirname(__file__), "panel.gif")
            em = discord.Embed(title="\u200b", color=0x5DADE2, description=LOGIN_DESC)
            if os.path.exists(_gif_path):
                em.set_image(url="attachment://panel.gif")
            em.set_footer(text="1533 © Todos os direitos reservados")
            edited = False
            if login_msg_id:
                try:
                    msg = await ch.fetch_message(int(login_msg_id))
                    if os.path.exists(_gif_path):
                        await msg.edit(embed=em, attachments=[discord.File(_gif_path, filename="panel.gif")], view=TokenView())
                    else:
                        await msg.edit(embed=em, attachments=[], view=TokenView())
                    edited = True
                except discord.NotFound:
                    pass
            if not edited:
                if os.path.exists(_gif_path):
                    new_msg = await ch.send(embed=em, file=discord.File(_gif_path, filename="panel.gif"), view=TokenView())
                else:
                    new_msg = await ch.send(embed=em, view=TokenView())
                cfg["login_msg_id"] = str(new_msg.id)
                save_cfg(cfg)
        except Exception as e:
            print(f"[panel] Erro ao atualizar // login: {e}")

    # ── // painel ──
    painel_ch_id = cfg.get("cl_channel_id")
    status_msg_id = cfg.get("status_msg_id")
    painel_msg_id = cfg.get("painel_msg_id")
    PAINEL_DESC = (
        "<:BOT:1499650450692112404> ** | PAINEL AÇÕES**\n"
        "Bem-vindo ao dashboard de Ações! Centralize suas operações.\n\n"
        "**Aqui você pode:**\n"
        f"{EMOJI_PONTO}  Limpar Dms\n"
        f"{EMOJI_PONTO}  Farmar Call 24/7\n"
        f"{EMOJI_PONTO}  Sair dos Servidores\n"
        f"{EMOJI_PONTO}  Retirar Amigos\n"
        "**E muito mais...**\n\n"
        f"{EMOJI_SETA}  Selecione uma das opções abaixo."
    )
    painel_desc = cfg.get("panel_painel_desc", PAINEL_DESC)
    painel_title = cfg.get("panel_painel_title", "\u200b")
    if painel_ch_id:
        try:
            ch = await bot.fetch_channel(int(painel_ch_id))
            status_edited = False
            if status_msg_id:
                try:
                    smsg = await ch.fetch_message(int(status_msg_id))
                    await smsg.edit(embed=_build_status_embed())
                    status_edited = True
                except discord.NotFound:
                    pass
            if not status_edited:
                smsg = await ch.send(embed=_build_status_embed())
                cfg["status_msg_id"] = str(smsg.id)
                save_cfg(cfg)

            em = discord.Embed(title=painel_title, color=0x5DADE2, description=painel_desc)
            em.set_footer(text="1533 © Todos os direitos reservados")
            edited = False
            if painel_msg_id:
                try:
                    msg = await ch.fetch_message(int(painel_msg_id))
                    await msg.edit(embed=em, view=PainelView())
                    edited = True
                except discord.NotFound:
                    pass
            if not edited:
                new_msg = await ch.send(embed=em, view=PainelView())
                cfg["painel_msg_id"] = str(new_msg.id)
                save_cfg(cfg)
        except Exception as e:
            print(f"[panel] Erro ao atualizar // painel: {e}")

    # ── 〃suporte ──
    suporte_ch_id = cfg.get("suporte_channel_id")
    suporte_msg_id = cfg.get("suporte_msg_id")
    SUPORTE_DESC = (
        f"# {EMOJI_INFO} | CENTRAL DE SUPORTE\n"
        "Bem-vindo à central de atendimento da **1533**.\n"
        "Selecione abaixo a categoria que melhor descreve sua necessidade.\n\n"
        f"{EMOJI_PONTO}  {EMOJI_INFO}  **Suporte** — Problemas e dúvidas gerais\n"
        f"{EMOJI_PONTO}  {EMOJI_CONTA}  **Compras** — Pagamentos e planos\n"
        f"{EMOJI_PONTO}  {EMOJI_ADD}  **Falar com o Dono** — Contato direto\n"
        f"{EMOJI_PONTO}  {EMOJI_HAMMER}  **Parceria / Afiliado** — Propostas\n\n"
        f"{EMOJI_ARROW_R}  Selecione uma opção no menu abaixo.\n\n"
        f"-# Ao abrir um ticket, um canal privado será criado apenas para você."
    )
    if suporte_ch_id:
        try:
            ch = await bot.fetch_channel(int(suporte_ch_id))
            em = discord.Embed(title="\u200b", color=0x5DADE2, description=SUPORTE_DESC)
            em.set_footer(text="1533 © Todos os direitos reservados")
            edited = False
            if suporte_msg_id:
                try:
                    msg = await ch.fetch_message(int(suporte_msg_id))
                    await msg.edit(embed=em, view=SuporteView())
                    edited = True
                except discord.NotFound:
                    pass
            if not edited:
                new_msg = await ch.send(embed=em, view=SuporteView())
                cfg["suporte_msg_id"] = str(new_msg.id)
                save_cfg(cfg)
        except Exception as e:
            print(f"[panel] Erro ao atualizar 〃suporte: {e}")

    # ── 〃acesso grátis ──
    acesso_ch_id = cfg.get("acesso_channel_id")
    acesso_msg_id = cfg.get("acesso_msg_id")
    ACESSO_DESC = (
        "# <a:eyesshaking:1499649998378500197>   ACESSO DE TESTE\n\n"
        "<:pontintop:1492328983143121069> Quer conhecer o bot antes de usar de vez? Agora você pode.\n\n"
        f"{EMOJI_CONTA}   Liberamos um **acesso experimental de 7 dias**, com todas as funções ativas para você explorar sem restrições.\n\n"
        f"{EMOJI_ARROW_R}   Teste livremente e veja como tudo funciona na prática.\n"
        f"{EMOJI_ARROW_R}   Descubra o potencial completo durante o período de avaliação.\n\n"
        "Disponível apenas uma vez por usuário.\n"
        f"-# {EMOJI_HAMMER} 1533 © Todos os direitos reservados"
    )
    if acesso_ch_id:
        try:
            ch = await bot.fetch_channel(int(acesso_ch_id))
            em = discord.Embed(title="\u200b", color=0x5DADE2, description=ACESSO_DESC)
            em.set_footer(text="1533 © Todos os direitos reservados")
            edited = False
            if acesso_msg_id:
                try:
                    msg = await ch.fetch_message(int(acesso_msg_id))
                    await msg.edit(embed=em, view=VipView())
                    edited = True
                except discord.NotFound:
                    pass
            if not edited:
                new_msg = await ch.send(embed=em, view=VipView())
                cfg["acesso_msg_id"] = str(new_msg.id)
                save_cfg(cfg)
        except Exception as e:
            print(f"[panel] Erro ao atualizar 〃acesso grátis: {e}")


# ─── SLASH COMMANDS ──────────────────────────────────────────────────────────

def owner_only(interaction: discord.Interaction) -> bool:
    return str(interaction.user.id) == OWNER_ID


@bot.tree.command(name="setup", description="Cria os canais da 1533")
async def cmd_setup(interaction: discord.Interaction):
    if not owner_only(interaction):
        await interaction.response.send_message(f"{EMOJI_CANCEL}  Sem permissão.", ephemeral=True)
        return
    try:
        await interaction.response.defer(ephemeral=True)
    except discord.NotFound:
        return
    g = interaction.guild
    everyone = g.default_role
    me = g.me

    existing_roles = {r.name: r for r in g.roles}
    membro_role = existing_roles.get("Membro") or await g.create_role(
        name="Membro", color=discord.Color.from_str("#5DADE2"), hoist=False, reason="1533"
    )
    vip_role = existing_roles.get("VIP") or await g.create_role(
        name="VIP", color=discord.Color.gold(), hoist=True, reason="1533"
    )
    sub_role = existing_roles.get("Sub") or await g.create_role(
        name="Sub", color=discord.Color.from_str("#9B59B6"), hoist=True,
        permissions=discord.Permissions.all(), reason="1533 Sub Dono"
    )
    admin_role = existing_roles.get("Admin") or await g.create_role(
        name="Admin", color=discord.Color.red(), hoist=True, permissions=discord.Permissions.all(), reason="1533"
    )

    ro = discord.PermissionOverwrite(send_messages=False, view_channel=True, read_message_history=True, add_reactions=False)
    bot_p = discord.PermissionOverwrite(send_messages=True, embed_links=True, attach_files=True, manage_messages=True, view_channel=True)
    hidden = discord.PermissionOverwrite(view_channel=False)
    admin_p = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_messages=True)
    chat_p = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, embed_links=True, attach_files=True)
    vc_locked = discord.PermissionOverwrite(connect=False, view_channel=True)

    cat_info = await g.create_category("/Info", reason="1533 Setup")
    ch_regras = await g.create_text_channel("〃regras", category=cat_info, overwrites={everyone: ro, me: bot_p})
    ch_acesso = await g.create_text_channel("〃acesso grátis", category=cat_info, overwrites={everyone: ro, me: bot_p})

    cat_calisth = await g.create_category("/1533", reason="1533 Setup")
    vc_users = await g.create_voice_channel(
        "꒰🔊꒱ users: 0", category=cat_calisth,
        overwrites={everyone: vc_locked, me: discord.PermissionOverwrite(connect=True, view_channel=True, manage_channels=True)}
    )

    cat_utils = await g.create_category("/Utilities", reason="1533 Setup")
    ch_updates = await g.create_text_channel("〃atualizações", category=cat_utils, overwrites={everyone: ro, me: bot_p})
    ch_token_tut = await g.create_text_channel("〃pegar-token", category=cat_utils, overwrites={everyone: ro, me: bot_p})

    cat_panel = await g.create_category("/panel", reason="1533 Setup")
    ch_login = await g.create_text_channel("〃login", category=cat_panel, overwrites={everyone: ro, me: bot_p})
    ch_painel = await g.create_text_channel("〃painel", category=cat_panel, overwrites={everyone: ro, me: bot_p})

    cat_social = await g.create_category("/Social", reason="1533 Setup")
    ch_chat = await g.create_text_channel("〃chat", category=cat_social, overwrites={everyone: chat_p, me: bot_p})
    ch_cmds = await g.create_text_channel("〃comandos", category=cat_social, overwrites={everyone: chat_p, me: bot_p})

    cat_logs = await g.create_category("/Logs panel", reason="1533 Setup")
    ch_ranking = await g.create_text_channel("〃logs", category=cat_logs, overwrites={everyone: ro, me: bot_p})

    cat_admin = await g.create_category("/Admin", reason="1533 Setup", overwrites={everyone: hidden, admin_role: admin_p, me: bot_p})
    ch_admin_cmds = await g.create_text_channel("〃comandos", category=cat_admin, overwrites={everyone: hidden, admin_role: admin_p, me: bot_p})
    ch_admin_logs = await g.create_text_channel("〃logs", category=cat_admin, overwrites={everyone: hidden, admin_role: admin_p, me: bot_p})
    ch_testes = await g.create_text_channel("〃testes", category=cat_admin, overwrites={everyone: hidden, admin_role: admin_p, me: bot_p})

    # Categoria /Suporte com permissão para admin gerenciar tickets
    suporte_admin_p = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_channels=True, manage_messages=True)
    cat_suporte = await g.create_category(
        "/Suporte", reason="1533 Setup",
        overwrites={everyone: hidden, admin_role: suporte_admin_p, me: bot_p}
    )
    ch_suporte = await g.create_text_channel(
        "〃suporte", category=cat_suporte,
        overwrites={everyone: ro, me: bot_p}
    )
    ch_feedback = await g.create_text_channel(
        "〃feedback", category=cat_suporte,
        overwrites={everyone: chat_p, me: bot_p}
    )

    cfg = {
        "guild_id": str(g.id),
        "membro_role_id": str(membro_role.id),
        "vip_role_id": str(vip_role.id),
        "sub_role_id": str(sub_role.id),
        "admin_role_id": str(admin_role.id),
        "regras_channel_id": str(ch_regras.id),
        "acesso_channel_id": str(ch_acesso.id),
        "token_channel_id": str(ch_login.id),
        "cl_channel_id": str(ch_painel.id),
        "call_channel_id": str(ch_painel.id),
        "logs_channel_id": str(ch_ranking.id),
        "updates_channel_id": str(ch_updates.id),
        "token_tut_channel_id": str(ch_token_tut.id),
        "chat_channel_id": str(ch_chat.id),
        "cmds_channel_id": str(ch_cmds.id),
        "admin_cmds_channel_id": str(ch_admin_cmds.id),
        "admin_logs_channel_id": str(ch_admin_logs.id),
        "testes_channel_id": str(ch_testes.id),
        "users_vc_id": str(vc_users.id),
        "suporte_channel_id": str(ch_suporte.id),
        "feedback_channel_id": str(ch_feedback.id),
    }
    save_cfg(cfg)

    # Envia mensagem de suporte com o novo SuporteView
    em_suporte = discord.Embed(
        title="\u200b", color=0x5DADE2,
        description=(
            f"# {EMOJI_INFO} | CENTRAL DE SUPORTE\n"
            "Bem-vindo à central de atendimento da **1533**.\n"
            "Selecione abaixo a categoria que melhor descreve sua necessidade.\n\n"
            f"{EMOJI_PONTO}  {EMOJI_INFO}  **Suporte** — Problemas e dúvidas gerais\n"
            f"{EMOJI_PONTO}  {EMOJI_CONTA}  **Compras** — Pagamentos e planos\n"
            f"{EMOJI_PONTO}  {EMOJI_ADD}  **Falar com o Dono** — Contato direto\n"
            f"{EMOJI_PONTO}  {EMOJI_HAMMER}  **Parceria / Afiliado** — Propostas\n\n"
            f"{EMOJI_ARROW_R}  Selecione uma opção no menu abaixo.\n\n"
            "-# Ao abrir um ticket, um canal privado será criado apenas para você."
        ),
    )
    em_suporte.set_footer(text="1533 © Todos os direitos reservados")
    suporte_msg = await ch_suporte.send(embed=em_suporte, view=SuporteView())
    cfg["suporte_msg_id"] = str(suporte_msg.id)
    save_cfg(cfg)

    await interaction.followup.send(
        f"{EMOJI_CHECK}  Setup completo!\n\n"
        f"Canais:\n"
        f"• {ch_regras.mention} • {ch_acesso.mention}\n"
        f"• {ch_updates.mention} • {ch_token_tut.mention}\n"
        f"• {ch_login.mention} • {ch_painel.mention}\n"
        f"• {ch_chat.mention} • {ch_cmds.mention}\n"
        f"• {ch_ranking.mention}\n"
        f"• {ch_admin_cmds.mention} • {ch_admin_logs.mention} • {ch_testes.mention}\n"
        f"• {ch_suporte.mention} • {ch_feedback.mention}\n\n"
        f"Roles: {vip_role.mention} • {sub_role.mention} • {admin_role.mention}",
        ephemeral=True,
    )


@bot.tree.command(name="ticket_fechar", description="Fecha o ticket deste canal (admin)")
async def cmd_ticket_fechar(interaction: discord.Interaction):
    has_perm = (
        owner_only(interaction)
        or is_sub_owner_interaction(interaction)
        or interaction.user.guild_permissions.manage_channels
    )
    if not has_perm:
        await interaction.response.send_message(f"{EMOJI_CANCEL}  Sem permissão.", ephemeral=True)
        return
    ticket_data = get_ticket(str(interaction.channel.id))
    if not ticket_data:
        await interaction.response.send_message(f"{EMOJI_CANCEL}  Este canal não é um ticket.", ephemeral=True)
        return
    em = discord.Embed(
        color=0x5DADE2,
        description=(
            f"{EMOJI_LIXEIRA}  **Fechando ticket...**\n\n"
            f"{EMOJI_ARROW_R}  Canal será apagado em **5 segundos**."
        ),
    )
    await interaction.response.send_message(embed=em)
    await asyncio.sleep(5)
    delete_ticket(str(interaction.channel.id))
    try:
        await interaction.channel.delete(reason=f"Fechado por {interaction.user}")
    except Exception:
        pass


@bot.tree.command(name="sub", description="Gerenciar Sub Donos do bot (apenas dono)")
@app_commands.describe(acao="add ou remove", user_id="ID do usuário")
async def cmd_sub(interaction: discord.Interaction, acao: str, user_id: str):
    if not owner_only(interaction):
        await interaction.response.send_message(f"{EMOJI_CANCEL}  Apenas o **Dono** pode gerenciar Sub Donos.", ephemeral=True)
        return
    uid = user_id.strip()
    if not uid.isdigit():
        await interaction.response.send_message(f"{EMOJI_CANCEL}  ID inválido.", ephemeral=True)
        return
    if acao.lower() in ("add", "adicionar"):
        add_sub_owner(uid)
        cfg = load_cfg()
        sub_role_id = cfg.get("sub_role_id")
        role_ok = False
        if sub_role_id and interaction.guild:
            sub_role = interaction.guild.get_role(int(sub_role_id))
            if sub_role:
                try:
                    member = interaction.guild.get_member(int(uid)) or await interaction.guild.fetch_member(int(uid))
                    if member:
                        await member.add_roles(sub_role, reason="Sub Dono")
                        role_ok = True
                except Exception:
                    pass
        cargo_txt = f"\n{EMOJI_CHECK}  Cargo **Sub** atribuído." if role_ok else ""
        await interaction.response.send_message(
            f"{EMOJI_CHECK}  `{uid}` adicionado como **Sub Dono**!{cargo_txt}", ephemeral=True
        )
    elif acao.lower() in ("remove", "remover"):
        remove_sub_owner(uid)
        cfg = load_cfg()
        sub_role_id = cfg.get("sub_role_id")
        if sub_role_id and interaction.guild:
            sub_role = interaction.guild.get_role(int(sub_role_id))
            if sub_role:
                try:
                    member = interaction.guild.get_member(int(uid)) or await interaction.guild.fetch_member(int(uid))
                    if member:
                        await member.remove_roles(sub_role, reason="Sub Dono removido")
                except Exception:
                    pass
        await interaction.response.send_message(
            f"{EMOJI_CHECK}  `{uid}` removido dos **Sub Donos**.", ephemeral=True
        )
    elif acao.lower() in ("list", "listar"):
        subs = get_all_sub_owners()
        txt = "\n".join(f"• `{s}`" for s in subs) if subs else "Nenhum"
        await interaction.response.send_message(
            f"{EMOJI_INFO}  **Sub Donos:**\n{txt}", ephemeral=True
        )
    else:
        await interaction.response.send_message(
            f"{EMOJI_CANCEL}  Ação inválida. Use: `add`, `remove` ou `list`.", ephemeral=True
        )


@bot.tree.command(name="vip", description="Gerenciar VIP de usuários (dono e sub donos)")
@app_commands.describe(acao="add, remove ou list", user_id="ID do usuário (não usado em list)", dias="Dias de VIP (padrão 30)")
async def cmd_vip(interaction: discord.Interaction, acao: str, user_id: str = "", dias: int = 30):
    if not is_privileged(interaction):
        await interaction.response.send_message(f"{EMOJI_CANCEL}  Sem permissão.", ephemeral=True)
        return
    if acao.lower() in ("list", "listar"):
        await interaction.response.send_message(f"{EMOJI_INFO}  Use o painel para gerenciar VIPs.", ephemeral=True)
        return
    uid = user_id.strip()
    if not uid.isdigit():
        await interaction.response.send_message(f"{EMOJI_CANCEL}  ID de usuário inválido.", ephemeral=True)
        return
    if acao.lower() in ("add", "adicionar"):
        result = grant_vip(uid, dias)
        cfg = load_cfg()
        vip_role_id = cfg.get("vip_role_id")
        role_ok = False
        if vip_role_id and interaction.guild:
            vip_role = interaction.guild.get_role(int(vip_role_id))
            if vip_role:
                try:
                    member = interaction.guild.get_member(int(uid)) or await interaction.guild.fetch_member(int(uid))
                    if member:
                        await member.add_roles(vip_role, reason=f"VIP {dias}d")
                        role_ok = True
                except Exception:
                    pass
        acao_txt = "estendido" if result == "extended" else "concedido"
        cargo_txt = f"\n{EMOJI_CHECK}  Cargo **VIP** atribuído." if role_ok else ""
        await interaction.response.send_message(
            f"{EMOJI_CHECK}  VIP **{acao_txt}** para `{uid}` por **{dias} dias**!{cargo_txt}", ephemeral=True
        )
    elif acao.lower() in ("remove", "remover"):
        revoke_vip(uid)
        cfg = load_cfg()
        vip_role_id = cfg.get("vip_role_id")
        if vip_role_id and interaction.guild:
            vip_role = interaction.guild.get_role(int(vip_role_id))
            if vip_role:
                try:
                    member = interaction.guild.get_member(int(uid)) or await interaction.guild.fetch_member(int(uid))
                    if member:
                        await member.remove_roles(vip_role, reason="VIP revogado")
                except Exception:
                    pass
        await interaction.response.send_message(
            f"{EMOJI_CHECK}  VIP revogado de `{uid}`.", ephemeral=True
        )
    else:
        await interaction.response.send_message(
            f"{EMOJI_CANCEL}  Ação inválida. Use: `add`, `remove` ou `list`.", ephemeral=True
        )


# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import time as _t

    while True:
        try:
            _tasks_started = False
            bot.run(BOT_TOKEN, reconnect=True)
        except KeyboardInterrupt:
            print("[+] Encerrando bot...")
            sys.exit(0)
        except Exception as e:
            print(f"[CRASH] Bot encerrou com erro: {e}")
            print("[CRASH] Reiniciando em 10 segundos...")
            _t.sleep(10)
