import json
import secrets
import sqlite3
import time
from contextlib import contextmanager

from backend.config import CHAT_DB_PATH, SESSION_SECONDS, MAX_CHAT_MESSAGES


class SesionExpirada(Exception):
    pass


class ConversacionOcupada(Exception):
    pass


@contextmanager
def conectar():
    conexion = sqlite3.connect(CHAT_DB_PATH, timeout=10)
    conexion.row_factory = sqlite3.Row
    try:
        yield conexion
    finally:
        conexion.close()


def inicializar_historial():
    CHAT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with conectar() as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("""
            CREATE TABLE IF NOT EXISTS conversaciones (
                sid TEXT PRIMARY KEY,
                cliente_id INTEGER NOT NULL,
                expira REAL NOT NULL,
                mensajes TEXT NOT NULL,
                meta TEXT NOT NULL,
                lease TEXT,
                ocupado_hasta REAL NOT NULL DEFAULT 0
            )
        """)
        con.execute("DELETE FROM conversaciones WHERE expira <= ?", (time.time(),))
        con.commit()


def crear_conversacion(cliente_id):
    sid = secrets.token_urlsafe(32)
    with conectar() as con:
        con.execute("DELETE FROM conversaciones WHERE expira <= ?", (time.time(),))
        con.execute(
            "INSERT INTO conversaciones (sid, cliente_id, expira, mensajes, meta) "
            "VALUES (?, ?, ?, '[]', '{}')",
            (sid, cliente_id, time.time() + SESSION_SECONDS)
        )
        con.commit()
    return sid


def borrar_conversacion(sid):
    if isinstance(sid, str):
        with conectar() as con:
            con.execute("DELETE FROM conversaciones WHERE sid = ?", (sid,))
            con.commit()


def leer_conversacion(sid, cliente_id):
    with conectar() as con:
        con.execute("DELETE FROM conversaciones WHERE expira <= ?", (time.time(),))
        con.commit()
        row = con.execute(
            "SELECT * FROM conversaciones WHERE sid = ? AND cliente_id = ? "
            "AND expira > ?", (sid, cliente_id, time.time())
        ).fetchone()
    if row is None:
        raise SesionExpirada()
    return {"mensajes": json.loads(row["mensajes"]), "meta": json.loads(row["meta"])}


def reservar_conversacion(sid, cliente_id):
    token = secrets.token_hex(16)
    ahora = time.time()
    with conectar() as con:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute(
            "SELECT * FROM conversaciones WHERE sid = ? AND cliente_id = ? "
            "AND expira > ?", (sid, cliente_id, ahora)
        ).fetchone()
        if row is None:
            raise SesionExpirada()
        if row["ocupado_hasta"] > ahora:
            raise ConversacionOcupada()
        con.execute(
            "UPDATE conversaciones SET lease = ?, ocupado_hasta = ? WHERE sid = ?",
            (token, ahora + 180, sid)
        )
        con.commit()
    return token, {
        "mensajes": json.loads(row["mensajes"]), "meta": json.loads(row["meta"])
    }


def guardar_turno(sid, cliente_id, token, mensajes, meta):
    with conectar() as con:
        result = con.execute(
            "UPDATE conversaciones SET mensajes = ?, meta = ?, lease = NULL, "
            "ocupado_hasta = 0 WHERE sid = ? AND cliente_id = ? AND lease = ? "
            "AND expira > ?",
            (
                json.dumps(mensajes[-MAX_CHAT_MESSAGES:], ensure_ascii=False, allow_nan=False),
                json.dumps(meta, ensure_ascii=False, allow_nan=False),
                sid, cliente_id, token, time.time()
            )
        )
        con.commit()
        if result.rowcount != 1:
            raise SesionExpirada()


def liberar_conversacion(sid, token):
    with conectar() as con:
        con.execute(
            "UPDATE conversaciones SET lease = NULL, ocupado_hasta = 0 "
            "WHERE sid = ? AND lease = ?", (sid, token)
        )
        con.commit()

