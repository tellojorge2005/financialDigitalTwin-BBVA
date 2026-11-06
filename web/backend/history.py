import json
import secrets
import sqlite3
import time
from contextlib import contextmanager

from web.backend.config import CHAT_DB_PATH, SESSION_SECONDS, MAX_CHAT_MESSAGES
from web.backend.turns import LIMITE_RESERVA


class SesionExpirada(Exception):
    pass


class ConversacionOcupada(Exception):
    pass


@contextmanager
def conectar():
    conexion = sqlite3.connect(CHAT_DB_PATH, timeout=5)
    conexion.row_factory = sqlite3.Row
    try:
        yield conexion
    finally:
        conexion.close()


def inicializar_historial():
    CHAT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with conectar() as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("BEGIN IMMEDIATE")
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
        columnas = {fila[1] for fila in con.execute("PRAGMA table_info(conversaciones)")}
        if "ultimo_turno" not in columnas:
            con.execute("ALTER TABLE conversaciones ADD COLUMN ultimo_turno TEXT")
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
            (token, ahora + LIMITE_RESERVA, sid)
        )
        con.commit()
    return token, {
        "mensajes": json.loads(row["mensajes"]), "meta": json.loads(row["meta"])
    }


def guardar_turno(sid, cliente_id, token, mensajes, meta, request_id=None):
    with conectar() as con:
        result = con.execute(
            "UPDATE conversaciones SET mensajes = ?, meta = ?, lease = NULL, "
            "ocupado_hasta = 0, ultimo_turno = ? WHERE sid = ? AND cliente_id = ? AND lease = ? "
            "AND expira > ? AND ocupado_hasta > ?",
            (
                json.dumps(mensajes[-MAX_CHAT_MESSAGES:], ensure_ascii=False, allow_nan=False),
                json.dumps(meta, ensure_ascii=False, allow_nan=False),
                request_id, sid, cliente_id, token, time.time(), time.time()
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



def estado_conversacion(sid, cliente_id):
    # Recuperar historial y ocupación sin volver a consultar S3 ni al modelo
    ahora = time.time()
    with conectar() as con:
        con.execute(
            "UPDATE conversaciones SET lease = NULL, ocupado_hasta = 0 "
            "WHERE sid = ? AND cliente_id = ? AND ocupado_hasta <= ?",
            (sid, cliente_id, ahora),
        )
        row = con.execute(
            "SELECT mensajes, lease, ocupado_hasta, ultimo_turno FROM conversaciones "
            "WHERE sid = ? AND cliente_id = ? AND expira > ?",
            (sid, cliente_id, ahora),
        ).fetchone()
        con.commit()
    if row is None:
        raise SesionExpirada()
    return {
        "historial": json.loads(row["mensajes"]),
        "en_curso": bool(row["lease"] and row["ocupado_hasta"] > ahora),
        "ultimo_turno": row["ultimo_turno"],
    }
