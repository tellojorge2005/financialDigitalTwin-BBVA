import base64
import csv
import hashlib
import hmac
import io
import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import boto3
from botocore.config import Config
from langchain_core.callbacks import BaseCallbackHandler

# Importar config también carga el .env del proyecto.
from backend.config import AWS_REGION, S3_BUCKET, SESSION_SECRET


logger = logging.getLogger(__name__)

CONTADORES = (
    "llamadas_modelo",
    "errores_modelo",
    "llamadas_herramientas",
    "errores_herramientas",
    "usos_meta",
    "usos_escenario",
    "usos_historial",
    "usos_cancelacion",
)

CAMPOS_CSV = (
    "interaccion_id",
    "conversacion_id",
    "fecha_hora_utc",
    "fecha_utc",
    "hora_utc",
    "duracion_segundos",
    "http_status",
    "resultado",
    "registro_completo",
    *CONTADORES,
)

HERRAMIENTAS = {
    "gestionar_meta": "usos_meta",
    "gestionar_escenario": "usos_escenario",
    "ver_historial": "usos_historial",
    "cancelar_plan": "usos_cancelacion",
}


def serializar(valor):
    return json.dumps(valor, ensure_ascii=False, allow_nan=False)


class MetricasInteraccion(BaseCallbackHandler):
    """Contadores por solicitud; nunca conserva mensajes ni argumentos."""

    def __init__(self):
        super().__init__()
        self.interaccion_id = uuid4().hex
        self.inicio_epoch = time.time()
        self.inicio_reloj = time.perf_counter()
        self._lock = threading.Lock()
        self._contadores = dict.fromkeys(CONTADORES, 0)
        self._conversacion_id = None

        fecha = datetime.fromtimestamp(self.inicio_epoch, timezone.utc)
        self._base = {
            "interaccion_id": self.interaccion_id,
            "conversacion_id": None,
            "fecha_hora_utc": fecha.isoformat(timespec="milliseconds"),
            "fecha_utc": fecha.date().isoformat(),
            "hora_utc": fecha.hour,
            "duracion_segundos": None,
            "http_status": None,
            "resultado": "en_curso",
            "registro_completo": 0,
            **dict.fromkeys(CONTADORES, None),
        }

    def asociar_conversacion(self, sid):
        # Permite contar conversaciones sin exportar el token de sesión.
        identificador = hmac.new(
            SESSION_SECRET.encode("utf-8"),
            ("monitor:" + sid).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        with self._lock:
            self._conversacion_id = identificador

    def _incrementar(self, nombre):
        with self._lock:
            self._contadores[nombre] += 1

    def on_chat_model_start(self, serialized, messages, **kwargs):
        self._incrementar("llamadas_modelo")

    def on_llm_start(self, serialized, prompts, **kwargs):
        self._incrementar("llamadas_modelo")

    def on_llm_error(self, error, **kwargs):
        self._incrementar("errores_modelo")

    def on_tool_start(self, serialized, input_str, **kwargs):
        nombre = (serialized or {}).get("name")
        with self._lock:
            self._contadores["llamadas_herramientas"] += 1
            campo = HERRAMIENTAS.get(nombre)
            if campo:
                self._contadores[campo] += 1

    def on_tool_error(self, error, **kwargs):
        self._incrementar("errores_herramientas")

    def registro_inicial(self):
        return dict(self._base)

    def registro_final(self, http_status, resultado):
        with self._lock:
            return {
                **self._base,
                "conversacion_id": self._conversacion_id,
                "duracion_segundos": round(
                    time.perf_counter() - self.inicio_reloj, 3
                ),
                "http_status": http_status,
                "resultado": resultado,
                "registro_completo": 1,
                **self._contadores,
            }


class MonitorInteracciones:
    """Cola persistente de eventos y exportador de lotes CSV a S3."""

    def __init__(self):
        ruta = os.getenv("MONITOR_DB_PATH", "").strip()
        self.db_path = (
            Path(ruta).expanduser()
            if ruta
            else Path.home() / ".financial_digital_twin" / "monitor.sqlite3"
        )

        self.bucket = S3_BUCKET
        self.prefix = os.getenv(
            "S3_INTERACTIONS_PREFIX", ""
        ).strip().strip("/")

        self.intervalo = int(os.getenv("MONITOR_EXPORT_SECONDS", "300"))
        self.retencion_dias = int(os.getenv("MONITOR_RETENTION_DAYS", "30"))
        self.tamano_lote = 500

        if not self.bucket or not self.prefix:
            raise RuntimeError(
                "Configura S3_BUCKET y S3_INTERACTIONS_PREFIX en .env."
            )
        if "://" in self.prefix:
            raise RuntimeError(
                "S3_INTERACTIONS_PREFIX debe ser un prefijo, no una URL."
            )
        if self.intervalo < 10 or self.retencion_dias < 1:
            raise RuntimeError(
                "MONITOR_EXPORT_SECONDS debe ser al menos 10 y "
                "MONITOR_RETENTION_DAYS al menos 1."
            )

        self._detener = threading.Event()
        self._despertar = threading.Event()
        self._hilo = None
        self._s3 = None

    @contextmanager
    def _conexion(self):
        con = sqlite3.connect(self.db_path, timeout=5)
        con.row_factory = sqlite3.Row
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def inicializar(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with self._conexion() as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.executescript("""
                CREATE TABLE IF NOT EXISTS interacciones (
                    id TEXT PRIMARY KEY,
                    inicio_epoch REAL NOT NULL,
                    registro TEXT NOT NULL,
                    finalizado INTEGER NOT NULL DEFAULT 0,
                    lote_id TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_interacciones_pendientes
                ON interacciones(finalizado, lote_id, inicio_epoch);

                CREATE TABLE IF NOT EXISTS lotes (
                    id TEXT PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    objeto TEXT NOT NULL,
                    contenido BLOB,
                    creado REAL NOT NULL,
                    exportado REAL
                );
            """)

    def registrar_inicio(self, metricas):
        try:
            with self._conexion() as con:
                con.execute(
                    """
                    INSERT INTO interacciones(id, inicio_epoch, registro)
                    VALUES (?, ?, ?)
                    """,
                    (
                        metricas.interaccion_id,
                        metricas.inicio_epoch,
                        serializar(metricas.registro_inicial()),
                    ),
                )
        except Exception as exc:
            # Un problema de monitoreo no debe romper el chat.
            logger.error(
                "No se registró el inicio de una interacción: %s",
                type(exc).__name__,
            )

    def registrar_fin(self, metricas, registro):
        try:
            contenido = serializar(registro)
            with self._conexion() as con:
                # Recuperar el registro si el guardado inicial falló.
                con.execute(
                    """
                    INSERT OR IGNORE INTO interacciones
                        (id, inicio_epoch, registro, finalizado)
                    VALUES (?, ?, ?, 1)
                    """,
                    (
                        metricas.interaccion_id,
                        metricas.inicio_epoch,
                        contenido,
                    ),
                )
                con.execute(
                    """
                    UPDATE interacciones
                    SET registro = ?, finalizado = 1
                    WHERE id = ? AND finalizado = 0 AND lote_id IS NULL
                    """,
                    (contenido, metricas.interaccion_id),
                )
        except Exception as exc:
            logger.error(
                "No se registró el cierre de una interacción: %s",
                type(exc).__name__,
            )

    def _recuperar_sin_cierre(self, con):
        # Un cierre abrupto puede dejar solicitudes abiertas.
        # Después de una hora se registran como incompletas, sin inventar
        # duración, resultado HTTP ni contadores.
        pendientes = con.execute(
            """
            SELECT id, registro FROM interacciones
            WHERE finalizado = 0 AND inicio_epoch < ?
            ORDER BY inicio_epoch LIMIT 500
            """,
            (time.time() - 3600,),
        ).fetchall()

        for fila in pendientes:
            registro = json.loads(fila["registro"])
            registro["resultado"] = "sin_cierre"
            con.execute(
                """
                UPDATE interacciones SET registro = ?, finalizado = 1
                WHERE id = ? AND finalizado = 0
                """,
                (serializar(registro), fila["id"]),
            )

    def _siguiente_lote(self):
        # La asignación de registros y el contenido del CSV se confirman
        # juntos. El lote no cambia aunque haya que reintentar la subida.
        with self._conexion() as con:
            con.execute("BEGIN IMMEDIATE")
            self._recuperar_sin_cierre(con)

            pendiente = con.execute(
                """
                SELECT * FROM lotes
                WHERE exportado IS NULL
                ORDER BY creado, id LIMIT 1
                """
            ).fetchone()

            if pendiente:
                return dict(pendiente)

            filas = con.execute(
                """
                SELECT id, registro FROM interacciones
                WHERE finalizado = 1 AND lote_id IS NULL
                ORDER BY inicio_epoch, id LIMIT ?
                """,
                (self.tamano_lote,),
            ).fetchall()

            if not filas:
                return None

            lote_id = uuid4().hex
            ahora = datetime.now(timezone.utc)
            objeto = (
                f"{self.prefix}/{ahora:%Y/%m/%d}/lote_{lote_id}.csv"
            )

            buffer = io.StringIO(newline="")
            writer = csv.DictWriter(
                buffer,
                fieldnames=CAMPOS_CSV,
                lineterminator="\n",
            )
            writer.writeheader()
            for fila in filas:
                writer.writerow(json.loads(fila["registro"]))

            # UTF-8 con BOM facilita la importación de acentos.
            contenido = buffer.getvalue().encode("utf-8-sig")

            con.execute(
                """
                INSERT INTO lotes
                    (id, bucket, objeto, contenido, creado)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    lote_id,
                    self.bucket,
                    objeto,
                    contenido,
                    time.time(),
                ),
            )
            con.executemany(
                "UPDATE interacciones SET lote_id = ? WHERE id = ?",
                [(lote_id, fila["id"]) for fila in filas],
            )

            return {
                "id": lote_id,
                "bucket": self.bucket,
                "objeto": objeto,
                "contenido": contenido,
            }

    def _subir_lote(self, lote):
        if self._s3 is None:
            self._s3 = boto3.Session(
                region_name=AWS_REGION
            ).client(
                "s3",
                config=Config(
                    connect_timeout=5,
                    read_timeout=15,
                    retries={"mode": "standard", "total_max_attempts": 2},
                ),
            )

        contenido = bytes(lote["contenido"])
        checksum = base64.b64encode(
            hashlib.md5(contenido).digest()
        ).decode("ascii")

        self._s3.put_object(
            Bucket=lote["bucket"],
            Key=lote["objeto"],
            Body=contenido,
            ContentType="text/csv; charset=utf-8",
            ContentMD5=checksum,
        )

        # Si este UPDATE falla tras subir, se reintentará el mismo objeto
        # con exactamente el mismo contenido.
        with self._conexion() as con:
            con.execute(
                """
                UPDATE lotes
                SET exportado = ?, contenido = NULL
                WHERE id = ? AND exportado IS NULL
                """,
                (time.time(), lote["id"]),
            )

        logger.info("Lote de monitoreo exportado: %s", lote["id"])

    def _limpiar_exportados(self):
        limite = time.time() - self.retencion_dias * 86400
        with self._conexion() as con:
            con.execute(
                """
                DELETE FROM interacciones
                WHERE lote_id IN (
                    SELECT id FROM lotes
                    WHERE exportado IS NOT NULL AND exportado < ?
                )
                """,
                (limite,),
            )
            con.execute(
                """
                DELETE FROM lotes
                WHERE exportado IS NOT NULL AND exportado < ?
                """,
                (limite,),
            )

    def _exportar(self, max_lotes):
        try:
            for _ in range(max_lotes):
                lote = self._siguiente_lote()
                if lote is None:
                    break
                self._subir_lote(lote)
                if self._detener.is_set():
                    break

            self._limpiar_exportados()
        except Exception as exc:
            logger.warning(
                "Exportación de monitoreo pendiente; se reintentará: %s",
                type(exc).__name__,
            )

    def _ejecutar(self):
        # Recuperar pendientes al arrancar, sin bloquear la aplicación.
        self._exportar(max_lotes=10)

        while not self._detener.is_set():
            self._despertar.wait(self.intervalo)
            self._despertar.clear()
            if self._detener.is_set():
                break
            self._exportar(max_lotes=10)

        # Intento final acotado al apagar normalmente el backend.
        self._exportar(max_lotes=1)

    def iniciar_exportador(self):
        self._hilo = threading.Thread(
            target=self._ejecutar,
            name="monitor-s3",
            daemon=True,
        )
        self._hilo.start()

    def solicitar_exportacion(self):
        # No realizar llamadas a S3 dentro de la respuesta de logout.
        self._despertar.set()

    def detener_exportador(self):
        self._detener.set()
        self._despertar.set()
        if self._hilo:
            self._hilo.join(timeout=30)
            if self._hilo.is_alive():
                logger.warning(
                    "El exportador sigue terminando; los lotes permanecen "
                    "respaldados en SQLite."
                )