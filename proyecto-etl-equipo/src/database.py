"""
Fase 7: carga transaccional a SQLite.

Guarda en pipeline_database.db las dos tablas que pide el proyecto:

    transacciones_limpias   registros que pasaron la validación de schemas.py
    cuarentena_errores      registros rechazados con su motivo

Además lleva una tabla bitacora_cargas con cada ejecución (inicio, fin,
filas cargadas y si terminó en COMMIT o en ROLLBACK), y opcionalmente el
catálogo de productos.

Las dos tablas principales se escriben dentro de una sola transacción. Si
cualquier INSERT falla, se hace rollback y la base queda exactamente como
estaba antes de la carga. La función no lanza la excepción hacia main.py;
devuelve un ResultadoCarga con el estado y el error para que el pipeline
pueda reportarlo y terminar en orden.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

# En la carpeta del proyecto, sin importar desde dónde se ejecute main.py.
RUTA_DB_DEFAULT = Path(__file__).resolve().parents[1] / "pipeline_database.db"

DDL = """
CREATE TABLE IF NOT EXISTS bitacora_cargas (
    id_carga          INTEGER PRIMARY KEY AUTOINCREMENT,
    inicio            TEXT NOT NULL,
    fin               TEXT,
    estado            TEXT NOT NULL CHECK (estado IN ('EN_PROCESO', 'COMMIT', 'ROLLBACK')),
    filas_limpias     INTEGER DEFAULT 0,
    filas_cuarentena  INTEGER DEFAULT 0,
    mensaje           TEXT
);

CREATE TABLE IF NOT EXISTS catalogo_productos (
    id_producto      TEXT PRIMARY KEY,
    nombre_producto  TEXT,
    categoria        TEXT,
    precio_base_mxn  REAL CHECK (precio_base_mxn IS NULL OR precio_base_mxn > 0),
    stock_minimo     INTEGER,
    proveedor        TEXT
);

CREATE TABLE IF NOT EXISTS transacciones_limpias (
    id_transaccion       TEXT PRIMARY KEY,
    canal                TEXT NOT NULL CHECK (canal IN ('TIENDA_FISICA', 'ONLINE')),
    fecha                TEXT NOT NULL CHECK (fecha GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    id_producto          TEXT NOT NULL,
    cantidad             INTEGER NOT NULL CHECK (cantidad > 0),
    precio_unitario_mxn  REAL NOT NULL CHECK (precio_unitario_mxn > 0),
    monto_total_mxn      REAL NOT NULL CHECK (monto_total_mxn > 0),
    moneda               TEXT NOT NULL DEFAULT 'MXN' CHECK (moneda = 'MXN'),
    metodo_pago          TEXT NOT NULL,
    sucursal             TEXT,
    id_cliente           TEXT,
    correo_cliente       TEXT,
    estado_orden         TEXT,
    clabe                TEXT,
    monto_normalizado    REAL CHECK (monto_normalizado IS NULL OR monto_normalizado BETWEEN 0 AND 1),
    segmento_monto       TEXT CHECK (segmento_monto IS NULL OR segmento_monto IN ('Bajo', 'Medio', 'Alto')),
    id_carga             INTEGER REFERENCES bitacora_cargas(id_carga),
    fecha_carga          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cuarentena_errores (
    id_error                   INTEGER PRIMARY KEY AUTOINCREMENT,
    id_origen                  TEXT,
    canal                      TEXT,
    etapa                      TEXT NOT NULL,
    campos_con_error           TEXT,
    motivo_rechazo_cuarentena  TEXT NOT NULL,
    registro_original          TEXT NOT NULL,
    fecha_rechazo              TEXT NOT NULL,
    id_carga                   INTEGER REFERENCES bitacora_cargas(id_carga)
);

CREATE INDEX IF NOT EXISTS idx_limpias_fecha    ON transacciones_limpias(fecha);
CREATE INDEX IF NOT EXISTS idx_limpias_producto ON transacciones_limpias(id_producto);
CREATE INDEX IF NOT EXISTS idx_cuarentena_etapa ON cuarentena_errores(etapa);
"""

COLUMNAS_LIMPIAS_BD = [
    "id_transaccion", "canal", "fecha", "id_producto", "cantidad",
    "precio_unitario_mxn", "monto_total_mxn", "moneda", "metodo_pago",
    "sucursal", "id_cliente", "correo_cliente", "estado_orden", "clabe",
    "monto_normalizado", "segmento_monto",
]

COLUMNAS_CUARENTENA_BD = [
    "id_origen", "canal", "etapa", "campos_con_error",
    "motivo_rechazo_cuarentena", "registro_original", "fecha_rechazo",
]

COLUMNAS_CATALOGO_BD = [
    "id_producto", "nombre_producto", "categoria",
    "precio_base_mxn", "stock_minimo", "proveedor",
]


@dataclass
class ResultadoCarga:
    estado: str                 # 'COMMIT' o 'ROLLBACK'
    id_carga: Optional[int]
    filas_limpias: int = 0
    filas_cuarentena: int = 0
    error: Optional[str] = None

    @property
    def exitosa(self) -> bool:
        return self.estado == "COMMIT"


def _ahora() -> str:
    return datetime.now().isoformat(timespec="seconds")


def conectar(ruta_db: str | Path = RUTA_DB_DEFAULT) -> sqlite3.Connection:
    """
    Abre la base en modo de transacciones manuales (isolation_level=None),
    así cada BEGIN, COMMIT y ROLLBACK queda escrito en el código y no depende
    del comportamiento implícito del módulo sqlite3.
    """
    conn = sqlite3.connect(str(ruta_db), isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def crear_tablas(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)


def _a_filas(df: Optional[pd.DataFrame], columnas: list[str]) -> list[tuple]:
    """Toma solo las columnas de la tabla y cambia NaN por NULL."""
    if df is None or df.empty:
        return []
    faltantes = [c for c in columnas if c not in df.columns]
    if faltantes:
        df = df.copy()
        for c in faltantes:
            df[c] = None
    filas = []
    for fila in df[columnas].itertuples(index=False, name=None):
        filas.append(tuple(
            None if (v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NA)
            else (v.item() if hasattr(v, "item") else v)
            for v in fila
        ))
    return filas


def cargar_resultados(
    df_limpios: pd.DataFrame,
    df_cuarentena: pd.DataFrame,
    ruta_db: str | Path = RUTA_DB_DEFAULT,
    df_catalogo: Optional[pd.DataFrame] = None,
    reemplazar: bool = True,
) -> ResultadoCarga:
    """
    Escribe limpios y cuarentena en una sola transacción.

    reemplazar=True vacía las tablas antes de insertar, de modo que correr
    el pipeline dos veces deja la misma base y no duplica registros. La
    bitácora nunca se borra.
    """
    conn = None
    id_carga = None

    try:
        filas_limpias = _a_filas(df_limpios, COLUMNAS_LIMPIAS_BD)
        filas_cuarentena = _a_filas(df_cuarentena, COLUMNAS_CUARENTENA_BD)
        filas_catalogo = _a_filas(df_catalogo, COLUMNAS_CATALOGO_BD)
        conn = conectar(ruta_db)
        crear_tablas(conn)

        # La bitácora se abre fuera de la transacción principal para que el
        # intento quede registrado aunque después haya rollback.
        cur = conn.execute(
            "INSERT INTO bitacora_cargas (inicio, estado) VALUES (?, 'EN_PROCESO')",
            (_ahora(),),
        )
        id_carga = cur.lastrowid
        fecha_carga = _ahora()

        conn.execute("BEGIN")

        if reemplazar:
            conn.execute("DELETE FROM transacciones_limpias")
            conn.execute("DELETE FROM cuarentena_errores")
            if filas_catalogo:
                conn.execute("DELETE FROM catalogo_productos")

        if filas_catalogo:
            conn.executemany(
                f"INSERT OR REPLACE INTO catalogo_productos ({', '.join(COLUMNAS_CATALOGO_BD)}) "
                f"VALUES ({', '.join('?' * len(COLUMNAS_CATALOGO_BD))})",
                filas_catalogo,
            )

        conn.executemany(
            f"INSERT INTO transacciones_limpias ({', '.join(COLUMNAS_LIMPIAS_BD)}, id_carga, fecha_carga) "
            f"VALUES ({', '.join('?' * len(COLUMNAS_LIMPIAS_BD))}, ?, ?)",
            [fila + (id_carga, fecha_carga) for fila in filas_limpias],
        )

        conn.executemany(
            f"INSERT INTO cuarentena_errores ({', '.join(COLUMNAS_CUARENTENA_BD)}, id_carga) "
            f"VALUES ({', '.join('?' * len(COLUMNAS_CUARENTENA_BD))}, ?)",
            [fila + (id_carga,) for fila in filas_cuarentena],
        )

        conn.execute("COMMIT")
        _cerrar_bitacora(conn, id_carga, "COMMIT", len(filas_limpias), len(filas_cuarentena), None)
        log.info(
            "Carga %s confirmada: %d limpias, %d en cuarentena",
            id_carga, len(filas_limpias), len(filas_cuarentena),
        )
        return ResultadoCarga("COMMIT", id_carga, len(filas_limpias), len(filas_cuarentena))

    except Exception as error:
        mensaje = f"{type(error).__name__}: {error}"
        if conn is not None and conn.in_transaction:
            conn.execute("ROLLBACK")
        log.error("Carga %s revertida: %s", id_carga, mensaje)
        if conn is not None and id_carga is not None:
            _cerrar_bitacora(conn, id_carga, "ROLLBACK", 0, 0, mensaje)
        return ResultadoCarga("ROLLBACK", id_carga, 0, 0, mensaje)

    finally:
        if conn is not None:
            conn.close()


def _cerrar_bitacora(conn, id_carga, estado, n_limpias, n_cuarentena, mensaje) -> None:
    try:
        conn.execute(
            "UPDATE bitacora_cargas SET fin = ?, estado = ?, filas_limpias = ?, "
            "filas_cuarentena = ?, mensaje = ? WHERE id_carga = ?",
            (_ahora(), estado, n_limpias, n_cuarentena, mensaje, id_carga),
        )
    except sqlite3.Error as error:
        log.warning("No se pudo actualizar la bitácora: %s", error)


def resumen_bd(ruta_db: str | Path = RUTA_DB_DEFAULT) -> dict:
    """Conteos rápidos para imprimir al final del pipeline o en la defensa."""
    conn = conectar(ruta_db)
    try:
        def uno(sql):
            return conn.execute(sql).fetchone()[0]

        return {
            "transacciones_limpias": uno("SELECT COUNT(*) FROM transacciones_limpias"),
            "cuarentena_errores": uno("SELECT COUNT(*) FROM cuarentena_errores"),
            "monto_total_mxn": round(uno("SELECT COALESCE(SUM(monto_total_mxn), 0) FROM transacciones_limpias"), 2),
            "por_canal": dict(conn.execute(
                "SELECT canal, COUNT(*) FROM transacciones_limpias GROUP BY canal"
            ).fetchall()),
            "por_segmento": dict(conn.execute(
                "SELECT COALESCE(segmento_monto, 'SIN_SEGMENTO'), COUNT(*) "
                "FROM transacciones_limpias GROUP BY 1"
            ).fetchall()),
            "cuarentena_por_etapa": dict(conn.execute(
                "SELECT etapa, COUNT(*) FROM cuarentena_errores GROUP BY etapa"
            ).fetchall()),
            "ultima_carga": conn.execute(
                "SELECT id_carga, estado, inicio, mensaje FROM bitacora_cargas "
                "ORDER BY id_carga DESC LIMIT 1"
            ).fetchone(),
        }
    finally:
        conn.close()
