"""
Pipeline ETL completo: de los archivos crudos a pipeline_database.db.

Fases:
    1. Diagnóstico de calidad de los datos crudos.
    2-3. Limpieza: imputación de faltantes y capping IQR (las ventas online
         se convierten a MXN antes, para que el IQR no mezcle divisas).
    4. Integración: fechas ISO, unificación de canales, duplicados y huérfanos.
    5. Transformación: Min-Max y segmentación del monto.
    6. Validación con Pydantic y cuarentena.
    7. Carga transaccional a SQLite.

Si una fase falla, se informa cuál y por qué, y el proceso termina con
código de salida 1.

Ejecución: python main.py
"""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

try:
    import pandas as pd

    from src.database import cargar_resultados, resumen_bd
    from src.diagnostico import perfilado_inicial
    from src.integracion import convertir_a_mxn, integrar
    from src.limpieza import limpiar_datos
    from src.schemas import rechazos_a_cuarentena, validar_lote
    from src.transformacion import transformar
except ImportError as error:
    sys.exit(f"No se pudieron importar las dependencias del pipeline: {error}. "
             "Instálelas con: pip install -r requirements.txt")

BASE = Path(__file__).resolve().parent
RUTA_FISICA = BASE / "datasets" / "ventas_tienda_fisica.csv"
RUTA_ONLINE = BASE / "datasets" / "ventas_online.json"
RUTA_CATALOGO = BASE / "data" / "catalogo_maestro.csv"
RUTA_DB = BASE / "pipeline_database.db"

MOTIVO_HUERFANO = "producto huérfano: no existe en el catálogo maestro"
MOTIVO_CANTIDAD = "cantidad <= 0: valor imposible"

log = logging.getLogger("pipeline")


class FaseFallida(Exception):
    """Error de una fase, con el nombre de la fase en el mensaje."""


def fase(nombre: str, funcion, *args, **kwargs):
    """Ejecuta una fase y convierte cualquier error en FaseFallida."""
    log.info("-> %s", nombre)
    try:
        return funcion(*args, **kwargs)
    except Exception as error:
        raise FaseFallida(f"{nombre}: {type(error).__name__}: {error}") from error


# ---------------------------------------------------------------------------
# Pasos del pipeline
# ---------------------------------------------------------------------------

def leer_fuentes() -> dict[str, pd.DataFrame]:
    with open(RUTA_ONLINE, encoding="utf-8") as archivo:
        online = pd.json_normalize(json.load(archivo))
    return {
        "fisica": pd.read_csv(RUTA_FISICA),
        "online": online,
        "catalogo": pd.read_csv(RUTA_CATALOGO),
    }


def diagnosticar(fuentes: dict[str, pd.DataFrame]) -> None:
    for nombre in ("fisica", "online"):
        perfil = perfilado_inicial(fuentes[nombre])
        nulos = {c: p for c, p in perfil["porcentaje_nulos"].items() if p > 0}
        texto_nulos = ", ".join(f"{c} {p:.1f} %" for c, p in nulos.items()) or "ninguno"
        log.info("   %-7s duplicados exactos: %d | nulos: %s",
                 nombre, perfil["filas_duplicadas"], texto_nulos)


def separar_imposibles(df: pd.DataFrame, columna: str, canal: str) -> pd.DataFrame:
    """Filas con cantidad <= 0, que limpiar_datos descarta."""
    cantidad = pd.to_numeric(df[columna], errors="coerce")
    return df.loc[cantidad.le(0)].assign(canal=canal)


def armar_cuarentena(validacion, huerfanos, imposibles: list[pd.DataFrame]) -> pd.DataFrame:
    # Cada fuente cruda por separado: sus columnas de id tienen nombres distintos
    # (id_ticket, order_id) y juntas en un solo DataFrame se duplicarían al
    # renombrarse a id_transaccion.
    return pd.concat(
        [
            validacion.cuarentena,
            rechazos_a_cuarentena(huerfanos, MOTIVO_HUERFANO, "integracion"),
            *(rechazos_a_cuarentena(df, MOTIVO_CANTIDAD, "limpieza") for df in imposibles),
        ],
        ignore_index=True,
    )


def _motivo_generico(motivo: str) -> str:
    """Quita valores concretos (textos entre comillas, ids, cifras) para agrupar."""
    motivo = re.sub(r"\s*\(valor recibido: .*\)$", "", motivo)
    motivo = re.sub(r"'[^']*'", "<valor>", motivo)
    motivo = re.sub(r"\b(POS|WEB|PROD)-[\d-]+", "<id>", motivo)
    return re.sub(r"\d[\d.,]*", "<n>", motivo).strip()


def motivos_frecuentes(cuarentena: pd.DataFrame, n: int = 3) -> pd.Series:
    motivos = (
        cuarentena["motivo_rechazo_cuarentena"]
        .fillna("")
        .str.split(" | ", regex=False)
        .explode()
        .map(_motivo_generico)
    )
    return motivos[motivos != ""].value_counts().head(n)


# ---------------------------------------------------------------------------
# Resumen
# ---------------------------------------------------------------------------

def imprimir_resumen(conteos: dict, cuarentena: pd.DataFrame, carga, bd: dict) -> None:
    linea = "=" * 64
    log.info("")
    log.info(linea)
    log.info("RESUMEN DEL PIPELINE")
    log.info(linea)
    log.info("Registros leídos")
    log.info("   tienda física           %6d", conteos["leidos_fisica"])
    log.info("   online                  %6d", conteos["leidos_online"])
    log.info("   catálogo (productos)    %6d", conteos["leidos_catalogo"])
    log.info("Salida por fase")
    log.info("   limpieza                %6d   (%d descartados por cantidad <= 0)",
             conteos["limpios_fase3"], conteos["imposibles"])
    log.info("   integración             %6d   (%d duplicados, %d huérfanos)",
             conteos["validos_fase4"], conteos["duplicados"], conteos["huerfanos"])
    log.info("   transformación          %6d", conteos["transformados"])
    log.info("   validación              %6d   (%d a cuarentena)",
             conteos["conformes"], conteos["rechazados_validacion"])
    log.info("Transacciones limpias por canal")
    for canal, total in sorted(bd["por_canal"].items()):
        log.info("   %-23s %6d", canal, total)
    log.info("Cuarentena por etapa")
    for etapa, total in sorted(bd["cuarentena_por_etapa"].items()):
        log.info("   %-23s %6d", etapa, total)
    log.info("Motivos de rechazo más comunes")
    for motivo, total in motivos_frecuentes(cuarentena).items():
        log.info("   %6d  %s", total, motivo)
    log.info("Segmentos de monto")
    for segmento in ("Bajo", "Medio", "Alto"):
        log.info("   %-23s %6d", segmento, bd["por_segmento"].get(segmento, 0))
    log.info(linea)
    log.info("Carga: %s | limpias %d | cuarentena %d | monto total %s MXN",
             carga.estado, bd["transacciones_limpias"], bd["cuarentena_errores"],
             f"{bd['monto_total_mxn']:,.2f}")
    log.info("Base: %s", RUTA_DB)
    log.info(linea)


# ---------------------------------------------------------------------------
# Orquestación
# ---------------------------------------------------------------------------

def ejecutar() -> int:
    fuentes = fase("Lectura de archivos", leer_fuentes)
    catalogo = fuentes["catalogo"]

    fase("Fase 1: diagnóstico", diagnosticar, fuentes)

    imposibles = fase(
        "Separación de cantidades <= 0",
        lambda: [
            separar_imposibles(fuentes["fisica"], "cantidad", "FISICA"),
            separar_imposibles(fuentes["online"], "units", "ONLINE"),
        ],
    )

    online_mxn = fase("Conversión de divisas a MXN", convertir_a_mxn,
                      fuentes["online"], col_moneda="currency")
    fisica_limpia = fase("Fases 2 y 3: limpieza (tienda física)", limpiar_datos, fuentes["fisica"])
    online_limpia = fase("Fases 2 y 3: limpieza (online)", limpiar_datos, online_mxn)

    validos, huerfanos, resumen_integracion = fase(
        "Fase 4: integración", integrar, fisica_limpia, online_limpia, catalogo
    )
    transformado = fase("Fase 5: transformación", transformar, validos)
    validacion = fase("Fase 6: validación", validar_lote, transformado, catalogo["id_producto"])
    cuarentena = fase("Armado de la cuarentena", armar_cuarentena, validacion, huerfanos, imposibles)

    carga = fase("Fase 7: carga a SQLite", cargar_resultados,
                 validacion.limpios, cuarentena, RUTA_DB, df_catalogo=catalogo)
    if not carga.exitosa:
        raise FaseFallida(f"Fase 7: carga a SQLite: ROLLBACK: {carga.error}")
    bd = fase("Lectura de conteos de la base", resumen_bd, RUTA_DB)

    conteos = {
        "leidos_fisica": len(fuentes["fisica"]),
        "leidos_online": len(fuentes["online"]),
        "leidos_catalogo": len(catalogo),
        "imposibles": sum(len(df) for df in imposibles),
        "limpios_fase3": len(fisica_limpia) + len(online_limpia),
        "duplicados": resumen_integracion["duplicados_pos_eliminados"],
        "huerfanos": resumen_integracion["huerfanos_a_cuarentena"],
        "validos_fase4": resumen_integracion["registros_validos"],
        "transformados": len(transformado),
        "conformes": validacion.resumen["conformes"],
        "rechazados_validacion": validacion.resumen["en_cuarentena"],
    }
    fase("Resumen", imprimir_resumen, conteos, cuarentena, carga, bd)
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    try:
        return ejecutar()
    except FaseFallida as error:
        log.error("ERROR en %s", error)
        log.error("El pipeline se detuvo.")
        return 1
    except Exception as error:  # cualquier error fuera de una fase
        log.error("ERROR inesperado: %s: %s", type(error).__name__, error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
