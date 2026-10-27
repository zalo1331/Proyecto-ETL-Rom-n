"""Integración multifuente: fechas ISO, divisas, unificación y huérfanos (Fase 4)."""

from __future__ import annotations

import pandas as pd

TIPO_CAMBIO_USD_MXN = 19.50
COLUMNA_MOTIVO = "motivo_rechazo_cuarentena"

COLUMNAS_UNIFICADAS = [
    "id_transaccion", "canal", "fecha", "id_producto", "cantidad", "precio",
    "monto_total", "moneda_original", "id_cliente", "correo_cliente",
    "direccion_envio", "sucursal", "metodo_pago", "pasarela_pago",
    "estado_pedido",
]

# Nombres de origen -> nombres unificados (por canal).
_MAPA_FISICA = {
    "id_ticket": "id_transaccion",
    "fecha_venta": "fecha",
}
_MAPA_ONLINE = {
    "order_id": "id_transaccion",
    "order_timestamp": "fecha",
    "product_id": "id_producto",
    "customer_email": "correo_cliente",
    "shipping_address": "direccion_envio",
    "order_status": "estado_pedido",
    "payment_gateway": "pasarela_pago",
    "currency": "moneda_original",
}


# --------------------------------------------------------------------------
# Catálogo maestro
# --------------------------------------------------------------------------
def preparar_catalogo(df_catalogo: pd.DataFrame) -> pd.DataFrame:
    """Normaliza el catálogo y valida que ``id_producto`` sea único."""
    if "id_producto" not in df_catalogo.columns:
        raise ValueError("El catálogo debe incluir la columna 'id_producto'.")
    cat = df_catalogo.copy()
    cat["id_producto"] = cat["id_producto"].astype("string").str.strip().str.upper()
    if cat["id_producto"].isna().any() or cat["id_producto"].duplicated().any():
        raise ValueError("El catálogo tiene id_producto nulos o duplicados.")
    return cat


# --------------------------------------------------------------------------
# Fechas -> YYYY-MM-DD
# --------------------------------------------------------------------------
def _fecha_a_iso(valor) -> str | None:
    """Convierte un valor heterogéneo a 'YYYY-MM-DD' (None si no es válido).

    Formatos soportados: ISO (``2026-10-03``), ISO con hora, ``DD-MM-YYYY``,
    ``DD/MM/YYYY`` y epoch Unix en segundos (interpretado como UTC).
    Los formatos con día al inicio se leen como DD/MM (convención mexicana).
    """
    if valor is None or (not isinstance(valor, str) and pd.isna(valor)):
        return None
    if isinstance(valor, (int, float)) and not isinstance(valor, bool):
        f = pd.to_datetime(valor, unit="s", utc=True, errors="coerce")
    else:
        texto = str(valor).strip()
        if texto.isdigit():
            f = pd.to_datetime(int(texto), unit="s", utc=True, errors="coerce")
        elif len(texto) >= 4 and texto[:4].isdigit() and texto[4] in "-/":
            f = pd.to_datetime(texto, errors="coerce", yearfirst=True)
        else:
            f = pd.to_datetime(texto, errors="coerce", dayfirst=True)
    return None if pd.isna(f) else f.strftime("%Y-%m-%d")


def unificar_fechas(df: pd.DataFrame, columna: str = "fecha") -> pd.DataFrame:
    """Devuelve una copia con ``columna`` en ISO; fechas ilegibles quedan nulas."""
    out = df.copy()
    out[columna] = out[columna].map(_fecha_a_iso)
    return out


# --------------------------------------------------------------------------
# Divisas
# --------------------------------------------------------------------------
def convertir_a_mxn(
    df: pd.DataFrame,
    tipo_cambio: float = TIPO_CAMBIO_USD_MXN,
    col_moneda: str = "moneda_original",
) -> pd.DataFrame:
    """Convierte a MXN los importes de las filas en USD.

    Convierte el precio (``precio``, ``unit_price`` o ``precio_unitario``) y el
    total (``monto_total`` o ``total_amount``) que existan. Conviene ejecutarla
    ANTES de ``limpiar_datos`` para que el IQR no mezcle escalas USD y MXN.
    """
    if col_moneda not in df.columns:
        raise ValueError(f"Falta la columna de moneda '{col_moneda}'.")
    out = df.copy()
    moneda = out[col_moneda].astype("string").str.strip().str.upper()
    desconocidas = set(moneda.dropna().unique()) - {"MXN", "USD"}
    if desconocidas:
        raise ValueError(f"Monedas no soportadas: {sorted(desconocidas)}.")
    es_usd = moneda.eq("USD").fillna(False).to_numpy()
    for col in ("precio", "unit_price", "precio_unitario", "monto_total", "total_amount"):
        if col in out.columns:
            out.loc[es_usd, col] = (out.loc[es_usd, col] * tipo_cambio).round(2)
    out[col_moneda] = moneda.fillna("MXN")  # se conserva la original en moneda_original
    return out


# --------------------------------------------------------------------------
# Armonización y fusión de fuentes
# --------------------------------------------------------------------------
def _estandarizar_texto(serie: pd.Series, mayusculas: bool = False) -> pd.Series:
    s = serie.astype("string").str.strip()
    return s.str.upper() if mayusculas else s.str.title()


def armonizar_fisica(df: pd.DataFrame) -> pd.DataFrame:
    """Lleva la tienda física (ya limpia) al esquema unificado."""
    out = df.rename(columns=_MAPA_FISICA).copy()
    out["canal"] = "FISICA"
    out["moneda_original"] = "MXN"
    out["metodo_pago"] = _estandarizar_texto(out["metodo_pago"])
    return out


def armonizar_online(df: pd.DataFrame) -> pd.DataFrame:
    """Lleva las ventas online (ya limpias y convertidas) al esquema unificado."""
    out = df.rename(columns=_MAPA_ONLINE).copy()
    out["canal"] = "ONLINE"
    out["estado_pedido"] = _estandarizar_texto(out["estado_pedido"], mayusculas=True)
    return out


def fusionar_fuentes(df_fisica: pd.DataFrame, df_online: pd.DataFrame) -> pd.DataFrame:
    """Concatena ambos canales en un único DataFrame con fechas ISO."""
    fis, onl = armonizar_fisica(df_fisica), armonizar_online(df_online)
    union = pd.concat([fis, onl], ignore_index=True, sort=False)
    for col in COLUMNAS_UNIFICADAS:
        if col not in union.columns:
            union[col] = pd.NA
    union = union[COLUMNAS_UNIFICADAS].copy()
    union = unificar_fechas(union, "fecha")
    # Campos exclusivos de un canal: etiqueta controlada, no nulo.
    for col in ("id_cliente", "correo_cliente", "direccion_envio", "sucursal",
                "metodo_pago", "pasarela_pago", "estado_pedido"):
        union[col] = union[col].astype("string").fillna("NO_APLICA")
    union["id_producto"] = union["id_producto"].astype("string").str.strip().str.upper()
    return union


def eliminar_duplicados(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Elimina reintentos de POS: misma ``id_transaccion`` (conserva la primera)."""
    antes = len(df)
    out = df.drop_duplicates(subset="id_transaccion", keep="first").reset_index(drop=True)
    return out, antes - len(out)


# --------------------------------------------------------------------------
# Catálogo y huérfanos
# --------------------------------------------------------------------------
def cruzar_con_catalogo(df: pd.DataFrame, df_catalogo: pd.DataFrame) -> pd.DataFrame:
    """Cruza (left join) contra el catálogo y marca huérfanos.

    Agrega ``nombre_producto``, ``categoria``, ``precio_base_mxn``,
    ``es_huerfano`` y ``motivo_rechazo_cuarentena`` (nulo si no es huérfano).
    """
    cat = preparar_catalogo(df_catalogo)[
        ["id_producto", "nombre_producto", "categoria", "precio_base_mxn"]
    ]
    cruce = df.merge(cat, on="id_producto", how="left", validate="many_to_one",
                     indicator=True)
    cruce["es_huerfano"] = cruce["_merge"].eq("left_only")
    cruce = cruce.drop(columns="_merge")
    cruce[COLUMNA_MOTIVO] = pd.Series(pd.NA, index=cruce.index, dtype="string")
    mask = cruce["es_huerfano"]
    cruce.loc[mask, COLUMNA_MOTIVO] = (
        "Producto huérfano: " + cruce.loc[mask, "id_producto"].astype("string")
        + " no existe en el catálogo maestro"
    )
    return cruce


def separar_huerfanos(df_cruzado: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Devuelve ``(validos, huerfanos)``; los huérfanos van a cuarentena."""
    mask = df_cruzado["es_huerfano"]
    return (df_cruzado.loc[~mask].reset_index(drop=True),
            df_cruzado.loc[mask].reset_index(drop=True))


def integrar(
    df_fisica_limpia: pd.DataFrame,
    df_online_limpia: pd.DataFrame,
    df_catalogo: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Orquesta la Fase 4. Devuelve ``(validos, huerfanos, resumen)``."""
    union = fusionar_fuentes(df_fisica_limpia, df_online_limpia)
    union, n_dup = eliminar_duplicados(union)
    cruzado = cruzar_con_catalogo(union, df_catalogo)
    validos, huerfanos = separar_huerfanos(cruzado)
    resumen = {
        "registros_fusionados": len(union) + n_dup,
        "duplicados_pos_eliminados": n_dup,
        "fechas_invalidas": int(union["fecha"].isna().sum()),
        "huerfanos_a_cuarentena": len(huerfanos),
        "registros_validos": len(validos),
    }
    return validos, huerfanos, resumen