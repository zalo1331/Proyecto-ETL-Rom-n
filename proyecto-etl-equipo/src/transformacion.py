"""Escalamiento Min-Max y discretización del importe (Fase 5)."""

from __future__ import annotations

import pandas as pd

ETIQUETAS = ["Bajo", "Medio", "Alto"]


def escalar_min_max(
    df: pd.DataFrame, columna: str = "monto_total", salida: str = "monto_total_norm"
) -> pd.DataFrame:
    """Escala ``columna`` a [0, 1]: (x - min) / (max - min).

    Si todos los valores son iguales (max == min) devuelve 0.0 para evitar
    división entre cero.
    """
    if columna not in df.columns:
        raise ValueError(f"Falta la columna '{columna}'.")
    out = df.copy()
    minimo, maximo = out[columna].min(), out[columna].max()
    rango = maximo - minimo
    out[salida] = 0.0 if (pd.isna(rango) or rango == 0) else (out[columna] - minimo) / rango
    return out


def discretizar_monto(
    df: pd.DataFrame,
    columna: str = "monto_total",
    salida: str = "segmento_monto",
    umbrales: tuple[float, float] | None = None,
) -> pd.DataFrame:
    """Segmenta el importe en Bajo / Medio / Alto.

    - ``umbrales=None``: terciles (cuantiles 1/3 y 2/3) -> grupos balanceados.
    - ``umbrales=(a, b)``: regla de negocio: Bajo <= a < Medio <= b < Alto.
    """
    if columna not in df.columns:
        raise ValueError(f"Falta la columna '{columna}'.")
    out = df.copy()
    if umbrales is None:
        a, b = out[columna].quantile([1 / 3, 2 / 3])
    else:
        a, b = umbrales
        if a >= b:
            raise ValueError("Los umbrales deben cumplir a < b.")
    bordes = [-float("inf"), a, b, float("inf")]
    if not (bordes[1] < bordes[2]):  # distribución degenerada
        out[salida] = pd.Series("Medio", index=out.index, dtype="string")
        return out
    out[salida] = pd.cut(out[columna], bins=bordes, labels=ETIQUETAS,
                         include_lowest=True).astype("string")
    return out


def transformar(df: pd.DataFrame, umbrales: tuple[float, float] | None = None) -> pd.DataFrame:
    """Aplica escalamiento Min-Max y discretización sobre ``monto_total``."""
    return discretizar_monto(escalar_min_max(df), umbrales=umbrales)