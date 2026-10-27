"""Herramientas para perfilar la calidad de datos en un DataFrame."""

from collections.abc import Hashable
from typing import Any

import pandas as pd


def perfilado_inicial(df: pd.DataFrame) -> dict[str, Any]:
    """Calcula métricas básicas de calidad sin modificar el DataFrame.

    Args:
        df: DataFrame crudo que se desea inspeccionar.

    Returns:
        Diccionario con el porcentaje de nulos por columna, el número de filas
        duplicadas exactas y los tipos de datos inferidos por Pandas.

    Raises:
        TypeError: Si ``df`` no es una instancia de ``pandas.DataFrame``.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df debe ser una instancia de pandas.DataFrame.")

    porcentaje_nulos: dict[Hashable, float] = (
        df.isna().mean().mul(100).fillna(0.0).to_dict()
    )
    tipos_datos: dict[Hashable, str] = {
        columna: str(tipo) for columna, tipo in df.dtypes.items()
    }

    return {
        "porcentaje_nulos": porcentaje_nulos,
        "filas_duplicadas": int(df.duplicated().sum()),
        "tipos_datos": tipos_datos,
    }
