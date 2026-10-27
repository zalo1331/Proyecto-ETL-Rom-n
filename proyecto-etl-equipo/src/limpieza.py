"""Funciones de imputación de faltantes y tratamiento estadístico de outliers."""

import numpy as np
import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype


def _validar_columnas_venta(df: pd.DataFrame) -> None:
    """Valida las columnas necesarias para calcular el monto total."""
    columnas_requeridas = {"cantidad", "precio"}
    faltantes = columnas_requeridas.difference(df.columns)
    if faltantes:
        nombres = ", ".join(sorted(faltantes))
        raise ValueError(f"Faltan columnas requeridas para la limpieza: {nombres}.")

    for columna in columnas_requeridas:
        tipo = df[columna].dtype
        if not is_numeric_dtype(tipo) or is_bool_dtype(tipo):
            raise TypeError(f"La columna '{columna}' debe tener un tipo numérico.")


def _normalizar_columnas_venta(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra alias conocidos a los nombres canónicos de las ventas."""
    alias_por_campo = {
        "cantidad": ("cantidad", "units"),
        "precio": ("precio", "precio_unitario", "unit_price"),
        "monto_total": ("monto_total", "monto_total_mxn", "total_amount"),
    }
    renombres: dict[str, str] = {}

    for canonico, alias in alias_por_campo.items():
        presentes = [columna for columna in alias if columna in df.columns]
        if len(presentes) > 1:
            nombres = ", ".join(presentes)
            raise ValueError(
                f"Columnas ambiguas para '{canonico}': {nombres}. "
                "Conserve solo una columna equivalente."
            )
        if presentes and presentes[0] != canonico:
            renombres[presentes[0]] = canonico

    return df.rename(columns=renombres).copy()


def _recalcular_monto_total(df: pd.DataFrame) -> None:
    """Actualiza el campo derivado a partir de cantidad y precio."""
    df["monto_total"] = df["cantidad"] * df["precio"]


def imputar_faltantes(df: pd.DataFrame) -> pd.DataFrame:
    """Imputa nulos numéricos con la mediana y categóricos con ``ANONIMO``.

    El campo ``monto_total`` se calcula de nuevo como ``cantidad * precio``.
    El DataFrame de entrada se conserva sin modificaciones.

    Args:
        df: DataFrame con las columnas ``cantidad`` y ``precio``.

    Returns:
        Una copia del DataFrame con los faltantes imputados.

    Raises:
        TypeError: Si ``df`` no es un DataFrame o las columnas de venta no son
            numéricas.
        ValueError: Si falta una columna requerida o una columna numérica
            completamente vacía no tiene mediana calculable.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df debe ser una instancia de pandas.DataFrame.")
    _validar_columnas_venta(df)

    limpio = df.copy()
    columnas_numericas = limpio.select_dtypes(include=[np.number]).columns
    for columna in columnas_numericas:
        if columna == "monto_total":
            continue
        if limpio[columna].isna().any():
            mediana = limpio[columna].median()
            if pd.isna(mediana):
                raise ValueError(
                    f"No se puede imputar '{columna}': no tiene valores "
                    "numéricos para calcular la mediana."
                )
            limpio[columna] = limpio[columna].fillna(mediana)

    columnas_categoricas = limpio.select_dtypes(
        include=["object", "category", "string", "bool", "boolean"]
    ).columns
    for columna in columnas_categoricas:
        serie = limpio[columna]
        if not serie.isna().any():
            continue
        if isinstance(serie.dtype, pd.CategoricalDtype):
            if "ANONIMO" not in serie.cat.categories:
                serie = serie.cat.add_categories(["ANONIMO"])
            limpio[columna] = serie.fillna("ANONIMO")
        else:
            limpio[columna] = serie.fillna("ANONIMO")

    _recalcular_monto_total(limpio)
    return limpio


def tratar_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina cantidades no positivas y aplica capping IQR al resto.

    Solo se descartan filas cuya ``cantidad`` sea menor o igual a cero. Las
    demás columnas numéricas se recortan a los límites Q1 - 1.5*IQR y
    Q3 + 1.5*IQR, sin eliminar filas. ``monto_total`` no se limita por
    separado porque es derivado; se vuelve a calcular después del capping.

    Args:
        df: DataFrame con las columnas numéricas ``cantidad`` y ``precio``.

    Returns:
        Una copia del DataFrame sin registros de cantidad no positiva y con
        los valores numéricos recortados por IQR.

    Raises:
        TypeError: Si ``df`` no es un DataFrame o las columnas de venta no son
            numéricas.
        ValueError: Si falta una columna requerida.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df debe ser una instancia de pandas.DataFrame.")
    _validar_columnas_venta(df)

    limpio = df.copy()
    cantidades_no_validas = limpio["cantidad"].le(0).fillna(False)
    limpio = limpio.loc[~cantidades_no_validas].copy()

    columnas_numericas = limpio.select_dtypes(include=[np.number]).columns
    for columna in columnas_numericas:
        if columna == "monto_total":
            continue

        q1 = limpio[columna].quantile(0.25)
        q3 = limpio[columna].quantile(0.75)
        if pd.isna(q1) or pd.isna(q3):
            continue

        rango_intercuartil = q3 - q1
        limite_inferior = q1 - 1.5 * rango_intercuartil
        limite_superior = q3 + 1.5 * rango_intercuartil
        limpio[columna] = limpio[columna].clip(
            lower=limite_inferior,
            upper=limite_superior,
        )

    limpio["cantidad"] = np.floor(limpio["cantidad"]).clip(lower=1).astype("int64")
    _recalcular_monto_total(limpio)
    return limpio


def limpiar_datos(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza alias de ventas y ejecuta imputación y tratamiento de outliers.

    Args:
        df: DataFrame de ventas. ``cantidad`` también puede llamarse ``units``;
            ``precio`` puede llamarse ``precio_unitario`` o ``unit_price``.
            El total puede llamarse ``monto_total_mxn`` o ``total_amount``.

    Returns:
        DataFrame limpio con las columnas canónicas ``cantidad``, ``precio``
        y ``monto_total``. El total siempre se recalcula como cantidad por
        precio y el DataFrame de entrada no se modifica.

    Raises:
        TypeError: Si el argumento o las columnas requeridas tienen tipos
            incompatibles.
        ValueError: Si faltan columnas requeridas, existen alias ambiguos o no
            se puede calcular una mediana para una columna numérica vacía.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df debe ser una instancia de pandas.DataFrame.")

    normalizado = _normalizar_columnas_venta(df)
    return tratar_outliers(imputar_faltantes(normalizado))
