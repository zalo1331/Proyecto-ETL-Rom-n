"""
Fase 6: validación con Pydantic y zona de cuarentena.

Este módulo recibe el DataFrame que sale de transformacion.py (ya unificado,
con fechas en ISO y montos en MXN) y revisa registro por registro que cumpla
las reglas de negocio. Lo que pasa la validación queda listo para la tabla
transacciones_limpias; lo que no, se manda a cuarentena con el motivo del
rechazo. Un registro malo nunca detiene el lote.

Uso típico desde main.py:

    from src.schemas import validar_lote
    resultado = validar_lote(df_transformado, catalogo=ids_catalogo)
    resultado.limpios      # DataFrame conforme
    resultado.cuarentena   # DataFrame con motivo_rechazo_cuarentena
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable, Literal, Optional

import pandas as pd
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)

# ---------------------------------------------------------------------------
# Parámetros de negocio. Están aquí arriba para poder explicarlos y cambiarlos
# sin buscar dentro del código.
# ---------------------------------------------------------------------------

FECHA_MINIMA = date(2020, 1, 1)

# Una venta al menudeo con más de 100 piezas del mismo artículo no es normal
# en esta cadena. En los datos crudos aparecen tickets de 500 unidades que
# corresponden a bots o errores de captura. Si la fase 3 ya recortó outliers,
# esta regla casi no rechaza nada; queda como segunda barrera.
CANTIDAD_MAXIMA = 100

# Diferencia permitida entre monto_total y cantidad * precio_unitario.
# El 1 % cubre redondeos de la conversión USD -> MXN.
TOLERANCIA_MONTO = 0.01
VALIDAR_CONSISTENCIA_MONTO = True

SEGMENTOS_VALIDOS = ("Bajo", "Medio", "Alto")
VALORES_ANONIMOS = {"ANONIMO", "SIN_CORREO", "NO_REGISTRADO"}

PATRON_ID_PRODUCTO = re.compile(r"^PROD-\d{3}$")
PATRON_ID_TRANSACCION = re.compile(r"^(POS-\d{4}-\d{4}|WEB-\d{5})$")
PATRON_FECHA_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# Usuario, una sola arroba, dominio con al menos un punto y sin puntos dobles.
PATRON_CORREO = re.compile(
    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$"
)

# Nombres que pueden traer las columnas según quién las haya generado.
# Si la integración ya usa los nombres finales, este mapa no hace nada.
ALIAS_COLUMNAS = {
    "id_ticket": "id_transaccion",
    "order_id": "id_transaccion",
    "fecha_venta": "fecha",
    "order_timestamp": "fecha",
    "product_id": "id_producto",
    "units": "cantidad",
    "unit_price": "precio_unitario_mxn",
    "precio_unitario": "precio_unitario_mxn",
    "precio": "precio_unitario_mxn",
    "total_amount": "monto_total_mxn",
    "monto_total": "monto_total_mxn",
    "customer_email": "correo_cliente",
    "payment_gateway": "metodo_pago",
    "order_status": "estado_orden",
    "currency": "moneda",
    "monto_minmax": "monto_normalizado",
    "segmento": "segmento_monto",
    "huerfano": "es_huerfano",
}

CANALES = {
    "TIENDA_FISICA": "TIENDA_FISICA",
    "FISICA": "TIENDA_FISICA",
    "FISICO": "TIENDA_FISICA",
    "POS": "TIENDA_FISICA",
    "ONLINE": "ONLINE",
    "WEB": "ONLINE",
    "DIGITAL": "ONLINE",
    "ECOMMERCE": "ONLINE",
}


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def _es_vacio(valor: Any) -> bool:
    """True para None, NaN, NaT, pd.NA y cadenas vacías."""
    if valor is None:
        return True
    if isinstance(valor, str):
        return valor.strip() == ""
    try:
        return bool(pd.isna(valor))
    except (TypeError, ValueError):
        return False


def _a_nativo(valor: Any) -> Any:
    """Convierte tipos de numpy/pandas a tipos de Python para Pydantic."""
    if _es_vacio(valor):
        return None
    if hasattr(valor, "item") and not isinstance(valor, (str, bytes)):
        try:
            return valor.item()
        except (ValueError, AttributeError):
            pass
    if isinstance(valor, pd.Timestamp):
        return valor.date().isoformat()
    return valor


def validar_clabe(clabe: str) -> bool:
    """
    Revisa una CLABE interbancaria de 18 dígitos con su dígito verificador.

    Los primeros 17 dígitos se multiplican por los pesos 3, 7, 1 en ciclo,
    se suma el módulo 10 de cada producto y el verificador es
    (10 - suma % 10) % 10.
    """
    if not isinstance(clabe, str) or not re.fullmatch(r"\d{18}", clabe):
        return False
    pesos = (3, 7, 1)
    suma = sum((int(d) * pesos[i % 3]) % 10 for i, d in enumerate(clabe[:17]))
    return (10 - suma % 10) % 10 == int(clabe[17])


# ---------------------------------------------------------------------------
# Esquema principal
# ---------------------------------------------------------------------------

class TransaccionLimpia(BaseModel):
    """Una venta (física u online) que puede entrar a transacciones_limpias."""

    model_config = ConfigDict(
        str_strip_whitespace=True,
        extra="ignore",
        validate_default=True,
    )

    id_transaccion: str = Field(min_length=1)
    canal: Literal["TIENDA_FISICA", "ONLINE"]
    fecha: str
    id_producto: str
    cantidad: int = Field(gt=0, le=CANTIDAD_MAXIMA)
    precio_unitario_mxn: float = Field(gt=0)
    monto_total_mxn: float = Field(gt=0)
    moneda: Literal["MXN"] = "MXN"
    metodo_pago: str = Field(min_length=1)
    sucursal: Optional[str] = None
    id_cliente: Optional[str] = None
    correo_cliente: Optional[str] = None
    estado_orden: Optional[str] = None
    clabe: Optional[str] = None
    monto_normalizado: Optional[float] = Field(default=None, ge=0, le=1)
    segmento_monto: Optional[Literal["Bajo", "Medio", "Alto"]] = None
    es_huerfano: bool = False

    # --- validadores de campo -------------------------------------------

    @field_validator("id_transaccion")
    @classmethod
    def _id_transaccion(cls, v: str) -> str:
        v = v.upper()
        if not PATRON_ID_TRANSACCION.match(v):
            raise ValueError(
                f"formato de id no reconocido '{v}' "
                "(se espera POS-AAAA-NNNN o WEB-NNNNN)"
            )
        return v

    @field_validator("canal", mode="before")
    @classmethod
    def _canal(cls, v: Any) -> Any:
        if isinstance(v, str):
            clave = v.strip().upper().replace(" ", "_").replace("-", "_")
            return CANALES.get(clave, clave)
        return v

    @field_validator("fecha", mode="before")
    @classmethod
    def _fecha(cls, v: Any) -> str:
        if isinstance(v, (datetime, pd.Timestamp)):
            v = v.date()
        if isinstance(v, date):
            v = v.isoformat()
        if not isinstance(v, str):
            raise ValueError(f"la fecha debe ser texto ISO, llegó {type(v).__name__}")
        v = v.strip()[:10]
        if not PATRON_FECHA_ISO.match(v):
            raise ValueError(f"fecha '{v}' no está en formato AAAA-MM-DD")
        try:
            f = date.fromisoformat(v)
        except ValueError:
            raise ValueError(f"fecha '{v}' no existe en el calendario")
        if f < FECHA_MINIMA or f > date.today():
            raise ValueError(f"fecha {v} fuera del rango permitido")
        return v

    @field_validator("id_producto")
    @classmethod
    def _id_producto(cls, v: str, info: ValidationInfo) -> str:
        v = v.upper()
        if not PATRON_ID_PRODUCTO.match(v):
            raise ValueError(f"id de producto con formato inválido '{v}'")
        catalogo = (info.context or {}).get("catalogo")
        if catalogo is not None and v not in catalogo:
            raise ValueError(f"producto huérfano: {v} no existe en el catálogo maestro")
        return v

    @field_validator("cantidad", mode="before")
    @classmethod
    def _cantidad(cls, v: Any) -> Any:
        # Pydantic acepta 2.0 como 2, pero 2.5 piezas no tiene sentido.
        if isinstance(v, float) and not v.is_integer():
            raise ValueError(f"la cantidad debe ser entera, llegó {v}")
        return v

    @field_validator("precio_unitario_mxn", "monto_total_mxn", "monto_normalizado")
    @classmethod
    def _numero_finito(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and (math.isnan(v) or math.isinf(v)):
            raise ValueError("el valor no es un número finito")
        return v

    @field_validator("moneda", mode="before")
    @classmethod
    def _moneda(cls, v: Any) -> Any:
        if v is None:
            return "MXN"
        if isinstance(v, str) and v.strip().upper() != "MXN":
            raise ValueError(
                f"el importe sigue en {v.strip().upper()}; debe convertirse a MXN antes de validar"
            )
        return "MXN" if isinstance(v, str) else v

    @field_validator("metodo_pago")
    @classmethod
    def _metodo_pago(cls, v: str) -> str:
        # "efectivo", "EFECTIVO" y "Efectivo" son el mismo método.
        return v if any(c.isupper() for c in v[1:]) else v.capitalize()

    @field_validator("correo_cliente", mode="before")
    @classmethod
    def _correo(cls, v: Any) -> Optional[str]:
        if _es_vacio(v):
            return None
        if not isinstance(v, str):
            raise ValueError("el correo debe ser texto")
        v = v.strip()
        if v.upper() in VALORES_ANONIMOS:
            return v.upper()
        v = v.lower()
        if ".." in v or not PATRON_CORREO.match(v):
            raise ValueError(f"correo malformado '{v}'")
        return v

    @field_validator("clabe", mode="before")
    @classmethod
    def _clabe(cls, v: Any) -> Optional[str]:
        if _es_vacio(v):
            return None
        v = str(v).strip().replace(" ", "")
        if not validar_clabe(v):
            raise ValueError("CLABE inválida (deben ser 18 dígitos con verificador correcto)")
        return v

    @field_validator("estado_orden")
    @classmethod
    def _estado(cls, v: Optional[str]) -> Optional[str]:
        return v.upper() if v else v

    @field_validator("segmento_monto", mode="before")
    @classmethod
    def _segmento(cls, v: Any) -> Any:
        return v.strip().capitalize() if isinstance(v, str) else v

    @field_validator("es_huerfano", mode="before")
    @classmethod
    def _huerfano(cls, v: Any) -> bool:
        if _es_vacio(v):
            return False
        if isinstance(v, str):
            return v.strip().lower() in {"true", "1", "si", "sí", "yes"}
        return bool(v)

    # --- reglas que involucran varios campos --------------------------------

    @model_validator(mode="after")
    def _reglas_cruzadas(self) -> "TransaccionLimpia":
        if self.es_huerfano:
            raise ValueError(
                f"producto huérfano: {self.id_producto} no existe en el catálogo maestro"
            )

        if self.canal == "TIENDA_FISICA" and not self.sucursal:
            raise ValueError("una venta de tienda física debe indicar la sucursal")

        prefijo = "POS-" if self.canal == "TIENDA_FISICA" else "WEB-"
        if not self.id_transaccion.startswith(prefijo):
            raise ValueError(
                f"el id {self.id_transaccion} no corresponde al canal {self.canal}"
            )

        if VALIDAR_CONSISTENCIA_MONTO:
            esperado = self.cantidad * self.precio_unitario_mxn
            if abs(self.monto_total_mxn - esperado) > esperado * TOLERANCIA_MONTO:
                raise ValueError(
                    f"monto_total_mxn {self.monto_total_mxn:.2f} no cuadra con "
                    f"cantidad x precio = {esperado:.2f}"
                )
        return self


# ---------------------------------------------------------------------------
# Esquema de la cuarentena
# ---------------------------------------------------------------------------

class RegistroCuarentena(BaseModel):
    """Lo que se guarda de un registro rechazado."""

    id_origen: Optional[str] = None
    canal: Optional[str] = None
    etapa: str = "validacion_pydantic"
    campos_con_error: str = ""
    motivo_rechazo_cuarentena: str
    registro_original: str
    fecha_rechazo: str = Field(
        default_factory=lambda: datetime.now().isoformat(timespec="seconds")
    )


@dataclass
class ResultadoValidacion:
    limpios: pd.DataFrame
    cuarentena: pd.DataFrame
    resumen: dict = field(default_factory=dict)


COLUMNAS_LIMPIAS = list(TransaccionLimpia.model_fields.keys())
COLUMNAS_CUARENTENA = list(RegistroCuarentena.model_fields.keys())


# ---------------------------------------------------------------------------
# Funciones públicas
# ---------------------------------------------------------------------------

def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas conocidas al nombre que espera el esquema."""
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    renombres = {
        viejo: nuevo
        for viejo, nuevo in ALIAS_COLUMNAS.items()
        if viejo in df.columns and nuevo not in df.columns
    }
    return df.rename(columns=renombres)


def _formatear_errores(error: ValidationError) -> tuple[str, str]:
    """Devuelve (campos, mensaje) legibles a partir de un ValidationError."""
    campos, mensajes = [], []
    for e in error.errors():
        campo = ".".join(str(p) for p in e.get("loc", ())) or "registro"
        msg = e.get("msg", "").removeprefix("Value error, ")
        if e.get("type") == "missing":
            msg = "campo obligatorio ausente"
        elif "input" in e and e.get("type") not in ("value_error",):
            msg = f"{msg} (valor recibido: {e['input']!r})"
        campos.append(campo)
        mensajes.append(f"{campo}: {msg}")
    return ", ".join(dict.fromkeys(campos)), " | ".join(mensajes)


def _serializar(registro: Any) -> str:
    try:
        return json.dumps(registro, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return repr(registro)


def _registro_a_cuarentena(
    registro: Any, motivo: str, campos: str = "", etapa: str = "validacion_pydantic"
) -> dict:
    datos = registro if isinstance(registro, dict) else {}
    canal = datos.get("canal")
    return RegistroCuarentena(
        id_origen=None if _es_vacio(datos.get("id_transaccion")) else str(datos.get("id_transaccion")),
        canal=None if _es_vacio(canal) else str(canal),
        etapa=etapa,
        campos_con_error=campos,
        motivo_rechazo_cuarentena=motivo,
        registro_original=_serializar(registro),
    ).model_dump()


def validar_registro(registro: dict, catalogo: Optional[set] = None) -> TransaccionLimpia:
    """Valida un solo registro. Lanza ValidationError si no cumple."""
    return TransaccionLimpia.model_validate(registro, context={"catalogo": catalogo})


def validar_lote(
    datos: pd.DataFrame | Iterable[dict],
    catalogo: Optional[Iterable[str]] = None,
) -> ResultadoValidacion:
    """
    Separa un lote en registros conformes y registros en cuarentena.

    datos     DataFrame o lista de diccionarios ya transformados.
    catalogo  ids de producto válidos. Si se pasa, también se detectan
              huérfanos aquí, aunque la integración no los haya marcado.
    """
    catalogo_set = {str(c).strip().upper() for c in catalogo} if catalogo is not None else None

    if isinstance(datos, pd.DataFrame):
        registros = normalizar_columnas(datos).to_dict(orient="records")
    elif datos is None:
        registros = []
    else:
        registros = list(datos)

    limpios: list[dict] = []
    cuarentena: list[dict] = []
    vistos: set[str] = set()

    for crudo in registros:
        if not isinstance(crudo, dict):
            cuarentena.append(_registro_a_cuarentena(
                crudo, f"el registro no es un diccionario sino {type(crudo).__name__}", "registro"
            ))
            continue

        registro = {str(k).strip().lower(): _a_nativo(v) for k, v in crudo.items()}
        # Igual que normalizar_columnas: si ya viene el nombre final, gana ese.
        for viejo, nuevo in ALIAS_COLUMNAS.items():
            if viejo in registro and nuevo not in registro:
                registro[nuevo] = registro.pop(viejo)

        try:
            valido = validar_registro(registro, catalogo_set)
        except ValidationError as error:
            campos, motivo = _formatear_errores(error)
            cuarentena.append(_registro_a_cuarentena(registro, motivo, campos))
            continue
        except Exception as error:  # cualquier sorpresa va a cuarentena, no truena el lote
            cuarentena.append(_registro_a_cuarentena(
                registro, f"error inesperado: {type(error).__name__}: {error}", "registro"
            ))
            continue

        if valido.id_transaccion in vistos:
            cuarentena.append(_registro_a_cuarentena(
                registro,
                f"id_transaccion: {valido.id_transaccion} repetido dentro del lote",
                "id_transaccion",
            ))
            continue

        vistos.add(valido.id_transaccion)
        limpios.append(valido.model_dump())

    df_limpios = pd.DataFrame(limpios, columns=COLUMNAS_LIMPIAS)
    df_cuarentena = pd.DataFrame(cuarentena, columns=COLUMNAS_CUARENTENA)

    total = len(registros)
    resumen = {
        "total_recibidos": total,
        "conformes": len(df_limpios),
        "en_cuarentena": len(df_cuarentena),
        "porcentaje_rechazo": round(100 * len(df_cuarentena) / total, 2) if total else 0.0,
        "rechazos_por_campo": _contar_campos(df_cuarentena),
    }
    return ResultadoValidacion(df_limpios, df_cuarentena, resumen)


def rechazos_a_cuarentena(df: pd.DataFrame, motivo: str, etapa: str) -> pd.DataFrame:
    """
    Da formato de cuarentena a filas que otra fase decidió descartar.

    Sirve para que limpieza.py (cantidades <= 0) o integracion.py (huérfanos)
    no tiren esas filas, sino que las manden a la misma tabla con su motivo.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=COLUMNAS_CUARENTENA)
    filas = normalizar_columnas(df).to_dict(orient="records")
    filas = [{k: _a_nativo(v) for k, v in f.items()} for f in filas]
    return pd.DataFrame(
        [_registro_a_cuarentena(f, motivo, "", etapa) for f in filas],
        columns=COLUMNAS_CUARENTENA,
    )


def _contar_campos(df_cuarentena: pd.DataFrame) -> dict:
    if df_cuarentena.empty:
        return {}
    conteo = (
        df_cuarentena["campos_con_error"]
        .fillna("")
        .str.split(", ")
        .explode()
        .replace("", "sin_campo")
        .value_counts()
    )
    return {str(k): int(v) for k, v in conteo.items()}
