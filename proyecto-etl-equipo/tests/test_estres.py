"""
Pruebas de estrés de las fases 6 y 7.

La idea es meterle al pipeline todo lo que puede llegar mal desde las
fuentes (nulos, texto donde va un número, correos rotos, montos de bots,
columnas faltantes, filas que ni siquiera son diccionarios) y comprobar
tres cosas:

  1. Nunca se lanza una excepción hacia afuera.
  2. Cada registro termina en limpios o en cuarentena, ninguno se pierde.
  3. Si la carga a la base falla a la mitad, el rollback deja la base igual.

Se ejecutan con:  pytest -v tests/test_estres.py
"""

import json
import random
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import database
from src.schemas import (  # noqa: E402
    rechazos_a_cuarentena,
    validar_clabe,
    validar_lote,
)

CATALOGO = {f"PROD-{i:03d}" for i in range(1, 51)}


def registro_valido(**cambios):
    base = {
        "id_transaccion": "POS-2026-0001",
        "canal": "TIENDA_FISICA",
        "fecha": "2026-09-15",
        "id_producto": "PROD-010",
        "cantidad": 2,
        "precio_unitario_mxn": 1227.12,
        "monto_total_mxn": 2454.24,
        "moneda": "MXN",
        "metodo_pago": "efectivo",
        "sucursal": "Tecomán",
        "id_cliente": "CTE-100",
        "monto_normalizado": 0.25,
        "segmento_monto": "Medio",
    }
    base.update(cambios)
    return base


def registro_online(**cambios):
    base = registro_valido(
        id_transaccion="WEB-00001",
        canal="ONLINE",
        sucursal=None,
        metodo_pago="PayPal",
        correo_cliente="usuario_1@gmail.com",
    )
    base.update(cambios)
    return base


# ---------------------------------------------------------------------------
# Validación de registros sueltos
# ---------------------------------------------------------------------------

def test_registro_valido_pasa():
    r = validar_lote([registro_valido()], CATALOGO)
    assert len(r.limpios) == 1 and r.cuarentena.empty
    assert r.limpios.loc[0, "metodo_pago"] == "Efectivo"


@pytest.mark.parametrize(
    "cambio, campo",
    [
        ({"cantidad": 0}, "cantidad"),
        ({"cantidad": -3}, "cantidad"),
        ({"cantidad": 500}, "cantidad"),
        ({"cantidad": 2.5}, "cantidad"),
        ({"cantidad": "dos"}, "cantidad"),
        ({"precio_unitario_mxn": -10}, "precio_unitario_mxn"),
        ({"monto_total_mxn": None}, "monto_total_mxn"),
        ({"monto_total_mxn": float("inf")}, "monto_total_mxn"),
        ({"fecha": "13/09/2026"}, "fecha"),
        ({"fecha": "2026-02-30"}, "fecha"),
        ({"fecha": "2099-01-01"}, "fecha"),
        ({"fecha": 1789105020}, "fecha"),
        ({"id_producto": "PROD-976"}, "id_producto"),
        ({"id_producto": "XX-1"}, "id_producto"),
        ({"moneda": "USD"}, "moneda"),
        ({"canal": "MARCIANO"}, "canal"),
        ({"metodo_pago": ""}, "metodo_pago"),
        ({"segmento_monto": "Altisimo"}, "segmento_monto"),
        ({"monto_normalizado": 1.7}, "monto_normalizado"),
        ({"clabe": "123456789012345678"}, "clabe"),
        ({"id_transaccion": None}, "id_transaccion"),
    ],
)
def test_registro_corrupto_va_a_cuarentena(cambio, campo):
    r = validar_lote([registro_valido(**cambio)], CATALOGO)
    assert r.limpios.empty
    assert len(r.cuarentena) == 1
    assert campo in r.cuarentena.loc[0, "campos_con_error"]
    assert r.cuarentena.loc[0, "motivo_rechazo_cuarentena"]


@pytest.mark.parametrize(
    "correo",
    ["correo_invalido.com", "usuario@@gmail..com", "sin_arroba_dominio.org", "a@b", "x@dominio.c"],
)
def test_correos_malformados(correo):
    r = validar_lote([registro_online(correo_cliente=correo)], CATALOGO)
    assert r.limpios.empty
    assert "correo malformado" in r.cuarentena.loc[0, "motivo_rechazo_cuarentena"]


@pytest.mark.parametrize("correo", [None, "", "   ", "ANONIMO", float("nan")])
def test_correo_ausente_o_anonimo_se_acepta(correo):
    r = validar_lote([registro_online(correo_cliente=correo)], CATALOGO)
    assert len(r.limpios) == 1


def test_bot_con_monto_inflado_no_cuadra():
    # WEB-00508 del JSON: 3 piezas de 3443.17 con total de 826,360.80
    r = validar_lote([registro_online(cantidad=3, precio_unitario_mxn=3443.17,
                                      monto_total_mxn=826360.8)], CATALOGO)
    assert r.limpios.empty
    assert "no cuadra" in r.cuarentena.loc[0, "motivo_rechazo_cuarentena"]


def test_tienda_fisica_sin_sucursal():
    r = validar_lote([registro_valido(sucursal=None)], CATALOGO)
    assert "sucursal" in r.cuarentena.loc[0, "motivo_rechazo_cuarentena"]


def test_id_no_corresponde_al_canal():
    r = validar_lote([registro_valido(canal="ONLINE")], CATALOGO)
    assert "no corresponde al canal" in r.cuarentena.loc[0, "motivo_rechazo_cuarentena"]


def test_huerfano_marcado_por_integracion():
    r = validar_lote([registro_valido(es_huerfano=True)])  # sin catálogo
    assert "huérfano" in r.cuarentena.loc[0, "motivo_rechazo_cuarentena"]


def test_varios_errores_en_un_mismo_registro():
    r = validar_lote([registro_valido(cantidad=-1, fecha="ayer", moneda="USD")], CATALOGO)
    campos = r.cuarentena.loc[0, "campos_con_error"]
    assert {"cantidad", "fecha", "moneda"} <= set(campos.split(", "))


def test_id_duplicado_en_el_lote():
    r = validar_lote([registro_valido(), registro_valido()], CATALOGO)
    assert len(r.limpios) == 1 and len(r.cuarentena) == 1
    assert "repetido" in r.cuarentena.loc[0, "motivo_rechazo_cuarentena"]


def test_clabe():
    assert validar_clabe("002010077777777771")
    assert not validar_clabe("002010077777777772")
    assert not validar_clabe("12345")


# ---------------------------------------------------------------------------
# Lotes completos con basura
# ---------------------------------------------------------------------------

def test_lote_con_basura_no_truena():
    basura = [
        None,
        42,
        "texto suelto",
        [],
        {},
        {"id_transaccion": "POS-2026-9999"},
        {"columna_rara": object()},
        registro_valido(id_transaccion="POS-2026-0002"),
    ]
    r = validar_lote(basura, CATALOGO)
    assert len(r.limpios) == 1
    assert len(r.cuarentena) == len(basura) - 1
    # el registro original se guarda como texto, aunque traiga objetos raros
    assert r.cuarentena["registro_original"].str.len().gt(0).all()
    json.loads(r.cuarentena.loc[5, "registro_original"])


def test_dataframe_vacio_y_none():
    for entrada in (pd.DataFrame(), None, []):
        r = validar_lote(entrada, CATALOGO)
        assert r.limpios.empty and r.cuarentena.empty
        assert r.resumen["total_recibidos"] == 0


def test_dataframe_con_nombres_originales():
    """Si la integración deja los nombres del JSON, el alias los traduce."""
    df = pd.DataFrame([{
        "order_id": "WEB-00002", "canal": "ONLINE", "order_timestamp": "2026-10-12",
        "product_id": "PROD-041", "units": 1, "unit_price": 3306.99,
        "total_amount": 3306.99, "currency": "MXN", "customer_email": "u@gmail.com",
        "payment_gateway": "PayPal", "order_status": "completado",
    }]).rename(columns={"product_id": "id_producto"})
    r = validar_lote(df, CATALOGO)
    assert len(r.limpios) == 1
    assert r.limpios.loc[0, "estado_orden"] == "COMPLETADO"


def test_volumen_aleatorio_conserva_todos_los_registros():
    random.seed(7)
    malos = [
        {"cantidad": -1}, {"fecha": "31-12-2026"}, {"correo_cliente": "x@@y"},
        {"id_producto": "PROD-999"}, {"moneda": "USD"}, {"monto_total_mxn": "NaN"},
        {"precio_unitario_mxn": None}, {"cantidad": 10_000},
    ]
    lote = []
    for i in range(20_000):
        r = registro_online(id_transaccion=f"WEB-{i:05d}")
        if random.random() < 0.3:
            r.update(random.choice(malos))
        lote.append(r)
    df = pd.DataFrame(lote)

    r = validar_lote(df, CATALOGO)
    assert len(r.limpios) + len(r.cuarentena) == len(df)
    assert r.cuarentena["motivo_rechazo_cuarentena"].str.len().gt(0).all()
    assert r.limpios["id_transaccion"].is_unique


def test_rechazos_de_otras_fases():
    df = pd.DataFrame([{"id_ticket": "POS-2026-0100", "cantidad": -1}])
    c = rechazos_a_cuarentena(df, "cantidad <= 0", "limpieza")
    assert c.loc[0, "etapa"] == "limpieza"
    assert c.loc[0, "id_origen"] == "POS-2026-0100"


# ---------------------------------------------------------------------------
# Base de datos
# ---------------------------------------------------------------------------

@pytest.fixture
def ruta_db(tmp_path):
    return tmp_path / "prueba.db"


def contar(ruta, tabla):
    with sqlite3.connect(ruta) as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]


def test_carga_exitosa_y_repetible(ruta_db):
    lote = [registro_valido(id_transaccion=f"POS-2026-{i:04d}") for i in range(1, 51)]
    lote.append(registro_valido(id_transaccion="POS-2026-0999", cantidad=-5))
    r = validar_lote(lote, CATALOGO)

    for _ in range(2):  # correr dos veces no debe duplicar
        res = database.cargar_resultados(r.limpios, r.cuarentena, ruta_db)
        assert res.exitosa
    assert contar(ruta_db, "transacciones_limpias") == 50
    assert contar(ruta_db, "cuarentena_errores") == 1
    assert contar(ruta_db, "bitacora_cargas") == 2


def test_rollback_si_falla_a_la_mitad(ruta_db):
    buenos = validar_lote(
        [registro_valido(id_transaccion=f"POS-2026-{i:04d}") for i in range(1, 11)], CATALOGO
    )
    assert database.cargar_resultados(buenos.limpios, buenos.cuarentena, ruta_db).exitosa

    # Segundo lote con una fila que viola el CHECK de la tabla (cantidad 0).
    # Se arma a mano para saltarse Pydantic y forzar el error dentro de SQLite.
    roto = pd.concat([buenos.limpios.assign(id_transaccion=lambda d: d.id_transaccion.str.replace("POS-2026-0", "POS-2026-1"))],
                     ignore_index=True)
    roto.loc[5, "cantidad"] = 0

    res = database.cargar_resultados(roto, buenos.cuarentena, ruta_db)
    assert res.estado == "ROLLBACK"
    assert "CHECK" in res.error or "constraint" in res.error.lower()

    # La base conserva la carga anterior completa, ni una fila del lote roto.
    assert contar(ruta_db, "transacciones_limpias") == 10
    with sqlite3.connect(ruta_db) as conn:
        estados = [e for (e,) in conn.execute("SELECT estado FROM bitacora_cargas ORDER BY id_carga")]
    assert estados == ["COMMIT", "ROLLBACK"]


def test_rollback_por_llave_duplicada(ruta_db):
    r = validar_lote([registro_valido()], CATALOGO)
    duplicado = pd.concat([r.limpios, r.limpios], ignore_index=True)
    res = database.cargar_resultados(duplicado, r.cuarentena, ruta_db)
    assert res.estado == "ROLLBACK"
    assert contar(ruta_db, "transacciones_limpias") == 0


def test_carga_con_dataframes_vacios(ruta_db):
    res = database.cargar_resultados(pd.DataFrame(), pd.DataFrame(), ruta_db)
    assert res.exitosa and res.filas_limpias == 0


def test_ruta_invalida_no_truena(tmp_path):
    res = database.cargar_resultados(pd.DataFrame(), pd.DataFrame(), tmp_path / "no_existe" / "x.db")
    assert res.estado == "ROLLBACK"
    assert res.error
