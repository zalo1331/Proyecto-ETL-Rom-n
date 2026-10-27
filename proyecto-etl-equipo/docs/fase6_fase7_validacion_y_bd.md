# Fases 6 y 7: validación, cuarentena y base de datos

Responsable: Ingeniero/a 3. Archivos: `src/schemas.py`, `src/database.py`, `tests/test_estres.py`.

Esta parte recibe el DataFrame que sale de `transformacion.py`, revisa cada registro con Pydantic y guarda el resultado en `pipeline_database.db`. Lo que cumple las reglas va a `transacciones_limpias`; lo que no, a `cuarentena_errores` con el motivo escrito en `motivo_rechazo_cuarentena`. El texto de las secciones 3 y 4 se puede copiar tal cual a `REGLAS_DE_NEGOCIO.md`.

## 1. Qué columnas necesito de la fase 5

Para que la validación funcione, el DataFrame transformado debe traer estas columnas con estos nombres. Si alguna llega con el nombre original del CSV o del JSON (`id_ticket`, `order_id`, `units`, `total_amount`, `customer_email`, etc.), `schemas.py` la renombra sola, pero conviene acordar los nombres finales para no depender de eso.

| Columna | Tipo | Obligatoria | Nota |
|---|---|---|---|
| id_transaccion | texto | sí | `POS-AAAA-NNNN` en tienda, `WEB-NNNNN` en línea |
| canal | texto | sí | `TIENDA_FISICA` u `ONLINE` |
| fecha | texto | sí | ya en `AAAA-MM-DD` (fase 4) |
| id_producto | texto | sí | `PROD-NNN` |
| cantidad | entero | sí | después del capping |
| precio_unitario_mxn | real | sí | ya convertido a MXN |
| monto_total_mxn | real | sí | debe ser cantidad × precio, con 1 % de tolerancia |
| moneda | texto | no | si viene, tiene que decir `MXN` |
| metodo_pago | texto | sí | |
| sucursal | texto | sí en tienda física | |
| id_cliente | texto | no | `ANONIMO` si se imputó |
| correo_cliente | texto | no | vacío o `ANONIMO` se aceptan, un correo mal escrito no |
| estado_orden | texto | no | solo ventas en línea |
| es_huerfano | booleano | no | marca de la fase 4 |
| monto_normalizado | real | no | Min-Max en [0, 1] |
| segmento_monto | texto | no | `Bajo`, `Medio` o `Alto` |
| clabe | texto | no | los datos actuales no la traen; el validador queda listo |

Dos acuerdos con el Ingeniero/a 1:

- Cuando la fase 3 recorte la cantidad, hay que recalcular `monto_total_mxn = cantidad × precio_unitario_mxn`. Si se recorta el monto por separado, el total deja de cuadrar y el registro cae en cuarentena por la regla de consistencia.
- Las filas con cantidad ≤ 0 que limpieza descarta no deberían desaparecer. Con `rechazos_a_cuarentena(df_descartadas, "cantidad <= 0", "limpieza")` se guardan en la misma tabla de cuarentena con su etapa, y así se puede contar cuántos registros quitó cada fase.

## 2. Cómo se llama desde main.py

```python
import pandas as pd
from src.schemas import validar_lote, rechazos_a_cuarentena
from src.database import cargar_resultados, resumen_bd

catalogo = pd.read_csv("datasets/catalogo_productos.csv")

resultado = validar_lote(df_transformado, catalogo=catalogo["id_producto"])

cuarentena = pd.concat(
    [resultado.cuarentena,
     rechazos_a_cuarentena(df_imposibles, "cantidad <= 0: valor imposible", "limpieza")],
    ignore_index=True,
)

carga = cargar_resultados(resultado.limpios, cuarentena, df_catalogo=catalogo)
print(resultado.resumen)
print(carga.estado, carga.error)
print(resumen_bd())
```

`validar_lote` acepta un DataFrame o una lista de diccionarios y nunca lanza excepción. `cargar_resultados` tampoco: devuelve un objeto con `estado` (`COMMIT` o `ROLLBACK`) y el error si lo hubo.

## 3. Reglas de validación (fase 6)

Cada registro se valida con el modelo `TransaccionLimpia`. Si un campo falla, Pydantic junta todos los errores del registro y no solo el primero, así que en `motivo_rechazo_cuarentena` puede haber varios motivos separados por `|`, y en `campos_con_error` quedan los nombres de las columnas para poder contar.

| Regla | Por qué |
|---|---|
| `cantidad` entera, mayor que 0 y como máximo 100 | Cero o negativo no es una venta. En la tienda física aparecen tickets de 500 piezas que corresponden a bots o errores de captura; 100 es el tope de una compra al menudeo. Si la fase 3 ya recortó, esta regla casi no rechaza nada y sirve como segunda barrera. |
| `precio_unitario_mxn` y `monto_total_mxn` mayores que 0 y finitos | En el CSV hay montos negativos y nulos. Un NaN o infinito no se puede sumar en un reporte. |
| `monto_total_mxn` ≈ `cantidad × precio_unitario_mxn` (1 %) | El JSON trae compras de bots con totales 80 veces mayores al precio, por ejemplo WEB-00508 con 3 piezas de $3,443.17 y total de $826,360.80. El 1 % absorbe el redondeo de la conversión de dólares. |
| `fecha` en formato `AAAA-MM-DD`, que exista y que esté entre 2020-01-01 y hoy | La fase 4 homologa las fechas; si alguna se le escapa (`13/09/2026`, un timestamp Unix, un 30 de febrero) se detecta aquí. |
| `id_producto` con formato `PROD-NNN` y presente en el catálogo maestro | Hay 60 ventas con productos como PROD-976 que no existen en el catálogo. Sin producto no hay categoría ni proveedor, así que no sirven para análisis. |
| `moneda` igual a MXN | Evita guardar un importe en dólares como si fueran pesos. |
| `correo_cliente` vacío, `ANONIMO` o con formato válido | Vacío es una ausencia legítima (las ventas de tienda no piden correo). Un texto como `correo_invalido.com` o `usuario@@gmail..com` es un dato corrupto. |
| `canal` reconocido y `id_transaccion` con el prefijo de su canal | Un `POS-` marcado como ONLINE indica que la integración mezcló fuentes. |
| `sucursal` obligatoria en tienda física | Una venta física sin sucursal no se puede asignar a ninguna tienda. |
| `segmento_monto` en Bajo/Medio/Alto y `monto_normalizado` en [0, 1] | Confirma que la fase 5 se aplicó bien. |
| `clabe` de 18 dígitos con dígito verificador correcto | El plan lo pide. Los datos actuales no tienen CLABE, pero el validador queda listo para cuando la fuente la incluya. |
| `id_transaccion` sin repetirse en el lote | Los duplicados del POS deberían salir en la fase 1; si alguno queda, el segundo va a cuarentena para no romper la llave primaria de la base. |

Además, `metodo_pago` se normaliza a mayúscula inicial (`efectivo` y `Efectivo` eran dos categorías en el CSV) y `estado_orden` a mayúsculas (`completado` y `COMPLETADO`).

Las ventas canceladas o devueltas no se rechazan, porque son transacciones reales; el análisis decide si las incluye.

### Principio no crash

La validación corre dentro de `try ... except ValidationError`. Hay un segundo `except Exception` para cualquier error que no sea de Pydantic, y una revisión previa para filas que ni siquiera son diccionarios. En todos los casos la fila se va a cuarentena con su motivo y el ciclo sigue con la siguiente. El registro original completo se guarda como JSON en `registro_original`, para poder revisarlo o reprocesarlo después.

## 4. Carga a la base de datos (fase 7)

Se usa SQLite con el módulo estándar `sqlite3`, en modo de transacciones manuales (`isolation_level=None`), para que cada `BEGIN`, `COMMIT` y `ROLLBACK` esté escrito en el código.

Tablas:

- `transacciones_limpias`: llave primaria `id_transaccion` y restricciones `CHECK` (cantidad > 0, montos > 0, moneda = 'MXN', segmento válido, normalizado entre 0 y 1, fecha con forma ISO). Las restricciones repiten algunas reglas de Pydantic a propósito: si alguien inserta datos por fuera del pipeline, la base los rechaza igual.
- `cuarentena_errores`: id autoincremental, id de origen, canal, etapa que lo rechazó, campos con error, `motivo_rechazo_cuarentena`, registro original y fecha.
- `bitacora_cargas`: una fila por ejecución con inicio, fin, filas cargadas y si terminó en COMMIT o ROLLBACK.
- `catalogo_productos`: copia del catálogo maestro, para poder cruzar ventas con categoría y proveedor dentro de la base.

Las dos tablas principales se escriben en una sola transacción. Si un `INSERT` falla (una llave repetida o un valor que viola un `CHECK`), se ejecuta `ROLLBACK` y la base queda como estaba antes de esa carga: no quedan limpias sin su cuarentena ni a medias. La bitácora se escribe fuera de esa transacción, así el intento fallido queda registrado con su mensaje de error.

Por defecto cada ejecución vacía las tablas antes de insertar (`reemplazar=True`), dentro de la misma transacción. Correr el pipeline dos veces deja la misma base, sin duplicados.

## 5. Pruebas

`tests/test_estres.py` tiene 49 casos. Se corren con `pytest -v tests/test_estres.py` y cubren:

- cada regla de validación con un valor que la rompe, y varios errores en el mismo registro;
- los correos malformados que aparecen en `ventas_online.json`;
- un lote con `None`, números, textos sueltos, listas y diccionarios vacíos mezclados con un registro bueno;
- 20,000 registros con 30 % de errores aleatorios, comprobando que limpios + cuarentena suman el total;
- carga exitosa repetida dos veces sin duplicar;
- rollback cuando una fila viola un `CHECK` a la mitad del lote y cuando hay una llave primaria repetida, verificando que la base conserva la carga anterior completa;
- ruta de base inválida, que regresa ROLLBACK con el error en vez de detener el programa.

## 6. Resultado con los datos del proyecto

Con una versión simplificada de las fases 1 a 5 (quitar duplicados exactos, recalcular montos, recortar cantidades por IQR, convertir USD a 19.50 y segmentar por terciles), la validación recibió 1,189 registros: 1,104 pasaron y 85 fueron a cuarentena. De esos 85, 60 son productos huérfanos y 28 tienen correo malformado; tres registros tienen ambos problemas. Las 11 filas de tienda con cantidad ≤ 0 entran a la cuarentena desde la etapa de limpieza. Las cifras finales van a cambiar un poco cuando se conecten los módulos reales de los compañeros.
