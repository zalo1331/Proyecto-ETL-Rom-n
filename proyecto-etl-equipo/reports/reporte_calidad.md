# Informe comparativo de calidad de datos

**Ejecución:** 2026-10-26  
**Fuentes:** `datasets/ventas_tienda_fisica.csv` y `datasets/ventas_online.json`  
**Alcance:** perfilado inicial, imputación, recálculo del total derivado y capping IQR.

## Resultados: ventas de tienda física

### Antes y después: valores nulos

| Columna de origen | Nulos antes | Nulos antes (%) | Nulos después | Nulos después (%) | Tratamiento |
|---|---:|---:|---:|---:|---|
| `monto_total_mxn` → `monto_total` | 32 | 5.16% | 0 | 0.00% | Recalculado como `cantidad * precio` |
| `id_cliente` | 127 | 20.48% | 0 | 0.00% | Imputado con `ANONIMO` |
| Todas las demás columnas | 0 | 0.00% | 0 | 0.00% | Sin imputación |

### Registros y tratamiento

| Métrica | Antes | Después |
|---|---:|---:|
| Registros | 620 | 609 |
| Filas duplicadas exactas detectadas | 20 | 20 |
| Filas duplicadas eliminadas por este proceso | — | 0 |
| Registros con `cantidad <= 0` eliminados | — | 11 |
| Filas válidas ajustadas por capping IQR | — | 13 |
| Celdas numéricas ajustadas por capping IQR | — | 13 |
| Imputaciones numéricas con mediana | — | 0 |
| Imputaciones categóricas con `ANONIMO` | — | 127 |

## Resultados: ventas online

### Antes y después: valores nulos

| Columna | Nulos antes | Nulos antes (%) | Nulos después | Nulos después (%) | Tratamiento |
|---|---:|---:|---:|---:|---|
| `customer_email` | 15 | 2.50% | 0 | 0.00% | Imputado con `ANONIMO` |
| `shipping_address` | 77 | 12.83% | 0 | 0.00% | Imputado con `ANONIMO` |
| Todas las demás columnas | 0 | 0.00% | 0 | 0.00% | Sin imputación |

### Registros y tratamiento

| Métrica | Antes | Después |
|---|---:|---:|
| Registros | 600 | 600 |
| Filas duplicadas exactas detectadas | 0 | 0 |
| Filas duplicadas eliminadas por este proceso | — | 0 |
| Registros con `cantidad <= 0` eliminados | — | 0 |
| Filas válidas ajustadas por capping IQR | — | 0 |
| Celdas numéricas ajustadas por capping IQR | — | 0 |
| Imputaciones numéricas con mediana | — | 0 |
| Imputaciones categóricas con `ANONIMO` | — | 92 |

## Decisiones técnicas y compatibilidad

- El módulo normaliza los nombres de ambas fuentes de ventas: `units` se
  convierte en `cantidad`; `precio_unitario` y `unit_price` se convierten en
  `precio`; y `monto_total_mxn` y `total_amount` se convierten en
  `monto_total`. Si se proporcionan varios alias para un mismo campo, se
  informa una ambigüedad en vez de descartar datos silenciosamente.
- Los faltantes de variables numéricas se imputan con la mediana. En estas
  dos ejecuciones no hubo imputaciones numéricas por mediana; los 32 totales
  faltantes de tienda física se recuperaron al recalcular el campo derivado.
- Los faltantes categóricos se sustituyen por `ANONIMO`. En la tienda física
  se imputaron 127 valores de `id_cliente`; en ventas online, 15 valores de
  `customer_email` y 77 de `shipping_address`.
- `monto_total` se sobrescribe/recalcula como `cantidad * precio`; no se
  conserva un total de origen potencialmente inconsistente en una columna
  aparte.
- Solo se eliminan registros con `cantidad <= 0`. El proceso no elimina
  duplicados exactos: se detectaron 20 en tienda física y 0 en ventas online,
  y no se descartó ninguna fila por duplicidad.
- Para las demás variables numéricas se aplica capping con límites
  `Q1 - 1.5 * IQR` y `Q3 + 1.5 * IQR`, donde `IQR = Q3 - Q1`. Se ajustaron
  13 celdas, correspondientes a 13 filas válidas de tienda física; no hubo
  ajustes en ventas online. `monto_total` se recalcula después del capping y
  no se limita independientemente.
- La limpieza deja 609 registros válidos de tienda física y 600 de ventas
  online. No quedan nulos en las columnas de salida.

## Observaciones

Los duplicados exactos de tienda física permanecen en la salida porque la
regla de eliminación solicitada descarta únicamente cantidades no positivas.
Si el proyecto requiere deduplicación, deberá especificarse como una regla
separada para no eliminar registros válidos de forma implícita.
