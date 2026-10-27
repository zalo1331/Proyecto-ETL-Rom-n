from pathlib import Path
import pandas as pd

cat = pd.read_csv("datasets/catalogo_productos.csv")
cat["id_producto"] = cat["id_producto"].str.strip().str.upper()

if cat["id_producto"].isna().any() or cat["id_producto"].duplicated().any():
    raise ValueError("El catálogo tiene id_producto nulos o duplicados.")

Path("data").mkdir(exist_ok=True)
cat.to_csv("data/catalogo_maestro.csv", index=False)
print(f"Catálogo maestro generado: {len(cat)} productos")