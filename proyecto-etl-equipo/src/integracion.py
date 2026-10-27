import pandas as pd
import json
import os

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'datasets'))

def load_catalogo() -> pd.DataFrame:
    """Load product catalog CSV into a DataFrame."""
    path = os.path.join(DATA_DIR, 'catalogo_productos.csv')
    return pd.read_csv(path)

def load_ventas_online() -> pd.DataFrame:
    """Load online sales JSON file into a DataFrame."""
    path = os.path.join(DATA_DIR, 'ventas_online.json')
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return pd.json_normalize(data)

def load_ventas_fisica() -> pd.DataFrame:
    """Load physical store sales CSV into a DataFrame."""
    path = os.path.join(DATA_DIR, 'ventas_tienda_fisica.csv')
    return pd.read_csv(path)
