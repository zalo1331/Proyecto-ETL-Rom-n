import pandas as pd
import re

def clean_catalog(df: pd.DataFrame) -> pd.DataFrame:
    """Clean product catalog data.

    - Strip whitespace from string columns
    - Ensure price column is numeric
    - Fill missing values with sensible defaults
    - Drop duplicate product IDs
    """
    df = df.copy()
    # Trim whitespace
    str_cols = df.select_dtypes(include='object').columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())
    # Price to numeric
    if 'precio' in df.columns:
        df['precio'] = pd.to_numeric(df['precio'], errors='coerce')
    # Fill missing values
    df['precio'].fillna(0, inplace=True)
    df['nombre'].fillna('Sin nombre', inplace=True)
    # Drop duplicates based on product ID if present
    id_col = 'producto_id' if 'producto_id' in df.columns else None
    if id_col:
        df.drop_duplicates(subset=id_col, inplace=True)
    return df

def clean_ventas_online(df: pd.DataFrame) -> pd.DataFrame:
    """Clean online sales data.

    - Parse date strings to datetime
    - Ensure numeric columns are numeric
    - Remove records with missing order_id
    """
    df = df.copy()
    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
    numeric_cols = [c for c in df.columns if re.search(r'precio|monto|cantidad', c, re.IGNORECASE)]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(subset=['order_id'], inplace=True)
    return df

def clean_ventas_fisica(df: pd.DataFrame) -> pd.DataFrame:
    """Clean physical store sales data.

    - Standardize column names to snake_case
    - Parse dates, cast numeric fields
    - Remove rows with completely empty values
    """
    df = df.copy()
    # Normalize column names
    df.columns = [re.sub(r'\s+', '_', col.lower()) for col in df.columns]
    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()
    numeric_cols = [c for c in df.columns if re.search(r'precio|monto|cantidad', c, re.IGNORECASE)]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(how='all', inplace=True)
    return df
