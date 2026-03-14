import pandas as pd
import os

# Ruta correcta al CSV
BASE_DIR = r"C:\Users\100070410\Documents\Q2 MDS\BDM PROJECT"
CSV_PATH = os.path.join(BASE_DIR, "UrbanSound8K.csv")

# Leer el CSV
df = pd.read_csv(CSV_PATH)

# Información básica
print("="*50)
print("INFORMACIÓN BÁSICA - URBANSOUND8K")
print("="*50)
print(f"Total de audios: {len(df)}")
print(f"Columnas: {list(df.columns)}")
print(f"Total de categorías: {df['class'].nunique()}")

# Ver categorías
print("\n" + "="*50)
print("CATEGORÍAS DISPONIBLES:")
print("="*50)
print(df['class'].value_counts())

# Primeras filas
print("\n" + "="*50)
print("PRIMERAS 5 FILAS:")
print("="*50)
print(df.head())

