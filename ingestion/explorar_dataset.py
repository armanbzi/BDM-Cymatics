import pandas as pd

# Leer el CSV
df = pd.read_csv('meta/esc50.csv')

# Información básica
print("="*50)
print("INFORMACIÓN BÁSICA DEL DATASET")
print("="*50)
print(f"Total de audios: {len(df)}")
print(f"Total de categorías: {df['category'].nunique()}")
print(f"Columnas: {list(df.columns)}")

# Mostrar todas las categorías
print("\n" + "="*50)
print("TODAS LAS CATEGORÍAS:")
print("="*50)
print(df['category'].value_counts())

# Categorías especialmente buenas para cymatics
categorias_cymatics = [
    'rain', 'water_drops', 'sea_waves',
    'church_bells', 'clock_alarm', 'clock_tick',
    'crickets', 'insects', 'thunderstorm', 'wind'
]

# Guardar TODO el dataset (para big data)
df.to_csv('meta/esc50_full.csv', index=False)
print(f"\n Dataset completo guardado: meta/esc50_full.csv ({len(df)} audios)")

# Guardar también la selección cymatics como subconjunto especial
df_cymatics = df[df['category'].isin(categorias_cymatics)]
df_cymatics.to_csv('meta/esc50_cymatics_selection.csv', index=False)
print(f" Selección cymatics guardada: meta/esc50_cymatics_selection.csv ({len(df_cymatics)} audios)")

# Resumen final
print("\n" + "="*50)
print("RESUMEN:")
print("="*50)
print(f"  Total audios (cold path): {len(df)}")
print(f"  Categorías disponibles:   {df['category'].nunique()}")
print(f"  Audios prioritarios:      {len(df_cymatics)} (mejores para cymatics)")
print(f"  Audios adicionales:       {len(df) - len(df_cymatics)} (complementarios)")

