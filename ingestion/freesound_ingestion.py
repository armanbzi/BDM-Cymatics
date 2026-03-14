import requests
import os
import json
import pandas as pd
import time

from dotenv import load_dotenv
load_dotenv()

# FreeSound API credentials from .env
CLIENT_ID = os.environ.get("FREESOUND_CLIENT_ID", "")
API_KEY = os.environ.get("FREESOUND_API_KEY", "")

# Carpetas de salida
AUDIO_DIR = "freesound_audio"
META_DIR = "freesound_meta"
os.makedirs(AUDIO_DIR, exist_ok=True)
os.makedirs(META_DIR, exist_ok=True)

# Categorías para cymatics
QUERIES = ["rain", "church bells", "crickets", 
           "thunder", "water drops", "sea waves",
           "wind", "singing bowl", "tuning fork"]

def buscar_sonidos(query, num_resultados=10):
    """Busca sonidos en FreeSound por categoría"""
    url = "https://freesound.org/apiv2/search/text/"
    params = {
        "query": query,
        "token": API_KEY,
        "fields": "id,name,duration,previews,tags,description",
        "page_size": num_resultados,
        "filter": "duration:[1 TO 10]"
    }
    response = requests.get(url, params=params)
    return response.json()

def descargar_preview(url_audio, nombre_archivo):
    """Descarga el preview de un sonido"""
    ruta = os.path.join(AUDIO_DIR, nombre_archivo)
    if os.path.exists(ruta):
        print(f"      Ya existe: {nombre_archivo}")
        return ruta
    
    response = requests.get(url_audio)
    if response.status_code == 200:
        with open(ruta, 'wb') as f:
            f.write(response.content)
        print(f"     Descargado: {nombre_archivo}")
        return ruta
    else:
        print(f"     Error al descargar: {nombre_archivo}")
        return None

# ============================================
# INICIO DE LA INGESTA
# ============================================
if not API_KEY or not CLIENT_ID:
    raise RuntimeError(
        "Set FREESOUND_API_KEY and FREESOUND_CLIENT_ID in .env (see .env.example)"
    )
print("="*50)
print("INICIANDO INGESTA DESDE FREESOUND API")
print("="*50)

metadata_total = []

for query in QUERIES:
    print(f"\n🔍 Buscando: '{query}'")
    resultados = buscar_sonidos(query, num_resultados=10)
    
    if "results" not in resultados:
        print(f"   Error en búsqueda: {resultados}")
        continue
    
    print(f"  Encontrados: {resultados['count']} sonidos")
    
    for sonido in resultados["results"]:
        # Nombre limpio para el archivo
        nombre = f"{query.replace(' ', '_')}_{sonido['id']}.mp3"
        
        # Descargar el preview
        url_preview = sonido["previews"]["preview-lq-mp3"]
        ruta = descargar_preview(url_preview, nombre)
        
        # Guardar metadata
        metadata_total.append({
            "id": sonido["id"],
            "nombre": sonido["name"],
            "categoria": query,
            "duracion": sonido["duration"],
            "archivo": nombre,
            "ruta": ruta,
            "tags": str(sonido.get("tags", [])),
            "fuente": "freesound_api"
        })
        
        time.sleep(0.3)  # Respetar límites de la API

# Guardar metadata en CSV y JSON
df_meta = pd.DataFrame(metadata_total)
df_meta.to_csv(os.path.join(META_DIR, "freesound_metadata.csv"), index=False)

with open(os.path.join(META_DIR, "freesound_metadata.json"), "w") as f:
    json.dump(metadata_total, f, indent=2)

print("\n" + "="*50)
print("RESUMEN FINAL:")
print("="*50)
print(f"   Total sonidos descargados: {len(metadata_total)}")
print(f"   CSV guardado: freesound_meta/freesound_metadata.csv")
print(f"   JSON guardado: freesound_meta/freesound_metadata.json")
print(f"   Audios en: freesound_audio/")
