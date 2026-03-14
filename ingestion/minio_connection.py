from minio import Minio
from minio.error import S3Error
import os

from dotenv import load_dotenv
load_dotenv()

# MinIO config from .env
_MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
_MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
_MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
_MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"

# Lazy connection: create client when first needed (after env is loaded)
client = None

def _get_client():
    global client
    if client is None:
        if not _MINIO_ACCESS_KEY or not _MINIO_SECRET_KEY:
            raise RuntimeError("Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY in .env")
        client = Minio(
            _MINIO_ENDPOINT,
            access_key=_MINIO_ACCESS_KEY,
            secret_key=_MINIO_SECRET_KEY,
            secure=_MINIO_SECURE,
        )
    return client

# Buckets que necesitamos
BUCKETS = [
    "landing-esc50",
    "landing-urbansound8k", 
    "landing-freesound"
]

def crear_buckets():
    """Crea los buckets si no existen"""
    print("="*50)
    print("CONECTANDO A MINIO")
    print("="*50)
    
    c = _get_client()
    for bucket in BUCKETS:
        try:
            if not c.bucket_exists(bucket):
                c.make_bucket(bucket)
                print(f"  ✅ Bucket creado: {bucket}")
            else:
                print(f"  ℹ️  Bucket ya existe: {bucket}")
        except S3Error as e:
            print(f"  ❌ Error con bucket {bucket}: {e}")

def subir_archivo(bucket, ruta_local, nombre_objeto):
    """Sube un archivo a MinIO"""
    try:
        _get_client().fput_object(bucket, nombre_objeto, ruta_local)
        print(f"  ✅ Subido: {nombre_objeto} → {bucket}")
    except S3Error as e:
        print(f"  ❌ Error subiendo {nombre_objeto}: {e}")

def subir_metadata():
    """Sube los archivos de metadata a MinIO"""
    print("\n" + "="*50)
    print("SUBIENDO METADATA A MINIO")
    print("="*50)
    
    BASE = "C:/Users/100070410/Documents/Q2 MDS/BDM PROJECT"
    
    # Metadata ESC-50
    archivos_esc50 = [
        "esc50.csv",
        "esc50_cymatics_selection.csv"
    ]
    print("\n📁 ESC-50 metadata:")
    for archivo in archivos_esc50:
        ruta = f"{BASE}/landing_zone/esc50/metadata/{archivo}"
        if os.path.exists(ruta):
            subir_archivo("landing-esc50", ruta, f"metadata/{archivo}")
        else:
            print(f"  ❌ No encontrado: {ruta}")

    # Metadata FreeSound
    archivos_freesound = [
        "freesound_metadata.csv",
        "freesound_metadata.json"
    ]
    print("\n FreeSound metadata:")
    for archivo in archivos_freesound:
        ruta = f"{BASE}/landing_zone/freesound/metadata/{archivo}"
        if os.path.exists(ruta):
            subir_archivo("landing-freesound", ruta, f"metadata/{archivo}")
        else:
            print(f"  ❌ No encontrado: {ruta}")

    # Metadata UrbanSound8K
    print("\n UrbanSound8K metadata:")
    ruta_urban = f"{BASE}/UrbanSound8K.csv"
    if os.path.exists(ruta_urban):
        subir_archivo("landing-urbansound8k", ruta_urban, "metadata/UrbanSound8K.csv")
    else:
        print(f"   No encontrado: {ruta_urban}")

# Ejecutar
if __name__ == "__main__":
    crear_buckets()
    subir_metadata()
    print("\n" + "="*50)
    print(" PROCESO COMPLETADO")
    print("="*50)