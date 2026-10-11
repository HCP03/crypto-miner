import aiohttp
import asyncio
import time
import zipfile
from pathlib import Path

class RestClient:
    _session = None
    _semaphore = asyncio.Semaphore(15)
    _used_weight = 0
    _weight_limit = 1200
    _cooldown_global = 0.0  

    @classmethod
    async def get_session(cls):
        if cls._session is None or cls._session.closed:
            cls._session = aiohttp.ClientSession()
        return cls._session

    @classmethod
    async def close_session(cls):
        if cls._session and not cls._session.closed:
            await cls._session.close()

    @classmethod
    async def get(cls, url: str, params: dict = None):
        session = await cls.get_session()

        async with cls._semaphore:
            now = time.time()
            if now < cls._cooldown_global:
                await asyncio.sleep(cls._cooldown_global - now)

            if cls._used_weight > (cls._weight_limit * 0.9):
                print(f"[Preventivo] Peso al {cls._used_weight}. Pausando 5s")
                await asyncio.sleep(5)
                cls._used_weight = 0
                
            while True:
                now = time.time()
                if now < cls._cooldown_global:
                    await asyncio.sleep(cls._cooldown_global - now)

                async with session.get(url, params=params) as response:
                    for header, value in response.headers.items():
                        if header.lower().startswith('x-mbx-used-weight'):
                            cls._used_weight = int(value)
                            break

                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 10))
                        print(f"[429] Rate limit excedido. Pausa GLOBAL de {retry_after}s...")
                        cls._cooldown_global = time.time() + retry_after
                        continue

                    if response.status == 418:
                        retry_after = int(response.headers.get("Retry-After", 120))
                        print(f"[418] IP Baneada. Pausa GLOBAL de {retry_after}s...")
                        cls._cooldown_global = time.time() + retry_after
                        continue

                    response.raise_for_status() 
                    return await response.json()

    # ==========================================================
    #        FUNCIONES GENÉRICAS PARA ARCHIVOS Y ZIPS
    # ==========================================================
    @classmethod
    async def download_zip(cls, url: str, dest_zip_path: Path) -> bool:
        """Descarga un archivo ZIP en streaming de forma asíncrona."""
        session = await cls.get_session()
        dest_zip_path.parent.mkdir(parents=True, exist_ok=True)

        async with cls._semaphore:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status == 404:
                        return False
                    response.raise_for_status()

                    with open(dest_zip_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(65536): # Chunks de 64KB
                            f.write(chunk)
                return True
            except Exception as e:
                print(f"   ⚠️ Error descargando ZIP ({url}): {e}")
                cls.remove_file(dest_zip_path)
                return False

    @classmethod
    def unzip_and_remove(cls, zip_path: Path, final_csv_path: Path) -> Path | None:
        """Descomprime el CSV del ZIP, lo renombra al formato deseado y elimina el ZIP."""
        if not zip_path.exists():
            return None

        target_dir = final_csv_path.parent
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
                target = csv_files[0] if csv_files else zip_ref.namelist()[0]
                
                zip_ref.extract(target, target_dir)
                extracted_path = target_dir / target
                extracted_path.replace(final_csv_path)

            cls.remove_file(zip_path)
            return final_csv_path
        except Exception as e:
            print(f"   ⚠️ Error descomprimiendo {zip_path}: {e}")
            cls.remove_file(zip_path)
            return None

    @classmethod
    def remove_file(cls, file_path: Path) -> bool:
        """Elimina un archivo de forma segura (ej. el CSV tras ser ingestado en DB)."""
        try:
            if file_path and file_path.exists():
                file_path.unlink()
                return True
            return False
        except Exception as e:
            print(f"   ⚠️ Error eliminando archivo {file_path}: {e}")
            return False