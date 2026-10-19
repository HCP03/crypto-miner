import asyncio
import os

class WorkerLifecycle:
    def __init__(self, db, rest_api_class, worker_name: str, underlying: str):
        self.db = db
        self.rest_api = rest_api_class
        self.worker_name = worker_name
        self.underlying = underlying
        self.closing = False
        self.tasks = []

    def shutdown_gracefully(self):
        if self.closing:
            return
        self.closing = True
        self.db.log.info("Parada recibida. Vaciando buffers y eliminando .wal...")
        for t in self.tasks:
            if not t.done():
                t.cancel()

    async def _watch_stop_file(self):
        """Vigila la existencia del archivo de parada universal"""
        data_dir = os.getenv("DATA_DIR", "data")
        stop_file = f"{data_dir}/STOP_ALL"
        
        while not self.closing:
            if os.path.exists(stop_file):
                self.shutdown_gracefully()
                break
            await asyncio.sleep(1)

    async def run(self, tasks: list[asyncio.Task]):
        self.tasks = tasks
        watcher_task = asyncio.create_task(self._watch_stop_file())

        try:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            self.shutdown_gracefully()
        finally:
            watcher_task.cancel()
            print(f"[{self.worker_name}] Guardando datos pendientes de {self.underlying} en DuckDB...", flush=True)
            self.db.close()
            if self.rest_api:
                await self.rest_api.close_session()
            print(f"[{self.worker_name}] Detenido correctamente.", flush=True)