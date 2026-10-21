import asyncio
import sys
import os
import signal
import argparse
import yaml
from datetime import datetime
import duckdb

WORKERS_CONFIG = {
    "FUTURES": "workers.futures_worker",
    "SPOT": "workers.spot_worker",
    "OPTIONS": "workers.options_worker",
    "DERIBIT": "workers.deribit_worker"
}


def options_db_has_greeks(underlying: str, exchange: str = "binance", data_dir: str = "data") -> bool:
    db_path = f"{data_dir}/{exchange.lower()}/{underlying.upper()}/options_data.db"
    if not os.path.exists(db_path):
        return False
    try:
        con = duckdb.connect(db_path, read_only=True)
        try:
            tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
            if "griegas" not in tables:
                return False
            count = con.execute("SELECT COUNT(*) FROM griegas;").fetchone()[0]
            return count > 0
        finally:
            con.close()
    except Exception:
        return False

def load_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"No se encontro el archivo de configuracion: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

async def stream_process_logs(prefix: str, stream: asyncio.StreamReader, t0_event: asyncio.Event = None):
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
            decoded = line.decode("utf-8", errors="replace").rstrip()
            if decoded:
                print(f"[{prefix}] {decoded}", flush=True)
                if t0_event is not None and "T0 leido correctamente" in decoded:
                    t0_event.set()
    except asyncio.CancelledError:
        pass


async def spawn_worker(
    proc_key: str,
    module_path: str,
    underlying: str,
    sync_env: dict[str, str],
    extra_args: list[str] = None,
    t0_event: asyncio.Event = None
) -> tuple[asyncio.subprocess.Process, asyncio.Task]:
    env = os.environ.copy()
    env["UNDERLYING"] = underlying
    env["PYTHONUNBUFFERED"] = "1"
    env.update(sync_env)

    cmd = [sys.executable, "-u", "-m", module_path]
    if extra_args:
        cmd.extend(extra_args)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
        start_new_session=True
    )
    log_task = asyncio.create_task(stream_process_logs(proc_key, proc.stdout, t0_event))
    print(f"[MAIN] Proceso {proc_key} iniciado (PID: {proc.pid}).", flush=True)
    return proc, log_task


async def terminate_process(proc_key: str, proc: asyncio.subprocess.Process):
    if proc.returncode is not None:
        return
    print(f"[MAIN] Deteniendo {proc_key} (PID: {proc.pid})...", flush=True)
    try:
        proc.terminate()
        await asyncio.wait_for(proc.wait(), timeout=15)
    except asyncio.TimeoutError:
        print(f"[MAIN] Forzando cierre de {proc_key} (PID: {proc.pid})...", flush=True)
        proc.kill()
        await proc.wait()
    except Exception:
        pass


async def run_supervisor(config: dict):
    data_dir = config.get("global", {}).get("data_dir", "data")
    stop_file = f"{data_dir}/STOP_ALL"

    # Limpiamos el archivo de parada si quedo colgado de un crasheo anterior
    if os.path.exists(stop_file):
        os.remove(stop_file)

    print("=" * 75, flush=True)
    print(f"[MAIN] Iniciando Supervisor General", flush=True)
    print("=" * 75, flush=True)

    # Identificador de procesos: (underlying, "WORKER_exchange") -> ej: ("BTC", "OPTIONS_binance")
    processes: dict[tuple[str, str], asyncio.subprocess.Process] = {}
    log_tasks: dict[tuple[str, str], asyncio.Task] = {}
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    def _on_shutdown_signal(signum, frame):
        if not stop_event.is_set():
            print("\n[MAIN] Senal de apagado recibida. Notificando a los workers...", flush=True)
            os.makedirs(data_dir, exist_ok=True)
            with open(stop_file, "w") as f:
                f.write("STOP")
            loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGINT, _on_shutdown_signal)
    signal.signal(signal.SIGTERM, _on_shutdown_signal)

    try:
        # 1. Iterar dinamicamente sobre los exchanges configurados en el YAML
        for exchange_name, ex_config in config.get("exchanges", {}).items():
            if not ex_config.get("enabled", False):
                continue

            symbols = ex_config.get("symbols", ["BTC"])
            sync_mode = ex_config.get("sync_mode", "live_only")
            workers_config = ex_config.get("workers", {})
            
            sync_env = {
                "SYNC_MODE": sync_mode,
                "SYNC_START": ex_config.get("sync_start", ""),
                "SYNC_END": ex_config.get("sync_end", ""),
                "MAX_GAP_DAYS": str(ex_config.get("max_gap_days", 3)),
                "DATA_DIR": data_dir,
                "EXCHANGE": exchange_name,
                "HISTORY_ONLY": str(ex_config.get("history_only", False)).lower()
            }

            for idx, underlying in enumerate(symbols):
                if stop_event.is_set():
                    break

                if idx > 0:
                    await asyncio.sleep(5)
                if exchange_name == "binance":
                    # --init-only para Opciones Binance (arranque en frio)
                    if sync_mode == "greeks" and workers_config.get("options", {}).get("enabled"):
                        if not options_db_has_greeks(underlying, exchange_name, data_dir):
                            print(f"[MAIN] options_data.db no existe para {underlying} ({exchange_name}). Ejecutando --init-only...", flush=True)
                            init_proc, init_log = await spawn_worker(
                                f"OPT-INIT-{underlying}", WORKERS_CONFIG["OPTIONS"], underlying, sync_env, extra_args=["--init-only"]
                            )
                            await init_proc.wait()
                            init_log.cancel()
                            print(f"[MAIN] Primer snapshot de {underlying} completado y DB cerrada.", flush=True)

                    fut_t0_ready = asyncio.Event()
                    spt_t0_ready = asyncio.Event()
                    wait_events = []

                    if workers_config.get("futures", {}).get("enabled"):
                        proc_fut, log_fut = await spawn_worker(
                            f"FUT-{underlying}", WORKERS_CONFIG["FUTURES"], underlying, sync_env, t0_event=fut_t0_ready
                        )
                        processes[(underlying, f"FUTURES_{exchange_name}")] = proc_fut
                        log_tasks[(underlying, f"FUTURES_{exchange_name}")] = log_fut
                        wait_events.append(fut_t0_ready.wait())

                    if workers_config.get("spot", {}).get("enabled"):
                        proc_spt, log_spt = await spawn_worker(
                            f"SPT-{underlying}", WORKERS_CONFIG["SPOT"], underlying, sync_env, t0_event=spt_t0_ready
                        )
                        processes[(underlying, f"SPOT_{exchange_name}")] = proc_spt
                        log_tasks[(underlying, f"SPOT_{exchange_name}")] = log_spt
                        wait_events.append(spt_t0_ready.wait())

                    # Esperamos a que Spot y Futuros lean su T0 de la base de datos de Opciones
                    if wait_events:
                        try:
                            await asyncio.wait_for(asyncio.gather(*wait_events), timeout=10.0)
                        except asyncio.TimeoutError:
                            pass

                    # Lanzamos finalmente las Opciones
                    if workers_config.get("options", {}).get("enabled") and not stop_event.is_set():
                        proc_opt, log_opt = await spawn_worker(
                            f"OPT-{underlying}", WORKERS_CONFIG["OPTIONS"], underlying, sync_env
                        )
                        processes[(underlying, f"OPTIONS_{exchange_name}")] = proc_opt
                        log_tasks[(underlying, f"OPTIONS_{exchange_name}")] = log_opt

                elif exchange_name == "deribit":
                    if workers_config.get("options", {}).get("enabled") and not stop_event.is_set():
                        proc_der, log_der = await spawn_worker(
                            f"DER-{underlying}", WORKERS_CONFIG["DERIBIT"], underlying, sync_env
                        )
                        processes[(underlying, f"DERIBIT_{exchange_name}")] = proc_der
                        log_tasks[(underlying, f"DERIBIT_{exchange_name}")] = log_der

        # 2. Bucle Watchdog (Supervision constante)
        while not stop_event.is_set():
            for (underlying, w_key), proc in list(processes.items()):
                if proc and proc.returncode is not None and not stop_event.is_set():
                    if proc.returncode == 0 and sync_env.get("HISTORY_ONLY") == "true":
                        processes.pop((underlying, w_key), None)
                        continue
                    # Parsear la clave (ej: "OPTIONS_binance" -> worker_type="OPTIONS", exch="binance")
                    worker_type, exch = w_key.split("_", 1)
                    proc_tag = f"{worker_type[:3]}-{underlying}"
                    print(f"[MAIN WATCHDOG] Alerta: {proc_tag} ({exch}) termino con codigo {proc.returncode}. Reiniciando en 5s...", flush=True)
                    
                    if (underlying, w_key) in log_tasks:
                        log_tasks[(underlying, w_key)].cancel()
                        
                    await asyncio.sleep(5)
                    
                    # Recuperar variables especificas de este exchange desde config original
                    re_sync_env = config["exchanges"].get(exch, {})
                    env_vars = {
                        "SYNC_MODE": re_sync_env.get("sync_mode", "live_only"),
                        "SYNC_START": re_sync_env.get("sync_start", ""),
                        "SYNC_END": re_sync_env.get("sync_end", ""),
                        "MAX_GAP_DAYS": str(re_sync_env.get("max_gap_days", 3)),
                        "DATA_DIR": data_dir,
                        "EXCHANGE": exch
                    }
                    
                    new_proc, new_log = await spawn_worker(
                        proc_tag, WORKERS_CONFIG[worker_type], underlying, env_vars
                    )
                    processes[(underlying, w_key)] = new_proc
                    log_tasks[(underlying, w_key)] = new_log

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

    finally:
        print("\n[MAIN] Esperando a que los workers consoliden el .wal y cierren DuckDB...", flush=True)

        async def _wait_or_kill(label: str, proc: asyncio.subprocess.Process):
            if not proc or proc.returncode is not None:
                return
            try:
                await asyncio.wait_for(proc.wait(), timeout=15.0)
            except asyncio.TimeoutError:
                print(f"[MAIN] Timeout esperando a {label}. Forzando cierre...", flush=True)
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass

        await asyncio.gather(
            *(_wait_or_kill(f"{w_key.split('_')[0][:3]}-{und}", proc) for (und, w_key), proc in processes.items()),
            return_exceptions=True
        )

        for t in log_tasks.values():
            t.cancel()

        if os.path.exists(stop_file):
            os.remove(stop_file)

        print("[MAIN] Todos los workers se han detenido y los archivos .wal se han eliminado.", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crypto Data Collector")
    parser.add_argument("--config", default="workers/config.yaml", help="Ruta al archivo YAML de configuracion")
    args = parser.parse_args()

    try:
        cfg = load_config(args.config)
        asyncio.run(run_supervisor(cfg))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"\n[MAIN] Error critico: {e}")