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
}


def options_db_has_greeks(underlying: str, data_dir:str = "data") -> bool:
    db_path = f"{data_dir}/{underlying}/options_data.db"
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

    if os.path.exists(stop_file):
        os.remove(stop_file)

    binance_cfg = config.get("exchanges", {}).get("binance", {})
    if not binance_cfg.get("enabled", False):
        print("[MAIN] Binance esta desactivado en config.yaml. Saliendo...", flush=True)
        return

    underlyings = binance_cfg.get("symbols", ["BTC"])
    sync_env = {
        "SYNC_MODE": binance_cfg.get("sync_mode", "live_only"),
        "SYNC_START": binance_cfg.get("sync_start", ""),
        "SYNC_END": binance_cfg.get("sync_end", ""),
        "MAX_GAP_DAYS": str(binance_cfg.get("max_gap_days", 3)),
        "DATA_DIR": data_dir
    }

    print("=" * 75, flush=True)
    print(f"[MAIN] Iniciando Supervisor General para {underlyings}", flush=True)
    print("=" * 75, flush=True)

    processes: dict[tuple[str, str], asyncio.subprocess.Process] = {}
    log_tasks: dict[tuple[str, str], asyncio.Task] = {}
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    def _on_shutdown_signal(signum, frame):
        if not stop_event.is_set():
            print("\n[MAIN] Senal de apagado recibida. Notificando a los workers...", flush=True)
            # Crear el archivo que los hijos están vigilando
            os.makedirs(data_dir, exist_ok=True)
            with open(stop_file, "w") as f:
                f.write("STOP")
            loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGINT, _on_shutdown_signal)
    signal.signal(signal.SIGTERM, _on_shutdown_signal)

    try:
        for idx, underlying in enumerate(underlyings):
            if stop_event.is_set():
                break

            if idx > 0:
                await asyncio.sleep(5)

            # Solo necesitamos --init-only si estamos en modo 'greeks' y options_data.db aun no existe
            if sync_env["SYNC_MODE"] == "greeks" and not options_db_has_greeks(underlying):
                print(f"[MAIN] options_data.db no existe para {underlying}. Ejecutando --init-only...", flush=True)
                init_proc, init_log = await spawn_worker(
                    f"OPT-INIT-{underlying}", WORKERS_CONFIG["OPTIONS"], underlying, sync_env, extra_args=["--init-only"]
                )
                await init_proc.wait()
                init_log.cancel()
                print(f"[MAIN] Primer snapshot de {underlying} completado y DB cerrada.", flush=True)
                
            fut_t0_ready = asyncio.Event()
            spt_t0_ready = asyncio.Event()

            proc_fut, log_fut = await spawn_worker(
                f"FUT-{underlying}", WORKERS_CONFIG["FUTURES"], underlying, sync_env, t0_event=fut_t0_ready
            )
            processes[(underlying, "FUTURES")] = proc_fut
            log_tasks[(underlying, "FUTURES")] = log_fut

            proc_spt, log_spt = await spawn_worker(
                f"SPT-{underlying}", WORKERS_CONFIG["SPOT"], underlying, sync_env, t0_event=spt_t0_ready
            )
            processes[(underlying, "SPOT")] = proc_spt
            log_tasks[(underlying, "SPOT")] = log_spt

            try:
                await asyncio.wait_for(
                    asyncio.gather(fut_t0_ready.wait(), spt_t0_ready.wait()),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                pass

            if not stop_event.is_set():
                proc_opt, log_opt = await spawn_worker(
                    f"OPT-{underlying}", WORKERS_CONFIG["OPTIONS"], underlying, sync_env
                )
                processes[(underlying, "OPTIONS")] = proc_opt
                log_tasks[(underlying, "OPTIONS")] = log_opt

        while not stop_event.is_set():
            for (underlying, w_name), proc in list(processes.items()):
                if proc and proc.returncode is not None and not stop_event.is_set():
                    proc_key = f"{w_name[:3]}-{underlying}"
                    print(
                        f"[MAIN WATCHDOG] Alerta: {proc_key} termino con codigo {proc.returncode}. "
                        f"Reiniciando en 5 segundos...",
                        flush=True
                    )
                    if (underlying, w_name) in log_tasks:
                        log_tasks[(underlying, w_name)].cancel()
                    await asyncio.sleep(5)
                    new_proc, new_log = await spawn_worker(
                        proc_key, WORKERS_CONFIG[w_name], underlying, sync_env
                    )
                    processes[(underlying, w_name)] = new_proc
                    log_tasks[(underlying, w_name)] = new_log

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
            *(_wait_or_kill(f"{w_name[:3]}-{und}", proc) for (und, w_name), proc in processes.items()),
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