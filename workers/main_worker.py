import asyncio
import sys
import os
import signal
import argparse
import yaml

WORKERS_CONFIG = {
    "FUTURES": "workers.futures_worker",
    "SPOT": "workers.spot_worker",
    "OPTIONS": "workers.options_worker",
    "DERIBIT": "workers.deribit_worker"
}


def load_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"No se encontro el archivo de configuracion: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


async def stream_process_logs(prefix: str, stream: asyncio.StreamReader):
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
            decoded = line.decode("utf-8", errors="replace").rstrip()
            if decoded:
                print(f"[{prefix}] {decoded}", flush=True)
    except asyncio.CancelledError:
        pass


async def spawn_worker(
    proc_key: str,
    module_path: str,
    underlying: str,
    sync_env: dict[str, str],
) -> tuple[asyncio.subprocess.Process, asyncio.Task]:
    env = os.environ.copy()
    env["UNDERLYING"] = underlying
    env["PYTHONUNBUFFERED"] = "1"
    env.update(sync_env)

    cmd = [sys.executable, "-u", "-m", module_path]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
        start_new_session=True
    )
    log_task = asyncio.create_task(stream_process_logs(proc_key, proc.stdout))
    print(f"[MAIN] Proceso {proc_key} iniciado (PID: {proc.pid}).", flush=True)
    return proc, log_task


async def run_supervisor(config: dict):
    data_dir = config.get("global", {}).get("data_dir", "data")
    stop_file = f"{data_dir}/STOP_ALL"

    if os.path.exists(stop_file):
        os.remove(stop_file)

    print("=" * 75, flush=True)
    print("[MAIN] Iniciando Supervisor Ligero (StreamWriter)", flush=True)
    print("=" * 75, flush=True)

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
        for exchange_name, ex_config in config.get("exchanges", {}).items():
            if not ex_config.get("enabled", False):
                continue

            symbols = ex_config.get("symbols", ["BTC"])
            workers_config = ex_config.get("workers", {})
            sync_env = {
                "SYNC_MODE": ex_config.get("sync_mode", "live_only"),
                "DATA_DIR": data_dir,
                "EXCHANGE": exchange_name,
            }

            for idx, underlying in enumerate(symbols):
                if stop_event.is_set():
                    break
                if idx > 0:
                    await asyncio.sleep(3)

                if exchange_name == "binance":
                    for w_name, key in [("futures", "FUTURES"), ("spot", "SPOT"), ("options", "OPTIONS")]:
                        if workers_config.get(w_name, {}).get("enabled") and not stop_event.is_set():
                            proc, log_t = await spawn_worker(
                                f"{key[:3]}-{underlying}", WORKERS_CONFIG[key], underlying, sync_env
                            )
                            processes[(underlying, f"{key}_{exchange_name}")] = proc
                            log_tasks[(underlying, f"{key}_{exchange_name}")] = log_t

                elif exchange_name == "deribit":
                    if workers_config.get("options", {}).get("enabled") and not stop_event.is_set():
                        proc, log_t = await spawn_worker(
                            f"DER-{underlying}", WORKERS_CONFIG["DERIBIT"], underlying, sync_env
                        )
                        processes[(underlying, f"DERIBIT_{exchange_name}")] = proc
                        log_tasks[(underlying, f"DERIBIT_{exchange_name}")] = log_t

        while not stop_event.is_set():
            for (underlying, w_key), proc in list(processes.items()):
                if proc and proc.returncode is not None and not stop_event.is_set():
                    worker_type, exch = w_key.split("_", 1)
                    proc_tag = f"{worker_type[:3]}-{underlying}"
                    print(f"[MAIN WATCHDOG] {proc_tag} ({exch}) salio con codigo {proc.returncode}. Reiniciando en 5s...", flush=True)

                    if (underlying, w_key) in log_tasks:
                        log_tasks[(underlying, w_key)].cancel()

                    await asyncio.sleep(5)
                    env_vars = {
                        "SYNC_MODE": config["exchanges"].get(exch, {}).get("sync_mode", "live_only"),
                        "DATA_DIR": data_dir,
                        "EXCHANGE": exch,
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
        print("\n[MAIN] Esperando a que los workers vuelquen buffers...", flush=True)

        async def _wait_or_kill(label: str, proc: asyncio.subprocess.Process):
            if not proc or proc.returncode is not None:
                return
            try:
                await asyncio.wait_for(proc.wait(), timeout=15)
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

        print("[MAIN] Todos los workers se han detenido.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crypto Data Collector Light")
    parser.add_argument("--config", default="workers/default_config.yaml", help="Ruta al archivo YAML de configuracion")
    args = parser.parse_args()

    try:
        cfg = load_config(args.config)
        asyncio.run(run_supervisor(cfg))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"\n[MAIN] Error critico: {e}")
