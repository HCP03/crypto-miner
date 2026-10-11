import asyncio
import sys
import os
import signal
from datetime import datetime
import duckdb


WORKERS_CONFIG = {
    "FUTURES": "workers.futures_worker",
    "SPOT": "workers.spot_worker",
    "OPTIONS": "workers.options_worker",
}


def options_db_has_greeks(underlying: str) -> bool:
    db_path = f"data/{underlying}/options_data.db"
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


def ask_valid_date(prompt_text: str) -> str:
    while True:
        val = input(prompt_text).strip()
        try:
            datetime.strptime(val, "%Y-%m-%d")
            return val
        except ValueError:
            print("  [Error] Formato invalido. Usa exactamente YYYY-MM-DD (ej. 2026-09-15).")


def interactive_menu() -> tuple[list[str], dict[str, str]]:
    """Permite introducir uno o varios activos y elegir entre los 5 modos."""
    print("=" * 65)
    print("   CONFIGURACION DEL RECOLECTOR DE DATOS BINANCE")
    print("=" * 65)

    default_sym = os.getenv("UNDERLYING", "BTC")
    raw_input = input(f"Subyacentes separados por coma [{default_sym}] (ej. BTC, ETH): ").strip().upper()
    raw_symbols = raw_input if raw_input else default_sym
    
    underlyings = []
    for s in raw_symbols.split(","):
        clean = s.strip().replace("USDT", "")
        if clean and clean not in underlyings:
            underlyings.append(clean)

    print(f"\nActivos seleccionados: {', '.join(underlyings)}")
    print("\nSelecciona el modo de recoleccion:")
    print("  1. Rango de fechas en Vision (start_date -> end_date)")
    print("  2. Desde una fecha hasta hoy en Vision (start_date -> hoy)")
    print("  3. Backfill completo hacia atras en Vision (max 3 dias de gap)")
    print("  4. Default Completo: Vision desde el primer snapshot de griegas (T0) + Vivo")
    print("  5. SOLO EN VIVO (Sin Binance Vision: solo Opciones, Liquidaciones y Orderbook Spot)")

    while True:
        choice = input("\nOpcion [4]: ").strip()
        if not choice or choice == "4":
            sync_env = {"SYNC_MODE": "greeks", "SYNC_START": "", "SYNC_END": "", "MAX_GAP_DAYS": "3"}
            break
        elif choice == "1":
            start_d = ask_valid_date("  Fecha de inicio (YYYY-MM-DD): ")
            end_d = ask_valid_date("  Fecha de fin    (YYYY-MM-DD): ")
            if start_d > end_d:
                print("  [Error] La fecha de inicio no puede ser posterior a la fecha de fin.")
                continue
            sync_env = {"SYNC_MODE": "range", "SYNC_START": start_d, "SYNC_END": end_d, "MAX_GAP_DAYS": "3"}
            break
        elif choice == "2":
            start_d = ask_valid_date("  Fecha de inicio (YYYY-MM-DD): ")
            sync_env = {"SYNC_MODE": "from_date", "SYNC_START": start_d, "SYNC_END": "", "MAX_GAP_DAYS": "3"}
            break
        elif choice == "3":
            gap_input = input("  Maximo de dias de gap permitidos [3]: ").strip()
            max_gap = gap_input if gap_input.isdigit() else "3"
            sync_env = {"SYNC_MODE": "backfill_max", "SYNC_START": "", "SYNC_END": "", "MAX_GAP_DAYS": max_gap}
            break
        elif choice == "5":
            sync_env = {"SYNC_MODE": "live_only", "SYNC_START": "", "SYNC_END": "", "MAX_GAP_DAYS": "3"}
            break
        else:
            print("  [Error] Introduce un numero del 1 al 5.")

    print()
    return underlyings, sync_env


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
        env=env
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


async def run_supervisor(underlyings: list[str], sync_env: dict[str, str]):
    print("=" * 75, flush=True)
    print(
        f"[MAIN] Iniciando Supervisor General para {underlyings} | "
        f"Modo Historico: {sync_env['SYNC_MODE']}",
        flush=True
    )
    print("=" * 75, flush=True)

    # Diccionarios indexados por (underlying, worker_name), ej: ("BTC", "FUTURES")
    processes: dict[tuple[str, str], asyncio.subprocess.Process] = {}
    log_tasks: dict[tuple[str, str], asyncio.Task] = {}
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    def _on_shutdown_signal():
        if not stop_event.is_set():
            print("\n[MAIN] Senal de apagado recibida. Cerrando todos los workers...", flush=True)
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_shutdown_signal)
        except NotImplementedError:
            pass

    try:
        for idx, underlying in enumerate(underlyings):
            if stop_event.is_set():
                break

            # Escalonamos 5 segundos entre activos para no solapar picos de peticiones REST
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
                
            # 2. Lanzar FUTURES y SPOT para este activo y esperar lectura de T0
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

            # 3. Lanzar OPTIONS continuo para este activo
            if not stop_event.is_set():
                proc_opt, log_opt = await spawn_worker(
                    f"OPT-{underlying}", WORKERS_CONFIG["OPTIONS"], underlying, sync_env
                )
                processes[(underlying, "OPTIONS")] = proc_opt
                log_tasks[(underlying, "OPTIONS")] = log_opt

        # 4. Bucle Watchdog para todos los activos y mercados
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
        await asyncio.gather(
            *(terminate_process(f"{w_name[:3]}-{und}", proc) for (und, w_name), proc in processes.items()),
            return_exceptions=True
        )
        for t in log_tasks.values():
            t.cancel()
        print("[MAIN] Todos los workers se han detenido y las bases de datos estan cerradas.", flush=True)


if __name__ == "__main__":
    try:
        # Si se lanza desde systemd (--auto o sin terminal interactiva), usa variables de entorno
        if "--auto" in sys.argv or not sys.stdin.isatty():
            raw_syms = os.getenv("UNDERLYING", "BTC")
            default_syms = [s.strip().upper().replace("USDT", "") for s in raw_syms.split(",") if s.strip()]
            default_env = {
                "SYNC_MODE": os.getenv("SYNC_MODE", "greeks"),
                "SYNC_START": os.getenv("SYNC_START", ""),
                "SYNC_END": os.getenv("SYNC_END", ""),
                "MAX_GAP_DAYS": os.getenv("MAX_GAP_DAYS", "3"),
            }
            asyncio.run(run_supervisor(default_syms, default_env))
        else:
            # Si lo lanzas manualmente en tmux o consola, muestra el menu interactivo
            selected_symbols, selected_sync_env = interactive_menu()
            asyncio.run(run_supervisor(selected_symbols, selected_sync_env))
    except KeyboardInterrupt:
        print("\nEjecucion cancelada por el usuario.")