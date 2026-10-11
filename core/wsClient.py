import asyncio
import json
import time
import websockets
from websockets.exceptions import ConnectionClosed

class WSClient:
    # Rotacion preventiva a las 23h 50m (85,800s) antes del corte forzoso de 24h de Binance
    MAX_SESSION_SEC = (23 * 3600) + (50 * 60)

    @staticmethod
    async def fetch(url: str, parser_callback, handler_callback, read_timeout: float = 1800.0):
        """
        Mantiene una conexion continua 24/7 con auto-reconexion y rotacion preventiva <24h.
        - ping_interval=20 / ping_timeout=20: detecta caidas silenciosas de red (half-open sockets) en 20s.
        - read_timeout: fuerza reconexion si la libreria se bloquea esperando tramas.
        """
        retry_delay = 1
        
        while True:
            session_start = time.time()
            planned_rotation = False
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=4096
                ) as websocket:
                    print(f"[WS] Conectado a {url}", flush=True)
                    retry_delay = 1  # Resetear espera tras conexion exitosa
                    
                    while True:
                        # 1. Comprobar si nos acercamos al limite de 24h de Binance
                        if (time.time() - session_start) >= WSClient.MAX_SESSION_SEC:
                            print(f"[WS] Rotacion preventiva de 24h en {url}. Reconectando...", flush=True)
                            planned_rotation = True
                            break

                        # 2. Esperar mensaje con timeout para evitar bloqueos perpetuos
                        message = await asyncio.wait_for(websocket.recv(), timeout=read_timeout)

                        try:
                            data = json.loads(message)
                            parsed_record = parser_callback(data)
                            
                            if parsed_record is not None:
                                if asyncio.iscoroutinefunction(handler_callback):
                                    await handler_callback(parsed_record)
                                else:
                                    handler_callback(parsed_record)
                        except Exception as parse_err:
                            print(f"[WS] ERROR procesando mensaje en {url}: {parse_err}", flush=True)

            except asyncio.CancelledError:
                print(f"[WS] Cerrando conexion por parada del sistema: {url}", flush=True)
                break
            except asyncio.TimeoutError:
                print(f"[WS] Sin actividad tras {read_timeout}s en {url}. Refrescando conexion...", flush=True)
                await asyncio.sleep(1)
            except (ConnectionClosed, Exception) as e:
                print(f"[WS] Desconexion en {url} ({e}). Reconectando en {retry_delay}s...", flush=True)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

            # Si fue una rotacion programada de 23h50m, reconecta casi al instante (0.2s)
            if planned_rotation:
                await asyncio.sleep(0.2)

    @staticmethod
    def parse_trade(data: dict) -> tuple | None:
        """Parser generico de AggTrades para heredar en Spot y Futuros"""
        payload = data.get("data", data)
        if "p" in payload and "q" in payload:
            taker = -1 if payload.get("m") else 1
            price = float(payload["p"])
            qty = float(payload["q"])
            return (
                price,
                qty,
                price * qty,
                int(payload.get("T")),
                int(taker)
            )
        return None