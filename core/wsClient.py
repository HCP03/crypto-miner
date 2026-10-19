import asyncio
import json
import time
import websockets
from websockets.exceptions import ConnectionClosed
from core.logger import get_logger, short_ws_url

log = get_logger("WS")


class WSClient:
    MAX_SESSION_SEC = (23 * 3600) + (50 * 60)

    @staticmethod
    async def fetch(
        url: str, 
        parser_callback, 
        handler_callback, 
        read_timeout: float = 1800.0,
        subscribe_msg: dict = None
    ):
        retry_delay = 1
        tag = short_ws_url(url)
        first_connect = True
        
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
                    
                    if first_connect:
                        log.info(f"Conectado -> {tag}")
                        first_connect = False
                    else:
                        log.info(f"Reconectado -> {tag}")
                    log.debug(f"Full URL conectada: {url}")

                    if subscribe_msg:
                        await websocket.send(json.dumps(subscribe_msg))
                        log.debug(f"Suscripcion enviada a {tag}")

                    retry_delay = 1
                    
                    while True:
                        if (time.time() - session_start) >= WSClient.MAX_SESSION_SEC:
                            log.info(f"Rotacion preventiva 24h -> {tag}")
                            planned_rotation = True
                            break

                        message = await asyncio.wait_for(websocket.recv(), timeout=read_timeout)

                        try:
                            data = json.loads(message)

                            if isinstance(data, dict) and data.get("method") == "heartbeat" and data.get("params", {}).get("type") == "test_request":
                                await websocket.send(json.dumps({
                                    "jsonrpc": "2.0",
                                    "method": "public/test",
                                    "params": {}
                                }))
                                continue

                            parsed_record = parser_callback(data)
                            
                            if parsed_record is not None:
                                if asyncio.iscoroutinefunction(handler_callback):
                                    await handler_callback(parsed_record)
                                else:
                                    handler_callback(parsed_record)
                        except Exception as parse_err:
                            log.error(f"Error parseando mensaje en {tag}: {parse_err}")

            except asyncio.CancelledError:
                log.debug(f"Cerrando socket por parada: {tag}")
                break
            except asyncio.TimeoutError:
                log.warning(f"Inactividad ({read_timeout:.0f}s) en {tag}. Refrescando...")
                await asyncio.sleep(1)
            except (ConnectionClosed, Exception) as e:
                err_short = str(e).splitlines()[0][:80]
                log.warning(f"Caida en {tag} ({err_short}). Reintento en {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

            if planned_rotation:
                await asyncio.sleep(0.2)

    @staticmethod
    def parse_trade(data: dict) -> tuple | None:
        """Parser por defecto para Binance Spot/Futures (mantenido por compatibilidad)"""
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