import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse


def short_ws_url(url: str) -> str:
    """
    Acorta las URLs gigantes de WebSockets para que no inunden la consola.
    Ej: 'wss://nbstream.binance.com/eoptions/stream?streams=s1/s2/s3...' -> 'eoptions (25 streams)'
    """
    try:
        parsed = urlparse(url)
        host_tag = parsed.netloc.split(".")[0]  # fstream, stream, nbstream...
        if "streams=" in parsed.query:
            streams_part = parsed.query.split("streams=")[1]
            stream_list = streams_part.split("/")
            if len(stream_list) > 2:
                first_sym = stream_list[0].split("@")[0]
                stream_type = stream_list[0].split("@")[1] if "@" in stream_list[0] else "stream"
                return f"{host_tag}[{first_sym}..+{len(stream_list)-1} @{stream_type}]"
            return f"{host_tag}[{streams_part}]"
        path_clean = parsed.path.replace("/ws/", "")
        return f"{host_tag}[{path_clean}]"
    except Exception:
        return url[:45] + "..."


def get_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    """
    Configura un logger con salida limpia por consola (INFO) y archivo rotativo detallado (DEBUG).
    """
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # 1. Formato corto y limpio para la Terminal (solo INFO o superior)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-5s | %(message)s",
        datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_fmt)

    # 2. Formato completo en archivo rotativo (max 10 MB x 3 archivos)
    file_path = os.path.join(log_dir, "collector.log")
    file_handler = RotatingFileHandler(
        file_path, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_fmt = logging.Formatter(
        fmt="%(asctime)s | %(name)-10s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_fmt)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger