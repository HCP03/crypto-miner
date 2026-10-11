# Binance Data Collector (Spot, Futures & Options)

Recolector de datos de mercado para Binance (`Spot`, `USD-M Futures` y `European Options`) escrito en Python con `asyncio` y almacenamiento local en **DuckDB**. 

El objetivo del proyecto es capturar datos en tiempo real que no están disponibles en repositorios históricos (como la superficie de volatilidad de opciones, liquidaciones de futuros o el flujo L2 de orderbook en spot) y combinarlos automáticamente con los volcados diarios de **Binance Vision**.

---

## Por qué esta arquitectura

Durante el desarrollo de versiones anteriores surgieron varios problemas al intentar mantener la captura corriendo 24/7 en hardware limitado. Este repositorio los resuelve de la siguiente forma:

* **Datos que Binance Vision no guarda:** Binance publica históricos diarios de *trades* y métricas agregadas de futuros, pero no guarda el histórico de griegas/IV de opciones (`/eapi/v1/mark`), ni el orderbook L2 de opciones, ni el stream de liquidaciones forzadas (`!forceOrder`), ni las actualizaciones cada 100ms del libro de Spot. Este sistema captura esos flujos en vivo y delega el resto a descargas programadas de Vision a las 04:00 UTC.
* **Bloqueos de escritura en DuckDB:** DuckDB bloquea el archivo `.db` cuando un proceso lo abre en modo lectura/escritura. Para evitar colisiones sin montar un servidor de base de datos externo, cada mercado y activo escribe en su propio archivo (`options_data.db`, `futures_data.db`, `spot_data.db`) desde subprocesos independientes. Para sincronizar la fecha inicial ($T_0$) sin bloquear el archivo, el supervisor toma primero el snapshot inicial de opciones, cierra la conexión para que futuros y spot lean la fecha de arranque, y después levanta el recolector continuo de opciones.
* **Cero overhead de DataFrames en ingesta:** Se eliminó el paso por `polars` y `pyarrow` en los WebSockets. Los mensajes entran como tuplas nativas en un buffer en memoria y se insertan por lotes con `executemany`. Los CSV de Binance Vision se leen e insertan directamente desde el motor SQL de DuckDB (`read_csv`), agrupando los trades del mismo milisegundo (`GROUP BY timestamp, price, taker`) para no perder volumen por colisiones de clave primaria ni saturar la RAM.
* **Cortes de 24h en WebSockets y archivos `.wal`:** Binance corta cualquier conexión WebSocket a las 24 horas exactas. El cliente base fuerza una rotación limpia a las 23h 50m y mantiene pings cada 20s para evitar *sockets* zombis. Por su parte, el gestor de base de datos ejecuta `CHECKPOINT` cada 15 minutos para vaciar el archivo `.wal` en disco sin detener la captura.

---

## Estructura del proyecto

```text
.
├── core/
│   ├── db.py                 # Wrapper de DuckDB: buffers en RAM, auto-flush, checkpoints y export a Parquet
│   ├── restClient.py         # Cliente HTTP base (aiohttp), control de peso de API y descarga de ZIPs
│   └── wsClient.py           # Cliente WebSocket con reconexión, backoff y rotación a las 23h50m
│
├── markets/
│   ├── futures/
│   │   ├── futuresDB.py      # Tablas: trades, open_interest, funding_rate, ob_depth, liquidations
│   │   ├── restFutures.py    # Descarga e ingesta SQL desde Binance Vision + respaldo por REST (fapi)
│   │   └── wsFutures.py      # WebSocket de liquidaciones (!forceOrder) y aggTrades
│   │
│   ├── spot/
│   │   ├── spotDB.py         # Tablas: trades, ob_snapshot, ob_diff
│   │   ├── restSpot.py       # Snapshots del libro (/api/v3/depth) y descarga diaria de trades en Vision
│   │   └── wsSpot.py         # WebSocket de diferenciales del libro (@depth@100ms)
│   │
│   └── options/
│       ├── optionsDB.py      # Tablas: options, griegas, oi, trades, top_orderbook
│       ├── restOptions.py    # Snapshots de contratos, griegas, OI por vencimiento y tickers 24h
│       └── wsOptions.py      # Streams de trades, griegas y filtro dinámico de superficie activa (@depth5)
│
├── workers/
│   ├── main_worker.py        # Punto de entrada: menú, coordinación de T0 y watchdog de subprocesos
│   ├── futures_worker.py     # Proceso de futuros (liquidaciones en vivo + tarea diaria de Vision)
│   ├── spot_worker.py        # Proceso de spot (orderbook en vivo + tarea diaria de Vision)
│   └── options_worker.py     # Proceso de opciones (caché en memoria + cruce de libro L2 y griegas)
│
├── tests/
│   ├── base_validator.py     # Funciones de validación (nulos, huecos temporales, lados de mercado)
│   ├── test_futures.py       # Chequeo de futures_data.db
│   ├── test_spot.py          # Chequeo de spot_data.db
│   └── test_options.py       # Chequeo de options_data.db
│
├── data/                     # Bases de datos locales organizadas por activo (ej. data/BTC/*.db)
├── exports/                  # Volcados incrementales en formato .parquet
├── migrate_db.py             # Utilidad para migrar datos desde el formato antiguo
└── import_parquet.py         # Utilidad para inyectar los .parquet exportados en otra máquina
```

---

## Uso

Todo se lanza desde `workers/main_worker.py`. Este script levanta los tres recolectores como procesos independientes del sistema operativo, unifica sus salidas por consola con prefijos (`[OPT-BTC]`, `[FUT-BTC]`, `[SPT-BTC]`) y los reinicia automáticamente si alguno cae.

### 1. Modo interactivo (por consola)

Ejecuta desde la raíz del proyecto:

```bash
python -m workers.main_worker
```

El menú te pedirá:
1. **Subyacentes:** Pulsa `Enter` para `BTC` o introduce varios separados por coma (`BTC, ETH`).
2. **Modo de sincronización histórica:**
   * `1` **Rango de fechas:** Descarga desde `start_date` hasta `end_date` en Binance Vision y sigue recolectando en vivo.
   * `2` **Desde una fecha hasta hoy:** Descarga desde `start_date` hasta el cierre de ayer (UTC) y mantiene la descarga diaria.
   * `3` **Backfill hacia atrás:** Recorre el histórico desde ayer hacia atrás hasta toparse con más de 3 días seguidos sin datos en Binance Vision.
   * `4` **Default (desde $T_0$ de griegas):** Toma la fecha del primer registro de `griegas` en `options_data.db` y sincroniza Spot y Futuros desde ese día hasta hoy.
   * `5` **Solo en vivo (`live_only`):** Desactiva todas las descargas de Binance Vision. Solo guarda lo que no se puede recuperar después (Opciones, Liquidaciones de futuros y Orderbook de Spot). Pensado para correr en un portátil o VPS pequeño y descargar la parte pesada de Vision más adelante en otro ordenador.

### 2. Modo automático (para `systemd`, `tmux` o servidores)

Si pasas el argumento `--auto` (o si se ejecuta como servicio sin terminal interactiva), se salta el menú y lee la configuración de las variables de entorno:

```bash
# Ejecución estándar (BTC, modo default desde el primer snapshot de griegas)
python -m workers.main_worker --auto

# Ejecución ligera en segundo plano solo para datos en vivo (Modo 5)
UNDERLYING="BTC" SYNC_MODE="live_only" python -m workers.main_worker --auto

# Ejecución para varios activos desde una fecha concreta
UNDERLYING="BTC,ETH" SYNC_MODE="from_date" SYNC_START="2026-09-01" python -m workers.main_worker --auto
```

Para pararlo manualmente, pulsa `Ctrl + C`. El proceso principal enviará la señal de cierre a los tres workers para que vuelquen sus buffers pendientes a DuckDB antes de salir.

---

## Pasar datos de un equipo a otro (Exportación en caliente)

Si tienes un equipo secundario recolectando en modo `5` (`live_only`) y quieres pasar los datos a tu ordenador principal sin parar los WebSockets ni copiar los archivos `.db` enteros por USB:

1. **En el equipo que está recolectando**, crea los archivos disparadores desde otra terminal:
   ```bash
   touch data/BTC/EXPORT_OPTIONS data/BTC/EXPORT_FUTURES data/BTC/EXPORT_SPOT
   ```
   En el siguiente ciclo de volcado (máximo 5 segundos), cada worker exportará únicamente las filas nuevas desde la última exportación a la carpeta `exports/BTC/` en archivos `.parquet` comprimidos con ZSTD.

2. **Copia la carpeta `exports/BTC/` al ordenador principal** (por red local con `rsync`/`scp` o mediante USB):
   ```bash
   rsync -avz --remove-source-files usuario@ip-portatil:~/proyecto/exports/ ./exports/
   ```

3. **En el ordenador principal**,Con los workers apagados, fusiona los datos nuevos ejecutando:
   ```bash
   python import_parquet.py
   ```
   Una vez importados, puedes arrancar `main_worker.py` en la **Opción 4** para que el ordenador principal descargue los archivos pesados de Binance Vision que faltaban desde $T_0$.

---

## Tests de verificación

Con el recolector detenido (para que DuckDB permita abrir los archivos), puedes comprobar que no existan valores nulos, listas corruptas o cortes de tiempo imprevistos ejecutando:

```bash
python -m tests.test_options
python -m tests.test_futures
python -m tests.test_spot
```