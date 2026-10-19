# Crypto Institutional Data Collector (Binance & Deribit)

Sistema de recolección de datos de alta frecuencia e históricos para exchanges de criptomonedas (**Binance** en Spot, Futuros USD-M y Opciones Europeas; y **Deribit** en Opciones), escrito en Python asíncrono (`asyncio`) y respaldado por **DuckDB** como motor analítico columnar embebido.

El proyecto unifica en una sola arquitectura modular la captura de flujos en tiempo real por WebSocket, snapshots periódicos por API REST y la sincronización automatizada de archivos históricos masivos desde **Binance Vision**.

---

## 1. Características Principales y Problemas que Resuelve

### A. Cobertura de Datos Efímeros e Institucionales
* **Superficie de Liquidez y Griegas de Opciones (`Binance` y `Deribit`):** Captura continua de IV (`bidIV`, `askIV`, `markIV`), Griegas (`delta`, `gamma`, `vega`, `theta`), Open Interest y libro de órdenes L1/L2 enriquecido en tiempo real sobre los vencimientos y strikes activos. El sistema filtra de forma inteligente la superficie líquida mediante análisis de *Moneyness* y Deltas.
* **Liquidaciones Forzadas en Futuros (`!forceOrder`):** Recolección en vivo de órdenes de liquidación en Binance.
* **Reconstrucción L2 de Orderbook en Spot (`@depth@100ms` + Snapshots):** Almacenamiento coordinado de diferenciales cada 100ms junto con snapshots profundos periódicos para reconstruir el estado exacto del libro en cualquier milisegundo.

### B. Aislamiento por Exchange y Activo en DuckDB
Para evitar bloqueos de concurrencia (*Single-Writer Lock*) sin montar servidores externos, cada combinación de exchange y subyacente almacena sus bases de datos en directorios independientes bajo la ruta `data/{exchange}/{symbol}/`:
* `data/binance/BTC/options_data.db`
* `data/binance/BTC/futures_data.db`
* `data/binance/BTC/spot_data.db`
* `data/deribit/BTC/options_data.db`

### C. Ingesta Eficiente sin Overhead de RAM
* **WebSockets con Tuplas Nativas:** Los mensajes JSON se transforman directamente en tuplas ligeras de Python y se vuelcan por lotes atómicos (`executemany`) cada pocos segundos.
* **Traducción Universal (Driver de Deribit):** Deribit utiliza un esquema de streaming JSON-RPC que el sistema traduce al vuelo para estructurarlo bajo las mismas tablas de Binance, unificando la analítica posterior.

### D. Cierre Limpio Universal (`STOP_ALL`)
* El sistema implementa un mecanismo de parada basado en un archivo centinela (`STOP_ALL`) y manejadores de eventos asíncronos compatibles con Linux y Windows. Al pulsar `Ctrl + C`, los procesos vacían sus buffers, ejecutan `FORCE CHECKPOINT;` en DuckDB para fusionar y **eliminar por completo los archivos `.wal`**, garantizando cero corrupción de datos.

---

## 2. Estructura del Proyecto

```text
crypto-miner/
│
├── core/                           # Motores base compartidos
│   ├── db.py                       # DB_Manager (DuckDB, buffers, auto-flush, checkpoints y export Parquet)
│   ├── restClient.py               # Cliente HTTP asíncrono (aiohttp), rate-limits y descargas ZIP
│   ├── wsClient.py                 # Cliente WebSocket 24/7 con reconexión, backoff y rotación a las 23h50m
│   ├── lifecycle.py                # Gestor universal de ciclo de vida y apagado seguro por señal / STOP_ALL
│   └── routines.py                 # Rutinas compartidas (Sincronización diaria Binance Vision y Snapshots REST)
│
├── markets/                        # Lógica específica por exchange y mercado
│   ├── binance/                    # (Estructura interna: futures, spot, options)
│   └── deribit/                    # Adaptadores específicos para Deribit
│       ├── deribitDB.py            # Instancia DuckDB heredada de DB_Manager
│       ├── restDeribit.py          # Cliente REST para instrumentos y book summary
│       └── wsDeribit.py            # Suscripciones JSON-RPC y Traductor (Driver) a esquema base
│
├── workers/                        # Demonios de ejecución y supervisor general
│   ├── main_worker.py              # Supervisor multi-exchange, watchdog de subprocesos y control de T0
│   ├── futures_worker.py           # Orquestador de Futuros Binance
│   ├── spot_worker.py              # Orquestador de Spot Binance
│   ├── options_worker.py           # Orquestador de Opciones Binance (Caché y superficie activa L2)
│   └── deribit_worker.py           # Orquestador de Opciones Deribit (Superficie por Moneyness y L1)
│
├── tests/                          # Suite de auditoría y validación de integridad sobre DuckDB
│   ├── base_validator.py           # Validador base (0 NULLs, huecos temporales, lados de mercado)
│   ├── test_options.py             # Auditoría de opciones Binance
│   ├── test_futures.py             # Auditoría de futuros
│   ├── test_spot.py                # Auditoría de spot
│   └── test_deribit.py             # Auditoría de opciones Deribit
│
├── config.yaml                     # Archivo central de configuración (Activos, exchanges y granularidad)
├── data/                           # Bases de datos locales organizadas por exchange y activo
├── exports/                        # Volcados incrementales en formato .parquet (.zstd)
├── migrate_db.py                   # Utilidad para migrar esquemas antiguos
└── import_parquet.py               # Utilidad para inyectar .parquet exportados en la DB maestra