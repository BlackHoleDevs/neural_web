# 🔋 Neural Web – Standalone High-Performance Reverse-Proxy Accelerators

[![License](https://img.shields.io/badge/License-GPLv3-green.svg)](https://www.gnu.org/licenses/gpl-3.0.html)
[![Language](https://img.shields.io/badge/Language-Rust-orange.svg)](https://www.rust-lang.org/)
[![Tokio](https://img.shields.io/badge/Runtime-Tokio-blue.svg)](https://tokio.rs/)
[![Peak Throughput](https://img.shields.io/badge/Peak%20Throughput-43%2C500%2B%20RPS-brightgreen.svg)]()
[![Median Latency](https://img.shields.io/badge/Median%20Latency-%3C%2010ms-blue.svg)]()

**Neural Web** is a collection of production-ready, ultra-low-latency reverse-proxy caching engines written in Rust. Powered by the Tokio asynchronous runtime, DashMap sharded memory indexes, and native Socket2 network optimizations, these proxy servers are designed to shield backend application processes and serve dynamic and static content at lightning-fast speed.

Originally engineered to interface with **OmegaDrive shared-nothing database technology**, these open-source proxies are fully capable of achieving **43,500+ Requests Per Second (RPS)** with a median latency of **9.08ms** under high client concurrency stress.

---

## 🚀 The Proxy Server Suite

This repository features two specialized, standalone caching reverse-proxy applications:

### 1. `neural_web_server` (WooCommerce & WordPress Accelerator)
A hyper-fast reverse-proxy tailored to speed up standard PHP/CMS backends, specifically WordPress and WooCommerce:
*   **Hyper-Early Page Caching:** Intercepts dynamic GET requests and checks the OmegaDrive RAM cache before they ever touch Apache/Nginx or the PHP-FPM pool.
*   **Automatic WebP Transcoding:** Automatically transcodes images to modern, optimized WebP formats on-the-fly to reduce mobile bandwidth usage.
*   **Preloading & SEO Boost:** Detects LCP (Largest Contentful Paint) hero images and automatically injects fetchpriority preloads in the `<head>` tag.
*   **Accessibility Injector:** Modifies HTML on-the-fly to repair missing `aria-label` accessibility elements and incomplete WooCommerce tags, boosting Lighthouse accessibility scores.
*   **UDS Connection Pool:** Leverages Unix Domain Sockets (UDS) with a state-aware connection lease pool to recycle database client sessions safely and prevent socket starvation.

### 2. `neural_next_server` (React & Next.js Accelerator)
An advanced proxy accelerator optimized for modern React, Next.js, and SSR JavaScript applications:
*   **Dual-Layer Hybrid Caching:** Incorporates an in-memory L1 cache (Tokio-native `DashMap` with millisecond-exact expiry timers) backed by a high-throughput L2 key-value store (OmegaDrive).
*   **Speculative Preloader Integration:** Supports prefetching and predictive route loading to make client-side SPA route transitions instantaneous.
*   **Transparent Chunked De-chunking:** Captures, parses, and merges chunked transfer-encoded responses from Next.js backend workers into unified, cacheable binary page assets.
*   **Cookie-Aware Bypass Filters:** Auto-detects session identifiers (`session`, `auth`, `token`, `jwt`) to ensure highly transactional, authenticated user paths bypass cache lookups.

---

## ⚡ Performance Benchmark

Under realistic client stress testing utilizing `wrk` with **400 concurrent TCP connections** across **12 parallel benchmark threads**:

```bash
wrk -t 12 -c 400 -d 10s -H "Host: localhost:8081" http://127.0.0.1:8080/
```

We recorded the following metrics:

| Metric | Benchmark Performance |
| :--- | :---: |
| **Peak Throughput** | **43,522.78 requests/second** 🚀 |
| **Data Throughput** | **1.78 Gigabytes / second** |
| **Median Client Latency** | **9.08 ms** |
| **Socket Health** | **0 errors / 0 timeouts** (100% stability) |

---

## 🛠️ Architecture & Under the Hood

The extreme speed of **Neural Web** is derived from a series of bare-metal optimization choices:

1.  **High-Backlog TCP Handshake Queues:** Bypasses default Linux socket listen limits by utilizing `socket2` to configure custom connection backlogs up to `8192` sockets, preventing SYN queue overflows under high traffic.
2.  **Kernel-Level TCP Optimization:** Enables `TCP_NODELAY` immediately on client accept to disable Nagle's algorithm and ensure cached packets are flushed instantly to the wire.
3.  **Automatic Gzip Detection:** Evaluates `Accept-Encoding: gzip` headers. If the cached asset is already stored in compressed Gzip format, the proxy sends it directly to the socket without decompression overhead, reducing CPU cycle count.
4.  **Graceful File Descriptor Boosting:** Self-detects system constraints on startup and boosts system resource limits (`RLIMIT_NOFILE`) to `65,536` descriptors to effortlessly accommodate concurrent WebSocket and TCP connections.

---

## 📥 Compilation & Deployment

### 1. Prerequisites
Make sure you have a working Rust toolchain installed:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
The proxy requires an OmegaDrive (or compatible RESP protocol key-value database) running and listening on port `6380` or on Unix Domain Socket `/tmp/airdb.sock`.

### 2. Building the Binaries
Build all production-ready, release-optimized binaries:
```bash
cargo build --release
```

The compiled binaries will be located under `./target/release/`:
*   `neural_web_server`
*   `neural_next_server`

### 3. CLI Options & Running

#### Running the WordPress / WooCommerce Accelerator (`neural_web_server`):
The WordPress proxy runs with high-performance hardcoded defaults optimized for typical web servers:
*   **Bind Address:** `0.0.0.0:8080` (Entrance for visitor traffic)
*   **Upstream Backend:** `127.0.0.1:8081` (Apache / Nginx / PHP-FPM pool)
*   **Primary Database connection:** Unix Domain Socket at `/tmp/airdb.sock`
*   **Backup Database connection:** TCP Socket at `127.0.0.1:6380`

To start this proxy as a background daemon:
```bash
nohup ./target/release/neural_web_server > proxy.log 2>&1 &
```

#### Running the Next.js Accelerator (`neural_next_server`):
The Next.js proxy includes a full command-line parser (`clap`) for flexible configuration in containerized or multi-host staging environments:

```bash
./target/release/neural_next_server --help
```

##### Command-Line Arguments:
*   `-b, --bind-ip` : IP address to bind and listen on (Default: `0.0.0.0`)
*   `-p, --port` : Port to bind and listen on (Default: `8080`)
*   `-u, --upstream-ip` : Upstream Next.js backend application server IP (Default: `127.0.0.1`)
*   `-n, --upstream-port` : Upstream Next.js backend server port (Default: `3000`)
*   `--uds-path` : Path to the primary OmegaDrive Unix Domain Socket (Default: `/tmp/airdb.sock`)
*   `--db-port` : Backup TCP database port for OmegaDrive connections (Default: `6380`)

##### Example:
To run the proxy listening on port `80` forwarding to a local Next.js server on port `3000`:
```bash
./target/release/neural_next_server -p 80 -n 3000
```

---

## 📄 License
This suite is open-source software licensed under the **GPLv3 License**. Feel free to use, modify, and distribute according to the terms.
