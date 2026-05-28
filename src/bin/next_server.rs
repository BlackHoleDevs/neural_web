use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(not(unix))]
pub struct UnixStream;
#[cfg(not(unix))]
impl UnixStream {
    pub async fn connect<P: AsRef<std::path::Path>>(_path: P) -> Result<Self, std::io::Error> {
        Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "UDS not supported on Windows"))
    }
}
#[cfg(not(unix))]
impl tokio::io::AsyncRead for UnixStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
#[cfg(not(unix))]
impl tokio::io::AsyncWrite for UnixStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::task::Poll::Ready(Ok(0))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use bytes::BytesMut;
use clap::Parser;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use md5;
use dashmap::DashMap;
use lazy_static::lazy_static;

fn dechunk(body: &[u8]) -> Option<Vec<u8>> {
    let mut dechunked = Vec::new();
    let mut pos = 0;
    while pos < body.len() {
        let mut size_end = None;
        for i in pos..body.len().saturating_sub(1) {
            if body[i] == b'\r' && body[i+1] == b'\n' {
                size_end = Some(i);
                break;
            }
        }
        let idx = size_end?;
        let size_str = std::str::from_utf8(&body[pos..idx]).ok()?;
        let raw_hex = size_str.split(';').next()?.trim();
        let chunk_size = usize::from_str_radix(raw_hex, 16).ok()?;
        
        if chunk_size == 0 {
            break; 
        }
        
        pos = idx + 2; 
        if pos + chunk_size > body.len() {
            return None; 
        }
        dechunked.extend_from_slice(&body[pos..pos+chunk_size]);
        pos += chunk_size;
        
        if pos + 2 <= body.len() && body[pos] == b'\r' && body[pos+1] == b'\n' {
            pos += 2;
        } else {
            return None; 
        }
    }
    Some(dechunked)
}

lazy_static! {
    static ref L1_CACHE: DashMap<String, (Vec<u8>, u64)> = DashMap::new();
}

// Command Line Arguments
#[derive(Parser, Debug)]
#[command(author, version, about = "Omega Drive Next.js & React High-Performance Proxy Accelerator", long_about = None)]
struct Args {
    /// Proxy IP to bind and listen on
    #[arg(short = 'b', long, default_value = "0.0.0.0")]
    bind_ip: String,

    /// Proxy Port to bind and listen on
    #[arg(short = 'p', long, default_value_t = 8080)]
    port: u16,

    /// Upstream Next.js Server IP
    #[arg(short = 'u', long, default_value = "127.0.0.1")]
    upstream_ip: String,

    /// Upstream Next.js Server Port
    #[arg(short = 'n', long, default_value_t = 3000)]
    upstream_port: u16,

    /// Path to Omega DB Unix Domain Socket
    #[arg(long, default_value = "/tmp/airdb.sock")]
    uds_path: String,

    /// Backup TCP Port to Omega DB
    #[arg(long, default_value_t = 6380)]
    db_port: u16,
}

struct UdsPool {
    connections: std::sync::Mutex<Vec<UnixStream>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    path: String,
}

impl UdsPool {
    async fn new(size: usize, path: &str) -> Arc<Self> {
        let mut conns = Vec::with_capacity(size);
        for _ in 0..size {
            if let Ok(stream) = UnixStream::connect(path).await {
                conns.push(stream);
            }
        }
        let actual_size = conns.len();
        println!("🔋 [UDS POOL] Initialized with {}/{} active connections to {}", actual_size, size, path);
        Arc::new(UdsPool {
            connections: std::sync::Mutex::new(conns),
            semaphore: Arc::new(tokio::sync::Semaphore::new(actual_size)),
            path: path.to_string(),
        })
    }

    async fn get(self: &Arc<Self>) -> Option<(UnixStream, UdsPoolGuard)> {
        let permit = self.semaphore.clone().acquire_owned().await.ok()?;
        let conn = self.connections.lock().unwrap().pop()?;
        Some((conn, UdsPoolGuard {
            pool: Arc::clone(self),
            permit,
            is_healthy: true,
        }))
    }
}

struct UdsPoolGuard {
    pool: Arc<UdsPool>,
    permit: tokio::sync::OwnedSemaphorePermit,
    is_healthy: bool,
}

impl UdsPoolGuard {
    async fn release(self, conn: UnixStream) {
        if self.is_healthy {
            self.pool.connections.lock().unwrap().push(conn);
        } else {
            if let Ok(stream) = UnixStream::connect(&self.pool.path).await {
                self.pool.connections.lock().unwrap().push(stream);
            }
        }
    }
}

async fn connect_to_matrix(port: u16) -> Option<tokio::net::TcpStream> {
    if let Ok(stream) = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port)).await {
        let _ = stream.set_nodelay(true);
        Some(stream)
    } else {
        None
    }
}

async fn read_line<S>(stream: &mut S) -> Option<String>
where
    S: tokio::io::AsyncReadExt + Unpin,
{
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    while stream.read_exact(&mut byte).await.is_ok() {
        line.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
    }
    String::from_utf8(line).ok()
}

async fn read_resp_bulk<S>(stream: &mut S) -> Option<Vec<u8>>
where
    S: tokio::io::AsyncReadExt + Unpin,
{
    let line = read_line(stream).await?;
    if line.starts_with('$') {
        let bulk_len: isize = line[1..].trim().parse().unwrap_or(-1);
        if bulk_len >= 0 {
            let mut data = vec![0u8; bulk_len as usize];
            if stream.read_exact(&mut data).await.is_ok() {
                let mut crlf = [0u8; 2];
                let _ = stream.read_exact(&mut crlf).await;
                return Some(data);
            }
        }
    }
    None
}

async fn get_from_matrix(pool: &Arc<UdsPool>, key: &str, db_port: u16) -> Option<Vec<u8>> {
    if let Ok(Some((mut uds, guard))) = tokio::time::timeout(Duration::from_millis(50), pool.get()).await {
        let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
        if uds.write_all(cmd.as_bytes()).await.is_ok() {
            if let Some(res) = read_resp_bulk(&mut uds).await {
                guard.release(uds).await;
                return Some(res);
            }
        }
        let mut broken_guard = guard;
        broken_guard.is_healthy = false;
        broken_guard.release(uds).await;
    }

    if let Some(mut tcp) = connect_to_matrix(db_port).await {
        let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
        if tcp.write_all(cmd.as_bytes()).await.is_ok() {
            return read_resp_bulk(&mut tcp).await;
        }
    }
    None
}

async fn save_to_matrix(pool: &Arc<UdsPool>, key: &str, data: &[u8], db_port: u16) -> Option<()> {
    if let Ok(Some((mut uds, guard))) = tokio::time::timeout(Duration::from_millis(50), pool.get()).await {
        let cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n", key.len(), key, data.len());
        if uds.write_all(cmd.as_bytes()).await.is_ok() {
            if uds.write_all(data).await.is_ok() && uds.write_all(b"\r\n").await.is_ok() {
                if read_line(&mut uds).await.is_some() {
                    guard.release(uds).await;
                    return Some(());
                }
            }
        }
        let mut broken_guard = guard;
        broken_guard.is_healthy = false;
        broken_guard.release(uds).await;
    }

    if let Some(mut tcp) = connect_to_matrix(db_port).await {
        let cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n", key.len(), key, data.len());
        if tcp.write_all(cmd.as_bytes()).await.is_ok() {
            let _ = tcp.write_all(data).await;
            let _ = tcp.write_all(b"\r\n").await;
            return Some(());
        }
    }
    None
}

fn get_mime_type(uri: &str) -> &'static str {
    if uri.ends_with(".css") { "text/css" }
    else if uri.ends_with(".js") { "application/javascript" }
    else if uri.ends_with(".svg") { "image/svg+xml" }
    else if uri.ends_with(".png") { "image/png" }
    else if uri.ends_with(".jpg") || uri.ends_with(".jpeg") { "image/jpeg" }
    else if uri.ends_with(".webp") { "image/webp" }
    else if uri.ends_with(".woff2") { "font/woff2" }
    else if uri.ends_with(".woff") { "font/woff" }
    else if uri.ends_with(".json") { "application/json" }
    else { "text/html; charset=UTF-8" }
}

fn boost_fd_limit() {
    #[cfg(target_os = "linux")]
    {
        let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        unsafe {
            if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
                let target = 65536;
                if limit.rlim_max < target {
                    limit.rlim_max = target;
                }
                if limit.rlim_cur < target {
                    limit.rlim_cur = target;
                    if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) == 0 {
                        println!("🚀 [FD BOOST] System file descriptors boosted successfully to {}", target);
                        return;
                    }
                }
            }
        }
    }
    println!("ℹ️ [FD BOOST] Retaining system default file descriptor limits.");
}

#[tokio::main]
async fn main() {
    boost_fd_limit();
    let args = Args::parse();

    println!("⚡==================================================⚡");
    println!("🔋 OMEGA DRIVE 3.0 – NEXT.JS & REACT ACCELERATOR PROXY");
    println!("   Proxy Bind Address : {}:{}", args.bind_ip, args.port);
    println!("   Next.js Upstream   : {}:{}", args.upstream_ip, args.upstream_port);
    println!("   Omega DB UDS Path  : {}", args.uds_path);
    println!("   Omega DB Port      : {}", args.db_port);
    println!("⚡==================================================⚡");

    let pool = UdsPool::new(128, &args.uds_path).await;
    
    // Bind TcpListener using socket2 to configure a large custom listen backlog
    let listen_address = format!("{}:{}", args.bind_ip, args.port);
    let address: std::net::SocketAddr = match listen_address.parse() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("❌ Fatal error parsing bind address: {}", e);
            std::process::exit(1);
        }
    };

    let domain = if address.is_ipv6() {
        socket2::Domain::IPV6
    } else {
        socket2::Domain::IPV4
    };

    let socket = match socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("❌ Fatal error creating socket: {}", e);
            std::process::exit(1);
        }
    };

    if let Err(e) = socket.set_reuse_address(true) {
        eprintln!("⚠️ Warning setting reuse address: {}", e);
    }

    if let Err(e) = socket.bind(&socket2::SockAddr::from(address)) {
        eprintln!("❌ Fatal error binding port: {}", e);
        std::process::exit(1);
    }

    // Listen with a custom large backlog (8192) to completely eliminate SYN queue overflow timeouts under 10k connections
    if let Err(e) = socket.listen(8192) {
        eprintln!("❌ Fatal error listening on socket: {}", e);
        std::process::exit(1);
    }

    let std_listener: std::net::TcpListener = socket.into();
    let listener = match TcpListener::from_std(std_listener) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("❌ Fatal error converting TcpListener: {}", e);
            std::process::exit(1);
        }
    };

    let shared_pool = pool;
    let shared_args = Arc::new(args);

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let pool_ref = Arc::clone(&shared_pool);
                let args_ref = Arc::clone(&shared_args);
                tokio::spawn(async move {
                    let _ = socket.set_nodelay(true);
                    handle_client(socket, pool_ref, args_ref).await;
                });
            }
            Err(e) => {
                eprintln!("⚠️ [SOCKET ERROR] Accept failed: {}. Throttling for 5ms...", e);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }
}

async fn handle_client(
    mut socket: tokio::net::TcpStream,
    pool: Arc<UdsPool>,
    args: Arc<Args>,
) {
    let mut read_buffer = BytesMut::with_capacity(2048);

    loop {
        let mut headers_end = 0;

        // 1. Read HTTP request line and headers
        loop {
            // Check if we already have the headers in the read buffer
            for i in 0..read_buffer.len().saturating_sub(3) {
                if &read_buffer[i..i+4] == b"\r\n\r\n" {
                    headers_end = i + 4;
                    break;
                }
            }
            if headers_end > 0 {
                break;
            }
            if read_buffer.len() > 16384 {
                return; // Request entity too large
            }

            let n = match socket.read_buf(&mut read_buffer).await {
                Ok(n) if n > 0 => n,
                _ => return, // EOF or socket error
            };
        }

        // Split out the header bytes
        let req_bytes = read_buffer.split_to(headers_end);
        let req_str = String::from_utf8_lossy(&req_bytes);
        let mut lines = req_str.lines();

        let req_line = match lines.next() {
            Some(line) => line,
            None => return,
        };

        let parts: Vec<&str> = req_line.split_whitespace().collect();
        if parts.len() < 3 {
            return;
        }

        let method = parts[0];
        let uri = parts[1];

        let mut host = String::new();
        let mut is_googlebot = false;
        let mut has_session_cookie = false;

        // Parse essential headers
        for line in lines {
            if line.is_empty() { break; }
            let lower = line.to_lowercase();
            if lower.starts_with("host:") {
                host = line["host:".len()..].trim().to_string();
            } else if lower.starts_with("user-agent:") {
                let ua = &lower["user-agent:".len()..];
                if ua.contains("googlebot") || ua.contains("bingbot") || ua.contains("lighthouse") || ua.contains("chrome-lighthouse") {
                    is_googlebot = true;
                }
            } else if lower.starts_with("cookie:") {
                let cookie_val = &lower["cookie:".len()..];
                if cookie_val.contains("session") || cookie_val.contains("auth") || cookie_val.contains("token") || cookie_val.contains("jwt") || cookie_val.contains("sid") {
                    has_session_cookie = true;
                }
            }
        }

        if host.is_empty() {
            host = "localhost".to_string();
        }

        let is_get = method == "GET";
        let clean_uri = uri.split('?').next().unwrap_or(uri);

        // Static Asset identification
        let is_static = clean_uri.ends_with(".css") || clean_uri.ends_with(".js") || clean_uri.ends_with(".png") ||
                        clean_uri.ends_with(".jpg") || clean_uri.ends_with(".jpeg") || clean_uri.ends_with(".svg") ||
                        clean_uri.ends_with(".webp") || clean_uri.ends_with(".gif") || clean_uri.ends_with(".woff2") ||
                        clean_uri.ends_with(".woff") || clean_uri.ends_with(".ttf") || clean_uri.ends_with(".ico");

        let is_dynamic_api = uri.contains("/api/auth/") || uri.contains("/_next/data/") || method != "GET";

        let mut served_from_cache = false;

        if is_get && !is_dynamic_api && !has_session_cookie {
            let cache_key = if is_static {
                format!("next_cache:asset:{}", uri)
            } else {
                // MD5 of host + URI for HTML / Pages
                let md5_input = format!("{}{}", host, uri);
                let digest = md5::compute(md5_input.as_bytes());
                format!("next_cache:page:{:x}", digest)
            };

            let mut final_bitstream = Vec::new();
            let mut cache_hit = false;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            // 1. Check in-memory L1 Cache
            if let Some(entry) = L1_CACHE.get(&cache_key) {
                let (cached_bytes, expire_at) = entry.value();
                if *expire_at == 0 || now <= *expire_at {
                    final_bitstream = cached_bytes.clone();
                    cache_hit = true;
                } else {
                    L1_CACHE.remove(&cache_key);
                }
            }

            // 2. Fallback to L2 Cache (Omega DB)
            if !cache_hit {
                if let Some(cached_bitstream) = get_from_matrix(&pool, &cache_key, args.db_port).await {
                    let mut is_expired = false;
                    let mut db_bytes = &cached_bitstream[..];
                    let mut expire_at = 0;

                    if cached_bitstream.len() >= 8 {
                        let mut expire_bytes = [0u8; 8];
                        expire_bytes.copy_from_slice(&cached_bitstream[..8]);
                        expire_at = u64::from_be_bytes(expire_bytes);
                        
                        if expire_at > 0 {
                            if now > expire_at {
                                is_expired = true;
                                let _ = save_to_matrix(&pool, &cache_key, &[], args.db_port).await; // Evict in L2
                            } else {
                                db_bytes = &cached_bitstream[8..];
                                // Insert into local L1
                                L1_CACHE.insert(cache_key.clone(), (db_bytes.to_vec(), expire_at));
                            }
                        }
                    }

                    if !is_expired {
                        final_bitstream = db_bytes.to_vec();
                        cache_hit = true;
                    }
                }
            }

            // 3. Serve the cached packet
            if cache_hit {
                let _ = socket.write_all(&final_bitstream).await;
                let _ = socket.shutdown().await;
                return;
            }
        }

        // 2. Cache Miss or Dynamic Request -> Proxy to Next.js Upstream
        if !served_from_cache {
            if let Ok(mut upstream) = tokio::net::TcpStream::connect(format!("{}:{}", args.upstream_ip, args.upstream_port)).await {
                let _ = upstream.set_nodelay(true);
                
                if upstream.write_all(&req_bytes).await.is_err() { return; }
                
                // Forward POST body bytes currently in the read buffer
                if !read_buffer.is_empty() {
                    let body_bytes = read_buffer.split();
                    if upstream.write_all(&body_bytes).await.is_err() { return; }
                }

                // Read response from Next.js
                let mut resp_buf = BytesMut::with_capacity(8192);
                let mut upstream_closed = false;

                if is_dynamic_api || !is_get {
                    let _ = tokio::io::copy_bidirectional(&mut socket, &mut upstream).await;
                    return;
                }

                // For GET requests, we capture and cache HTML & static responses dynamically
                let mut resp_headers_end = 0;
                loop {
                    let n = match upstream.read_buf(&mut resp_buf).await {
                        Ok(n) if n > 0 => n,
                        _ => {
                            upstream_closed = true;
                            break;
                        },
                    };
                    
                    for i in 0..resp_buf.len().saturating_sub(3) {
                        if &resp_buf[i..i+4] == b"\r\n\r\n" {
                            resp_headers_end = i + 4;
                            break;
                        }
                    }
                    if resp_headers_end > 0 { break; }
                }

                if resp_headers_end > 0 {
                    let resp_headers_str = String::from_utf8_lossy(&resp_buf[..resp_headers_end]);
                    let mut lines = resp_headers_str.lines();
                    let status_line = lines.next().unwrap_or("");
                    let is_200 = status_line.contains("200");

                    let mut is_cacheable = false;
                    let mut cache_ttl: u64 = 300; // Default to 5 minutes
                    let mut is_chunked = false;
                    let mut content_length: Option<usize> = None;

                    for line in lines {
                        let lower = line.to_lowercase();
                        if lower.starts_with("content-length:") {
                            if let Some(len) = lower["content-length:".len()..].trim().parse::<usize>().ok() {
                                content_length = Some(len);
                            }
                        } else if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
                            is_chunked = true;
                        } else if lower.starts_with("cache-control:") {
                            if !lower.contains("no-store") && !lower.contains("no-cache") && !lower.contains("private") {
                                is_cacheable = is_200;
                                if let Some(s_maxage) = lower.split("s-maxage=").nth(1) {
                                    if let Some(secs) = s_maxage.split(',').next().and_then(|s| s.parse::<u64>().ok()) {
                                        cache_ttl = secs;
                                    }
                                } else if let Some(max_age) = lower.split("max-age=").nth(1) {
                                    if let Some(secs) = max_age.split(',').next().and_then(|s| s.parse::<u64>().ok()) {
                                        cache_ttl = secs;
                                    }
                                }
                            }
                        } else if lower.starts_with("x-omega-cache:") {
                            is_cacheable = lower.contains("true");
                        } else if lower.starts_with("x-omega-ttl:") {
                            if let Some(secs) = lower["x-omega-ttl:".len()..].trim().parse::<u64>().ok() {
                                cache_ttl = secs;
                            }
                        }
                    }

                    if is_200 && !is_cacheable && (resp_headers_str.contains("Content-Type: text/html") || is_static) {
                        is_cacheable = true;
                    }

                    let mut body_bytes = Vec::new();
                    body_bytes.extend_from_slice(&resp_buf[resp_headers_end..]);

                    if !upstream_closed {
                        let mut temp_buf = [0u8; 16384];
                        while body_bytes.len() < content_length.unwrap_or(usize::MAX) {
                            if is_chunked && (body_bytes.ends_with(b"0\r\n\r\n") || body_bytes.ends_with(b"\r\n0\r\n\r\n")) {
                                break;
                            }
                            match upstream.read(&mut temp_buf).await {
                                Ok(0) => break,
                                Ok(n) => {
                                    body_bytes.extend_from_slice(&temp_buf[..n]);
                                }
                                Err(_) => break,
                            }
                        }
                    }

                    let final_body = if is_chunked {
                        if let Some(dec) = dechunk(&body_bytes) {
                            dec
                        } else {
                            body_bytes.clone()
                        }
                    } else {
                        body_bytes.clone()
                    };

                    if is_cacheable {
                        let cache_key = if is_static {
                            format!("next_cache:asset:{}", uri)
                        } else {
                            let md5_input = format!("{}{}", host, uri);
                            let digest = md5::compute(md5_input.as_bytes());
                            format!("next_cache:page:{:x}", digest)
                        };

                        let mime_type = get_mime_type(clean_uri);
                        let is_gzip = final_body.len() >= 2 && final_body[0] == 0x1f && final_body[1] == 0x8b;

                        let mut packed_resp = Vec::new();
                        packed_resp.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
                        packed_resp.extend_from_slice(format!("Content-Type: {}\r\n", mime_type).as_bytes());
                        packed_resp.extend_from_slice(format!("Content-Length: {}\r\n", final_body.len()).as_bytes());
                        if is_gzip {
                            packed_resp.extend_from_slice(b"Content-Encoding: gzip\r\n");
                        }
                        packed_resp.extend_from_slice(b"X-Omega-Status: HYPER-HIT\r\n");
                        packed_resp.extend_from_slice(b"Access-Control-Allow-Origin: *\r\n");
                        packed_resp.extend_from_slice(b"Connection: close\r\n\r\n");
                        packed_resp.extend_from_slice(&final_body);

                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let expire_at = now + cache_ttl;

                        // L1 cache stores packed_resp directly without prefix!
                        L1_CACHE.insert(cache_key.clone(), (packed_resp.clone(), expire_at));

                        // L2 cache (Omega DB) stores binary prefix (8 bytes expire_at) + packed_resp!
                        let mut l2_bytes = Vec::with_capacity(8 + packed_resp.len());
                        l2_bytes.extend_from_slice(&expire_at.to_be_bytes());
                        l2_bytes.extend_from_slice(&packed_resp);
                        let _ = save_to_matrix(&pool, &cache_key, &l2_bytes, args.db_port).await;
                    }

                    let mut clean_headers = String::new();
                    for line in resp_headers_str.lines() {
                        if line.is_empty() { break; }
                        let lower = line.to_lowercase();
                        if !lower.starts_with("connection:") 
                            && !lower.starts_with("keep-alive:") 
                            && !lower.starts_with("transfer-encoding:")
                            && !lower.starts_with("content-length:") 
                        {
                            clean_headers.push_str(line);
                            clean_headers.push_str("\r\n");
                        }
                    }
                    clean_headers.push_str(&format!("Content-Length: {}\r\n", final_body.len()));
                    clean_headers.push_str("Connection: close\r\n\r\n");

                    let mut client_resp = Vec::new();
                    client_resp.extend_from_slice(clean_headers.as_bytes());
                    client_resp.extend_from_slice(&final_body);
                    let _ = socket.write_all(&client_resp).await;
                    let _ = socket.shutdown().await;
                }
                return;
            }
            return;
        }
    }
}
