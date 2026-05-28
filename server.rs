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
use dashmap::DashMap;
use bytes::{BytesMut, Buf};
use std::fs;
use std::path::Path;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;

struct UdsPool {
    connections: std::sync::Mutex<Vec<UnixStream>>,
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl UdsPool {
    async fn new(size: usize) -> Arc<Self> {
        let mut conns = Vec::with_capacity(size);
        for _ in 0..size {
            if let Ok(stream) = UnixStream::connect("/tmp/airdb.sock").await {
                conns.push(stream);
            }
        }
        let actual_size = conns.len();
        println!("🔋 [UDS POOL] Initialized with {}/{} active connections", actual_size, size);
        Arc::new(UdsPool {
            connections: std::sync::Mutex::new(conns),
            semaphore: Arc::new(tokio::sync::Semaphore::new(actual_size)),
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
            if let Ok(stream) = UnixStream::connect("/tmp/airdb.sock").await {
                self.pool.connections.lock().unwrap().push(stream);
            }
        }
    }
}

async fn connect_to_matrix() -> Option<tokio::net::TcpStream> {
    tokio::net::TcpStream::connect("127.0.0.1:6380").await.ok()
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

enum RespResult {
    Hit(Vec<u8>),
    Miss,
    Error,
}

async fn read_resp_bulk<S>(stream: &mut S) -> RespResult
where
    S: tokio::io::AsyncReadExt + Unpin,
{
    if let Some(line) = read_line(stream).await {
        if line.starts_with('$') {
            let bulk_len: isize = line[1..].trim().parse().unwrap_or(-1);
            if bulk_len == -1 {
                return RespResult::Miss;
            }
            if bulk_len >= 0 {
                let mut data = vec![0u8; bulk_len as usize];
                if stream.read_exact(&mut data).await.is_ok() {
                    let mut crlf = [0u8; 2];
                    if stream.read_exact(&mut crlf).await.is_ok() {
                        return RespResult::Hit(data);
                    }
                }
            }
        } else if line.starts_with('-') {
            return RespResult::Miss;
        }
    }
    RespResult::Error
}

async fn get_from_matrix(pool: &Arc<UdsPool>, key: &str) -> Option<Vec<u8>> {
    if let Ok(Some((mut uds, guard))) = tokio::time::timeout(std::time::Duration::from_millis(50), pool.get()).await {
        let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
        if uds.write_all(cmd.as_bytes()).await.is_ok() {
            match read_resp_bulk(&mut uds).await {
                RespResult::Hit(res) => {
                    guard.release(uds).await;
                    return Some(res);
                }
                RespResult::Miss => {
                    guard.release(uds).await;
                    return None;
                }
                RespResult::Error => {}
            }
        }
        let mut broken_guard = guard;
        broken_guard.is_healthy = false;
        broken_guard.release(uds).await;
    }

    if let Some(mut tcp) = connect_to_matrix().await {
        let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
        if tcp.write_all(cmd.as_bytes()).await.is_ok() {
            if let RespResult::Hit(res) = read_resp_bulk(&mut tcp).await {
                return Some(res);
            }
        }
    }
    None
}


async fn save_to_matrix(pool: &Arc<UdsPool>, key: &str, data: &[u8]) -> Option<()> {
    if let Ok(Some((mut uds, guard))) = tokio::time::timeout(std::time::Duration::from_millis(50), pool.get()).await {
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

    if let Some(mut tcp) = connect_to_matrix().await {
        let cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n", key.len(), key, data.len());
        if tcp.write_all(cmd.as_bytes()).await.is_ok() {
            let _ = tcp.write_all(data).await;
            let _ = tcp.write_all(b"\r\n").await;
            return Some(());
        }
    }
    None
}



async fn delete_from_matrix(pool: &Arc<UdsPool>, key: &str) -> Option<()> {
    if let Ok(Some((mut uds, guard))) = tokio::time::timeout(std::time::Duration::from_millis(50), pool.get()).await {
        let cmd = format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key.len(), key);
        if uds.write_all(cmd.as_bytes()).await.is_ok() {
            if read_line(&mut uds).await.is_some() {
                guard.release(uds).await;
                return Some(());
            }
        }
        let mut broken_guard = guard;
        broken_guard.is_healthy = false;
        broken_guard.release(uds).await;
    }

    if let Some(mut tcp) = connect_to_matrix().await {
        let cmd = format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key.len(), key);
        if tcp.write_all(cmd.as_bytes()).await.is_ok() {
            return Some(());
        }
    }
    None
}

fn gzip_compress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    encoder.finish()
}

fn get_mime_type(uri: &str) -> &'static str {
    let lower = uri.to_lowercase();
    if lower.ends_with(".html") { "text/html; charset=UTF-8" }
    else if lower.ends_with(".css") { "text/css" }
    else if lower.ends_with(".js") { "application/javascript" }
    else if lower.ends_with(".png") { "image/png" }
    else if lower.ends_with(".jpg") || lower.ends_with(".jpeg") { "image/jpeg" }
    else if lower.ends_with(".svg") { "image/svg+xml" }
    else if lower.ends_with(".webp") { "image/webp" }
    else if lower.ends_with(".gif") { "image/gif" }
    else if lower.ends_with(".woff2") { "font/woff2" }
    else if lower.ends_with(".woff") { "font/woff" }
    else if lower.ends_with(".ttf") { "font/ttf" }
    else if lower.ends_with(".otf") { "font/otf" }
    else if lower.ends_with(".eot") { "application/vnd.ms-fontobject" }
    else { "application/octet-stream" }
}

#[cfg(unix)]
fn boost_fd_limit() {
    unsafe {
        let mut rlim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            let old_cur = rlim.rlim_cur;
            let target = std::cmp::max(rlim.rlim_max, 65536);
            rlim.rlim_cur = target;
            rlim.rlim_max = target;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) != 0 {
                rlim.rlim_cur = rlim.rlim_max;
                if libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) != 0 {
                    rlim.rlim_cur = old_cur;
                    let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &rlim);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    boost_fd_limit();

    use socket2::{Socket, Domain, Type, Protocol, SockAddr};
    use std::net::SocketAddr;

    let addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true).unwrap_or(());
    socket.bind(&SockAddr::from(addr))?;
    socket.listen(16384)?; // MASSIVE BACKLOG
    socket.set_nonblocking(true)?;
    let listener = TcpListener::from_std(socket.into())?;

    // Pre-initialize a thread-safe connection pool with 128 persistent connections
    let pool = UdsPool::new(128).await;

    println!("🚀 OMEGA DRIVE NEURAL REVERSE-PROXY 3.0 ONLINE");
    println!("📍 Entrance: http://127.0.0.1:8080 -> Upstream: 127.0.0.1:8081");

    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::WouldBlock && e.kind() != std::io::ErrorKind::ConnectionAborted {
                    eprintln!("[WARN] Accept error: {}. Sleeping 5ms...", e);
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                continue;
            }
        };
        let pool = Arc::clone(&pool);
        
        tokio::spawn(async move {
            let _ = socket.set_nodelay(true);
            let mut read_buffer = BytesMut::with_capacity(65536);
            
            loop {
                let n = match socket.read_buf(&mut read_buffer).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };

                let mut headers_end = 0;
                for i in 0..read_buffer.len().saturating_sub(3) {
                    if &read_buffer[i..i+4] == b"\r\n\r\n" {
                        headers_end = i + 4;
                        break;
                    }
                }

                if headers_end == 0 {
                    if read_buffer.len() > 8192 { return; }
                    continue;
                }

                let request_str = String::from_utf8_lossy(&read_buffer[..headers_end]);
                let first_line = request_str.lines().next().unwrap_or("");
                let mut parts = first_line.split_whitespace();
                let method = parts.next().unwrap_or("GET");
                let uri = parts.next().unwrap_or("/");

                let mut host = String::new();
                let mut cookie_header = String::new();
                let mut supports_webp = false;
                for line in request_str.lines() {
                    let lower = line.to_lowercase();
                    if lower.starts_with("host:") {
                        host = line["host:".len()..].trim().to_string();
                    } else if lower.starts_with("cookie:") {
                        cookie_header = line["cookie:".len()..].trim().to_string();
                    } else if lower.starts_with("accept:") {
                        if lower.contains("image/webp") {
                            supports_webp = true;
                        }
                    }
                }
                if !supports_webp && request_str.to_lowercase().contains("image/webp") {
                    supports_webp = true;
                }
                if host.is_empty() {
                    host = "localhost:8080".to_string();
                }

                let is_get = method == "GET";
                let clean_uri = uri.split('?').next().unwrap_or(uri);
                let is_static = clean_uri.ends_with(".css") || clean_uri.ends_with(".js") || clean_uri.ends_with(".png") ||
                                clean_uri.ends_with(".jpg") || clean_uri.ends_with(".jpeg") || clean_uri.ends_with(".svg") ||
                                clean_uri.ends_with(".webp") || clean_uri.ends_with(".gif") || clean_uri.ends_with(".woff") ||
                                clean_uri.ends_with(".woff2") || clean_uri.ends_with(".ttf") || clean_uri.ends_with(".otf") ||
                                clean_uri.ends_with(".eot") || clean_uri.starts_with("/omega-ext/");

                let is_admin = uri.contains("/wp-admin") || uri.contains("/wp-login.php") || uri.contains("nocache");
                let is_checkout_or_cart = uri.contains("/cart") || uri.contains("/checkout") || uri.contains("/my-account") ||
                                          uri.contains("page_id=8") || uri.contains("page_id=9") || uri.contains("page_id=10");
                let is_wc_ajax = uri.contains("wc-ajax=");
                let is_rest_api = uri.contains("wp-json") || uri.contains("rest_route=");
                
                let is_bypass_requested = if is_static {
                    false
                } else {
                    is_admin || is_checkout_or_cart || is_wc_ajax || is_rest_api
                };
                
                if is_bypass_requested {
                    println!("[DEBUG PROXY] BYPASS REQUESTED! Reason: admin={}, cart_path={}, ajax={}", 
                        is_admin, is_checkout_or_cart, is_wc_ajax);
                }
                
                let mut served_from_cache = false;

                if is_get && !is_bypass_requested {

                    if is_static {
                        let is_convertible_image = clean_uri.ends_with(".jpg") || clean_uri.ends_with(".jpeg") || clean_uri.ends_with(".png");
                        let is_omega_ext = clean_uri.starts_with("/omega-ext/");
                        
                        let cache_key = if is_omega_ext {
                            format!("asset:{}", clean_uri)
                        } else if is_convertible_image && supports_webp {
                            format!("asset:webp:ttl:{}", uri)
                        } else {
                            format!("asset:ttl:{}", uri)
                        };

                        if let Some(cached_data) = get_from_matrix(&pool, &cache_key).await {
                            let mut is_expired = false;
                            let mut actual_data = &cached_data[..];
                            
                            // Check for 8-byte expiration timestamp on TTL-enabled static asset caches
                            let has_ttl = !is_omega_ext && (cache_key.starts_with("asset:webp:ttl:") || cache_key.starts_with("asset:ttl:"));
                            if has_ttl && cached_data.len() >= 8 {
                                let mut expire_bytes = [0u8; 8];
                                expire_bytes.copy_from_slice(&cached_data[..8]);
                                let expire_at = u64::from_be_bytes(expire_bytes);
                                
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs();
                                
                                if now > expire_at {
                                    is_expired = true;
                                    println!("[DEBUG STATIC] Cache expired for key '{}' (TTL reached). Evicting from omega!", cache_key);
                                    let _ = delete_from_matrix(&pool, &cache_key).await;
                                } else {
                                    actual_data = &cached_data[8..];
                                }
                            }

                            if !is_expired {
                                let mime_type = if cache_key.starts_with("asset:webp:") {
                                    "image/webp"
                                } else {
                                    get_mime_type(clean_uri)
                                };
                                let is_gzip_data = actual_data.len() >= 2 && actual_data[0] == 0x1F && actual_data[1] == 0x8B;

                                let mut response = Vec::new();
                                response.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
                                response.extend_from_slice(format!("Content-Type: {}\r\n", mime_type).as_bytes());
                                response.extend_from_slice(format!("Content-Length: {}\r\n", actual_data.len()).as_bytes());
                                response.extend_from_slice(b"Cache-Control: public, max-age=31536000, immutable\r\n");
                                if is_gzip_data {
                                    response.extend_from_slice(b"Content-Encoding: gzip\r\n");
                                }
                                response.extend_from_slice(b"Access-Control-Allow-Origin: *\r\n");
                                response.extend_from_slice(b"X-Omega-Status: ASSET-HIT\r\n");
                                response.extend_from_slice(b"Connection: keep-alive\r\n\r\n");
                                response.extend_from_slice(actual_data);
                                
                                if socket.write_all(&response).await.is_ok() {
                                    served_from_cache = true;
                                }
                            }
                        }
                    } else {
                        let md5_input = format!("{}{}", host, uri);
                        let digest = md5::compute(md5_input.as_bytes());
                        let hash_str = format!("{:x}", digest);
                        
                        let hyper_key = format!("hyper_matrix:{}", hash_str);
                        let wp_key = format!("wp_cache:{}", hash_str);
                        
                        println!("[DEBUG PROXY] Host: '{}', URI: '{}'", host, uri);
                        println!("[DEBUG PROXY] MD5 Input: '{}'", md5_input);
                        println!("[DEBUG PROXY] Hash Str: '{}'", hash_str);
                        
                        let cached_html = if let Some(html) = get_from_matrix(&pool, &hyper_key).await {
                            Some(html)
                        } else {
                            get_from_matrix(&pool, &wp_key).await
                        };

                        if let Some(cached_html) = cached_html {
                            println!("[DEBUG PROXY] CACHE HIT! LEN: {}", cached_html.len());
                            let is_gzip = cached_html.len() >= 2 && cached_html[0] == 0x1f && cached_html[1] == 0x8b;
                            
                            let mut response = Vec::new();
                            response.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
                            response.extend_from_slice(b"Content-Type: text/html; charset=UTF-8\r\n");
                            response.extend_from_slice(format!("Content-Length: {}\r\n", cached_html.len()).as_bytes());
                            if is_gzip {
                                response.extend_from_slice(b"Content-Encoding: gzip\r\n");
                            }
                            response.extend_from_slice(b"Access-Control-Allow-Origin: *\r\n");
                            response.extend_from_slice(b"X-Omega-Status: HYPER-HIT\r\n");
                            response.extend_from_slice(b"Connection: keep-alive\r\n\r\n");
                            response.extend_from_slice(&cached_html);
                            
                            if socket.write_all(&response).await.is_ok() {
                                served_from_cache = true;
                            }
                        } else {
                            println!("[DEBUG PROXY] CACHE MISS!");
                        }
                    }
                }

                if !served_from_cache {
                    if let Ok(mut upstream) = tokio::net::TcpStream::connect("127.0.0.1:8081").await {
                        // Modify connection headers to "Connection: close" for bypassed/dynamic requests
                        // to prevent copy_bidirectional from hanging indefinitely on Keep-Alive!
                        let req_bytes = &read_buffer[..headers_end];
                        let req_str = String::from_utf8_lossy(req_bytes);
                        let mut modified_req = req_str.replace("Connection: keep-alive", "Connection: close")
                                                      .replace("Connection: Keep-Alive", "Connection: close")
                                                      .replace("connection: keep-alive", "Connection: close");
                        if !modified_req.contains("Connection:") {
                            modified_req = modified_req.replace("\r\n\r\n", "\r\nConnection: close\r\n\r\n");
                        }

                        if upstream.write_all(modified_req.as_bytes()).await.is_err() { return; }
                        
                        // Write any remaining POST body bytes currently in the read_buffer to upstream
                        if read_buffer.len() > headers_end {
                            if upstream.write_all(&read_buffer[headers_end..]).await.is_err() { return; }
                        }
                        
                        let clean_uri = uri.split('?').next().unwrap_or(uri);
                        let is_static_get = is_get && !is_admin && (
                            clean_uri.ends_with(".css") || clean_uri.ends_with(".js") || clean_uri.ends_with(".png") ||
                            clean_uri.ends_with(".jpg") || clean_uri.ends_with(".jpeg") || clean_uri.ends_with(".svg") ||
                            clean_uri.ends_with(".webp") || clean_uri.ends_with(".gif") || clean_uri.ends_with(".woff") ||
                            clean_uri.ends_with(".woff2") || clean_uri.ends_with(".ttf") || clean_uri.starts_with("/omega-ext/")
                        );

                        if is_static_get {
                            let mut resp_buf = BytesMut::with_capacity(65536);
                            let mut resp_headers_end = 0;
                            
                            loop {
                                let n = match upstream.read_buf(&mut resp_buf).await {
                                    Ok(n) if n > 0 => n,
                                    _ => break,
                                };
                                
                                for i in 0..resp_buf.len().saturating_sub(3) {
                                    if &resp_buf[i..i+4] == b"\r\n\r\n" {
                                        resp_headers_end = i + 4;
                                        break;
                                    }
                                }
                                if resp_headers_end > 0 { break; }
                            }
                            
                            let mut fallback_success = false;
                            let mut final_resp_headers = String::new();
                            let mut final_body_bytes = Vec::new();
                            
                            if resp_headers_end > 0 {
                                let resp_headers_str = String::from_utf8_lossy(&resp_buf[..resp_headers_end]);
                                let first_resp_line = resp_headers_str.lines().next().unwrap_or("");
                                let is_200 = first_resp_line.contains("200");
                                
                                if !is_200 && clean_uri.ends_with(".webp") && clean_uri.starts_with("/wp-content/uploads/") {
                                    println!("[DEBUG STATIC] WebP request 404 upstream. Initiating transparent fallback for: {}", uri);
                                    if let Some((headers, body)) = try_fallback_upstream(uri, &read_buffer[..headers_end], ".jpg").await {
                                        final_resp_headers = headers;
                                        final_body_bytes = body;
                                        fallback_success = true;
                                    } else if let Some((headers, body)) = try_fallback_upstream(uri, &read_buffer[..headers_end], ".png").await {
                                        final_resp_headers = headers;
                                        final_body_bytes = body;
                                        fallback_success = true;
                                    } else if let Some((headers, body)) = try_fallback_upstream(uri, &read_buffer[..headers_end], ".jpeg").await {
                                        final_resp_headers = headers;
                                        final_body_bytes = body;
                                        fallback_success = true;
                                    }
                                }
                                
                                if !fallback_success {
                                    final_resp_headers = resp_headers_str.to_string();
                                    final_body_bytes.extend_from_slice(&resp_buf[resp_headers_end..]);
                                    
                                    let mut temp_buf = vec![0u8; 8192];
                                    loop {
                                        match upstream.read(&mut temp_buf).await {
                                            Ok(n) if n > 0 => {
                                                final_body_bytes.extend_from_slice(&temp_buf[..n]);
                                            }
                                            _ => break,
                                        }
                                    }
                                }
                                
                                let is_200_final = final_resp_headers.lines().next().unwrap_or("").contains("200");
                                let mut content_len = final_body_bytes.len();
                                let mut already_gzipped = false;
                                let mut is_html = false;
                                for line in final_resp_headers.lines() {
                                    let lower = line.to_lowercase();
                                    if lower.starts_with("content-encoding:") && lower.contains("gzip") {
                                        already_gzipped = true;
                                    }
                                    if lower.starts_with("content-type:") && lower.contains("text/html") {
                                        is_html = true;
                                    }
                                }
                                
                                if is_200_final && content_len > 0 && !is_html {
                                    let mut body_bytes = final_body_bytes;
                                    if fallback_success {
                                        println!("[DEBUG STATIC] Fallback success. Converting JPG/PNG response to WebP on the fly!");
                                        if let Ok(img) = image::load_from_memory(&body_bytes) {
                                            if let Ok(encoder) = webp::Encoder::from_image(&img) {
                                                let webp_bytes = encoder.encode(82.0).to_vec();
                                                body_bytes = webp_bytes;
                                                content_len = body_bytes.len();
                                                already_gzipped = false;
                                            }
                                        }
                                    }
                                    
                                    let is_compressible = clean_uri.ends_with(".css") || clean_uri.ends_with(".js") || clean_uri.ends_with(".svg");
                                    let is_convertible_image = clean_uri.ends_with(".jpg") || clean_uri.ends_with(".jpeg") || clean_uri.ends_with(".png");
                                    
                                    let mut final_data = body_bytes.clone();
                                    let mut mime_type = get_mime_type(clean_uri).to_string();
                                    let mut is_webp_served = false;
                                    
                                    if is_convertible_image && supports_webp {
                                         if let Ok(img) = image::load_from_memory(&body_bytes) {
                                             if let Ok(encoder) = webp::Encoder::from_image(&img) {
                                                 let webp_bytes = encoder.encode(82.0).to_vec();
                                                 let key = format!("asset:webp:ttl:{}", uri);
                                                 
                                                 // Prepend 8-byte expiration timestamp
                                                 let expire_at = std::time::SystemTime::now()
                                                     .duration_since(std::time::UNIX_EPOCH)
                                                     .unwrap_or_default()
                                                     .as_secs() + 604800; // 7 days
                                                 let mut cache_payload = Vec::with_capacity(8 + webp_bytes.len());
                                                 cache_payload.extend_from_slice(&expire_at.to_be_bytes());
                                                 cache_payload.extend_from_slice(&webp_bytes);
                                                 
                                                 println!("[DEBUG STATIC] SAVING NATIVE WEBP TO CACHE WITH 7D TTL! Key: {}, Size: {}", key, webp_bytes.len());
                                                 let _ = save_to_matrix(&pool, &key, &cache_payload).await;
                                                 
                                                 final_data = webp_bytes;
                                                 mime_type = "image/webp".to_string();
                                                 is_webp_served = true;
                                             }
                                         }
                                    }
                                    
                                    if !is_webp_served {
                                         let is_omega_ext = clean_uri.starts_with("/omega-ext/");
                                         let final_compressible_data = if is_compressible && !already_gzipped {
                                             gzip_compress(&body_bytes).unwrap_or(body_bytes.clone())
                                         } else {
                                             body_bytes.clone()
                                         };
                                         
                                         if is_omega_ext {
                                             let key = format!("asset:{}", clean_uri);
                                             println!("[DEBUG STATIC] SAVING OMEGA-EXT TO CACHE! Key: {}, Size: {}", key, final_compressible_data.len());
                                             let _ = save_to_matrix(&pool, &key, &final_compressible_data).await;
                                         } else {
                                             let key = format!("asset:ttl:{}", uri);
                                             // Prepend 8-byte expiration timestamp
                                             let expire_at = std::time::SystemTime::now()
                                                 .duration_since(std::time::UNIX_EPOCH)
                                                 .unwrap_or_default()
                                                 .as_secs() + 604800; // 7 days
                                             let mut cache_payload = Vec::with_capacity(8 + final_compressible_data.len());
                                             cache_payload.extend_from_slice(&expire_at.to_be_bytes());
                                             cache_payload.extend_from_slice(&final_compressible_data);
                                             
                                             println!("[DEBUG STATIC] SAVING ORIGINAL TO CACHE WITH 7D TTL! Key: {}, Size: {}", key, final_compressible_data.len());
                                             let _ = save_to_matrix(&pool, &key, &cache_payload).await;
                                         }
                                         
                                         final_data = final_compressible_data;
                                    }
                                        
                                        let is_gzipped = !is_convertible_image && (already_gzipped || (is_compressible && !already_gzipped));
                                        
                                        let mut response = Vec::new();
                                        response.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
                                        response.extend_from_slice(format!("Content-Type: {}\r\n", mime_type).as_bytes());
                                        response.extend_from_slice(format!("Content-Length: {}\r\n", final_data.len()).as_bytes());
                                        response.extend_from_slice(b"Cache-Control: public, max-age=31536000, immutable\r\n");
                                        if is_gzipped {
                                            response.extend_from_slice(b"Content-Encoding: gzip\r\n");
                                        }
                                        response.extend_from_slice(b"Access-Control-Allow-Origin: *\r\n");
                                        response.extend_from_slice(b"X-Omega-Status: ASSET-CACHED\r\n");
                                        response.extend_from_slice(b"Connection: keep-alive\r\n\r\n");
                                        response.extend_from_slice(&final_data);
                                        
                                        let _ = socket.write_all(&response).await;
                                } else {
                                    let _ = socket.write_all(&resp_buf).await;
                                    let _ = tokio::io::copy_bidirectional(&mut socket, &mut upstream).await;
                                }
                            }
                        } else {
                            let _ = tokio::io::copy_bidirectional(&mut socket, &mut upstream).await;
                        }
                    }
                }

                read_buffer.advance(headers_end);
            }
        });
    }
}

async fn try_fallback_upstream(original_uri: &str, original_req_bytes: &[u8], new_ext: &str) -> Option<(String, Vec<u8>)> {
    let clean_uri = original_uri.split('?').next().unwrap_or(original_uri);
    if !clean_uri.ends_with(".webp") { return None; }
    
    let target_uri = original_uri.replace(".webp", new_ext);
    let req_str = String::from_utf8_lossy(original_req_bytes);
    let modified_req = req_str.replace(original_uri, &target_uri)
                              .replace("Connection: keep-alive", "Connection: close")
                              .replace("Connection: Keep-Alive", "Connection: close")
                              .replace("connection: keep-alive", "Connection: close");
                              
    if let Ok(mut upstream) = tokio::net::TcpStream::connect("127.0.0.1:8081").await {
        let _ = upstream.set_nodelay(true);
        if upstream.write_all(modified_req.as_bytes()).await.is_err() { return None; }
        
        let mut resp_buf = BytesMut::with_capacity(65536);
        let mut resp_headers_end = 0;
        loop {
            let n = match upstream.read_buf(&mut resp_buf).await {
                Ok(n) if n > 0 => n,
                _ => break,
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
            let resp_headers_str = String::from_utf8_lossy(&resp_buf[..resp_headers_end]).to_string();
            let first_resp_line = resp_headers_str.lines().next().unwrap_or("");
            if first_resp_line.contains("200") {
                let mut content_len = 0;
                for line in resp_headers_str.lines() {
                    let lower = line.to_lowercase();
                    if lower.starts_with("content-length:") {
                        content_len = line["content-length:".len()..].trim().parse().unwrap_or(0);
                    }
                }
                
                let mut body_bytes = Vec::new();
                body_bytes.extend_from_slice(&resp_buf[resp_headers_end..]);
                
                let mut temp_buf = vec![0u8; 8192];
                while content_len > 0 && body_bytes.len() < content_len {
                    match upstream.read(&mut temp_buf).await {
                        Ok(n) if n > 0 => {
                            body_bytes.extend_from_slice(&temp_buf[..n]);
                        }
                        _ => break,
                    }
                }
                
                if content_len > 0 && body_bytes.len() >= content_len {
                    body_bytes.truncate(content_len);
                    return Some((resp_headers_str, body_bytes));
                }
            }
        }
    }
    None
}
