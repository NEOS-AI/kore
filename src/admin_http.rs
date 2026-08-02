//! Shared helpers for admin HTTP endpoints (metrics, deadlock UI).
//!
//! Minimal HTTP/1.1 request parsing and response writing — not a full stack.
//! Batch **GM**: optional Bearer / Basic auth, optional TLS, configurable bind.
//! No pipelining; callers close the connection after one response.
//!
//! **Routing convention (accepted):** non-`GET` on a **known** path → `405` +
//! `Allow: GET`; any method on an **unknown** path → `404` (path membership
//! first). `POST /nope` is therefore 404, not 405.
//!
//! **Auth:** when no token/user/password is configured, endpoints stay open
//! (legacy localhost MVP). When any credential is set, requests must pass
//! Bearer token and/or Basic auth or receive `401 Unauthorized`.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::TlsAcceptor;

/// Cap for reading the request line + headers.
pub const MAX_REQUEST_BUF: usize = 8 * 1024;

/// Parsed HTTP request line (method + path without query).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRequestLine {
    /// HTTP method as received (typically uppercase ASCII).
    pub method: String,
    /// Request-target path with query string stripped (e.g. `/metrics`).
    pub path: String,
}

impl ParsedRequestLine {
    /// True when the method is GET (case-insensitive).
    pub fn is_get(&self) -> bool {
        self.method.eq_ignore_ascii_case("GET")
    }
}

/// Full request line + header map (lowercase names) for auth checks.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub line: ParsedRequestLine,
    /// Header names lowercased; values as received (trimmed).
    pub headers: HashMap<String, String>,
}

/// Shared admin HTTP security options (metrics + deadlock UI).
#[derive(Clone, Default)]
pub struct AdminHttpOptions {
    /// Bearer token; empty = disabled.
    pub token: String,
    /// Basic auth username; empty = disabled.
    pub basic_user: String,
    /// Basic auth password (paired with [`basic_user`]).
    pub basic_password: String,
    /// Optional TLS acceptor (None = plain HTTP).
    pub tls: Option<TlsAcceptor>,
    /// Bind host (default `127.0.0.1`).
    pub bind: String,
}

impl AdminHttpOptions {
    pub fn new() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            ..Default::default()
        }
    }

    /// True when callers must present credentials.
    pub fn auth_required(&self) -> bool {
        !self.token.is_empty()
            || (!self.basic_user.is_empty() && !self.basic_password.is_empty())
    }

    /// Scheme label for logs (`https` when TLS is configured).
    pub fn scheme(&self) -> &'static str {
        if self.tls.is_some() {
            "https"
        } else {
            "http"
        }
    }

    /// Validate `Authorization` against configured credentials.
    ///
    /// Open (returns true) when [`auth_required`] is false.
    pub fn authorize(&self, headers: &HashMap<String, String>) -> bool {
        if !self.auth_required() {
            return true;
        }
        let Some(auth) = headers.get("authorization") else {
            return false;
        };
        let auth = auth.trim();

        // Bearer token
        if !self.token.is_empty() {
            if let Some(rest) = auth
                .strip_prefix("Bearer ")
                .or_else(|| auth.strip_prefix("bearer "))
            {
                if const_time_eq(rest.trim().as_bytes(), self.token.as_bytes()) {
                    return true;
                }
            }
        }

        // Basic user:password
        if !self.basic_user.is_empty() && !self.basic_password.is_empty() {
            if let Some(rest) = auth
                .strip_prefix("Basic ")
                .or_else(|| auth.strip_prefix("basic "))
            {
                if let Ok(decoded) = b64_decode(rest.trim()) {
                    if let Ok(s) = String::from_utf8(decoded) {
                        if let Some((u, p)) = s.split_once(':') {
                            if const_time_eq(u.as_bytes(), self.basic_user.as_bytes())
                                && const_time_eq(p.as_bytes(), self.basic_password.as_bytes())
                            {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        false
    }

    /// `WWW-Authenticate` challenges for 401 responses.
    pub fn www_authenticate(&self) -> String {
        let mut parts = Vec::new();
        if !self.token.is_empty() {
            parts.push(r#"Bearer realm="kore-admin""#.to_string());
        }
        if !self.basic_user.is_empty() && !self.basic_password.is_empty() {
            parts.push(r#"Basic realm="kore-admin""#.to_string());
        }
        if parts.is_empty() {
            r#"Bearer realm="kore-admin""#.to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// Constant-time equality for secrets (length mismatch → false).
fn const_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Minimal base64 decode (std-only) for Basic auth credentials.
fn b64_decode(input: &str) -> Result<Vec<u8>, ()> {
    fn val(c: u8) -> Result<u8, ()> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(()),
        }
    }

    let clean: Vec<u8> = input
        .bytes()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    if clean.is_empty() || clean.len() % 4 != 0 {
        return Err(());
    }
    let mut out = Vec::with_capacity(clean.len() / 4 * 3);
    for chunk in clean.chunks_exact(4) {
        let mut vals = [0u8; 4];
        let mut pad = 0usize;
        for (i, &c) in chunk.iter().enumerate() {
            if c == b'=' {
                vals[i] = 0;
                pad += 1;
            } else {
                vals[i] = val(c)?;
            }
        }
        if pad > 2 {
            return Err(());
        }
        let n = (u32::from(vals[0]) << 18)
            | (u32::from(vals[1]) << 12)
            | (u32::from(vals[2]) << 6)
            | u32::from(vals[3]);
        out.push(((n >> 16) & 0xff) as u8);
        if pad < 2 {
            out.push(((n >> 8) & 0xff) as u8);
        }
        if pad < 1 {
            out.push((n & 0xff) as u8);
        }
    }
    Ok(out)
}

/// Parse `"METHOD /path?query HTTP/1.x"` (version optional; origin-form target).
pub fn parse_request_line(line: &str) -> Option<ParsedRequestLine> {
    let line = line.trim_end_matches(['\r', '\n']);
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?;
    let path = target.split('?').next().unwrap_or(target).to_string();
    if method.is_empty() || path.is_empty() {
        return None;
    }
    Some(ParsedRequestLine { method, path })
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

/// Read until the first `\r\n` of the request line (or [`MAX_REQUEST_BUF`]).
///
/// Returns `Ok(None)` on EOF with no data. Remaining header/body bytes (if any)
/// are left unread; prefer [`read_http_request`] when auth headers are needed.
pub async fn read_request_line<S>(stream: &mut S) -> std::io::Result<Option<String>>
where
    S: AsyncRead + Unpin,
{
    let mut buf = vec![0u8; MAX_REQUEST_BUF];
    let mut filled = 0usize;

    while filled < MAX_REQUEST_BUF {
        let n = stream.read(&mut buf[filled..]).await?;
        if n == 0 {
            if filled == 0 {
                return Ok(None);
            }
            break;
        }
        filled += n;
        if let Some(pos) = find_crlf(&buf[..filled]) {
            let line = String::from_utf8_lossy(&buf[..pos]).into_owned();
            return Ok(Some(line));
        }
    }

    // No CRLF within budget: fall back to first LF or whole buffer.
    let slice = &buf[..filled];
    if let Some(pos) = slice.iter().position(|&b| b == b'\n') {
        let end = if pos > 0 && slice[pos - 1] == b'\r' {
            pos - 1
        } else {
            pos
        };
        return Ok(Some(String::from_utf8_lossy(&slice[..end]).into_owned()));
    }
    Ok(Some(String::from_utf8_lossy(slice).into_owned()))
}

/// Read request line + headers (until `\r\n\r\n` or buffer cap).
///
/// Returns `Ok(None)` on EOF with no data. Body bytes after headers are discarded
/// for the admin one-shot response model.
pub async fn read_http_request<S>(stream: &mut S) -> std::io::Result<Option<HttpRequest>>
where
    S: AsyncRead + Unpin,
{
    let mut buf = vec![0u8; MAX_REQUEST_BUF];
    let mut filled = 0usize;

    while filled < MAX_REQUEST_BUF {
        let n = stream.read(&mut buf[filled..]).await?;
        if n == 0 {
            if filled == 0 {
                return Ok(None);
            }
            break;
        }
        filled += n;
        if find_header_end(&buf[..filled]).is_some() {
            break;
        }
    }

    let end = find_header_end(&buf[..filled]).unwrap_or(filled);
    let text = String::from_utf8_lossy(&buf[..end]);
    let mut lines = text.split("\r\n");
    let first = lines.next().unwrap_or("").trim_end_matches('\n');
    let Some(line) = parse_request_line(first) else {
        // Unparseable — return a synthetic empty path so caller can 400.
        return Ok(Some(HttpRequest {
            line: ParsedRequestLine {
                method: String::new(),
                path: String::new(),
            },
            headers: HashMap::new(),
        }));
    };

    let mut headers = HashMap::new();
    for hline in lines {
        if hline.is_empty() {
            break;
        }
        if let Some((name, value)) = hline.split_once(':') {
            headers.insert(
                name.trim().to_ascii_lowercase(),
                value.trim().to_string(),
            );
        }
    }

    Ok(Some(HttpRequest { line, headers }))
}

/// Write a simple HTTP/1.1 response (`Connection: close`).
pub async fn write_response<S>(
    stream: &mut S,
    code: u16,
    reason: &str,
    content_type: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut resp = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: {}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n",
        code,
        reason,
        content_type,
        body.len()
    );
    for (k, v) in extra_headers {
        resp.push_str(k);
        resp.push_str(": ");
        resp.push_str(v);
        resp.push_str("\r\n");
    }
    resp.push_str("\r\n");
    resp.push_str(body);
    stream.write_all(resp.as_bytes()).await
}

/// `404 Not Found` plain-text body.
pub async fn write_404<S: AsyncWrite + Unpin>(stream: &mut S) -> std::io::Result<()> {
    write_response(
        stream,
        404,
        "Not Found",
        "text/plain; charset=utf-8",
        "not found\n",
        &[],
    )
    .await
}

/// `405 Method Not Allowed` with `Allow: GET` (admin endpoints are GET-only).
pub async fn write_405_get_only<S: AsyncWrite + Unpin>(stream: &mut S) -> std::io::Result<()> {
    write_response(
        stream,
        405,
        "Method Not Allowed",
        "text/plain; charset=utf-8",
        "method not allowed\n",
        &[("Allow", "GET")],
    )
    .await
}

/// `400 Bad Request` when the request line cannot be parsed.
pub async fn write_400<S: AsyncWrite + Unpin>(stream: &mut S) -> std::io::Result<()> {
    write_response(
        stream,
        400,
        "Bad Request",
        "text/plain; charset=utf-8",
        "bad request\n",
        &[],
    )
    .await
}

/// `401 Unauthorized` with `WWW-Authenticate` challenge(s).
pub async fn write_401<S: AsyncWrite + Unpin>(
    stream: &mut S,
    www_authenticate: &str,
) -> std::io::Result<()> {
    write_response(
        stream,
        401,
        "Unauthorized",
        "text/plain; charset=utf-8",
        "unauthorized\n",
        &[("WWW-Authenticate", www_authenticate)],
    )
    .await
}

/// Shared accept-loop helper: plain TCP or TLS, then `handler`.
pub async fn serve_connection<F, Fut>(
    stream: tokio::net::TcpStream,
    options: Arc<AdminHttpOptions>,
    handler: F,
) where
    F: FnOnce(AdminStream) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    if let Some(acceptor) = options.tls.clone() {
        match acceptor.accept(stream).await {
            Ok(tls_stream) => handler(AdminStream::Tls(tls_stream)).await,
            Err(e) => {
                tracing::debug!("admin TLS handshake failed: {}", e);
            }
        }
    } else {
        handler(AdminStream::Plain(stream)).await;
    }
}

/// Stream enum for plain or TLS admin connections.
pub enum AdminStream {
    Plain(tokio::net::TcpStream),
    Tls(tokio_rustls::server::TlsStream<tokio::net::TcpStream>),
}

impl AsyncRead for AdminStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            AdminStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            AdminStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AdminStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut *self {
            AdminStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            AdminStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            AdminStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            AdminStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            AdminStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            AdminStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Build [`AdminHttpOptions`] from CLI config fields (TLS acceptor optional).
pub fn options_from_parts(
    token: &str,
    user: &str,
    password: &str,
    bind: &str,
    tls: Option<TlsAcceptor>,
) -> AdminHttpOptions {
    AdminHttpOptions {
        token: token.to_string(),
        basic_user: user.to_string(),
        basic_password: password.to_string(),
        tls,
        bind: if bind.is_empty() {
            "127.0.0.1".to_string()
        } else {
            bind.to_string()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    #[test]
    fn parse_get_path_and_query() {
        let r = parse_request_line("GET /api/deadlock?x=1 HTTP/1.1").unwrap();
        assert!(r.is_get());
        assert_eq!(r.method, "GET");
        assert_eq!(r.path, "/api/deadlock");
    }

    #[test]
    fn parse_post_known_path() {
        let r = parse_request_line("POST /metrics HTTP/1.1").unwrap();
        assert!(!r.is_get());
        assert_eq!(r.path, "/metrics");
    }

    #[test]
    fn parse_get_root_no_version() {
        let r = parse_request_line("GET /").unwrap();
        assert!(r.is_get());
        assert_eq!(r.path, "/");
    }

    #[test]
    fn parse_method_case_insensitive_get() {
        let r = parse_request_line("get /metrics HTTP/1.0").unwrap();
        assert!(r.is_get());
    }

    #[test]
    fn parse_rejects_empty() {
        assert!(parse_request_line("").is_none());
        assert!(parse_request_line("GET").is_none());
    }

    #[test]
    fn auth_open_when_no_credentials() {
        let opts = AdminHttpOptions::default();
        assert!(!opts.auth_required());
        assert!(opts.authorize(&HashMap::new()));
    }

    #[test]
    fn auth_bearer_token() {
        let mut opts = AdminHttpOptions::default();
        opts.token = "s3cret".into();
        assert!(opts.auth_required());
        assert!(!opts.authorize(&HashMap::new()));
        let mut h = HashMap::new();
        h.insert("authorization".into(), "Bearer s3cret".into());
        assert!(opts.authorize(&h));
        h.insert("authorization".into(), "Bearer wrong".into());
        assert!(!opts.authorize(&h));
    }

    #[test]
    fn auth_basic() {
        let mut opts = AdminHttpOptions::default();
        opts.basic_user = "admin".into();
        opts.basic_password = "pw".into();
        // echo -n 'admin:pw' | base64 → YWRtaW46cHc=
        let mut h = HashMap::new();
        h.insert("authorization".into(), "Basic YWRtaW46cHc=".into());
        assert!(opts.authorize(&h));
        h.insert("authorization".into(), "Basic d3Jvbmc=".into());
        assert!(!opts.authorize(&h));
    }

    #[test]
    fn b64_roundtrip_admin_pw() {
        let raw = b64_decode("YWRtaW46cHc=").unwrap();
        assert_eq!(String::from_utf8(raw).unwrap(), "admin:pw");
    }

    /// Batch DL: assemble the request line across multiple TCP reads (partial
    /// chunks before the first `\r\n`).
    #[tokio::test]
    async fn read_request_line_across_partial_reads() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            // Deliberately split before CRLF so the server must loop on read.
            stream.write_all(b"GET /metrics").await.unwrap();
            stream.flush().await.unwrap();
            tokio::task::yield_now().await;
            stream
                .write_all(b" HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();
            stream.flush().await.unwrap();
            // Keep the connection open until the server finishes reading the line.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        });

        let (mut server, _) = listener.accept().await.unwrap();
        let line = read_request_line(&mut server)
            .await
            .unwrap()
            .expect("request line");
        assert_eq!(line, "GET /metrics HTTP/1.1");
        let req = parse_request_line(&line).unwrap();
        assert!(req.is_get());
        assert_eq!(req.path, "/metrics");
        let _ = client.await;
    }

    #[tokio::test]
    async fn read_http_request_parses_authorization() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(
                    b"GET /metrics HTTP/1.1\r\n\
                      Host: localhost\r\n\
                      Authorization: Bearer tok123\r\n\
                      \r\n",
                )
                .await
                .unwrap();
            stream.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        });

        let (mut server, _) = listener.accept().await.unwrap();
        let req = read_http_request(&mut server)
            .await
            .unwrap()
            .expect("request");
        assert!(req.line.is_get());
        assert_eq!(req.line.path, "/metrics");
        assert_eq!(
            req.headers.get("authorization").map(String::as_str),
            Some("Bearer tok123")
        );
        let _ = client.await;
    }

    /// Batch DL: a line longer than [`MAX_REQUEST_BUF`] with no CRLF returns the
    /// buffered prefix; callers that `parse_request_line` garbage → 400 path.
    #[tokio::test]
    async fn read_request_line_oversized_no_crlf_yields_unparseable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            // > 8 KiB of non-HTTP noise, no CRLF anywhere.
            let blob = vec![b'A'; MAX_REQUEST_BUF + 512];
            let _ = stream.write_all(&blob).await;
            let _ = stream.flush().await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        });

        let (mut server, _) = listener.accept().await.unwrap();
        let line = read_request_line(&mut server)
            .await
            .unwrap()
            .expect("oversize fallback returns buffer");
        // Reader stops at the cap; no CRLF/LF → whole filled buffer.
        assert!(
            line.len() <= MAX_REQUEST_BUF,
            "line len {} exceeds cap",
            line.len()
        );
        assert!(
            line.len() >= MAX_REQUEST_BUF / 2,
            "expected a large filled buffer, got {}",
            line.len()
        );
        // No spaces → not a valid "METHOD path" request line → 400 at call sites.
        assert!(
            parse_request_line(&line).is_none(),
            "oversize garbage must not parse as a request line"
        );
        let _ = client.await;
    }

    /// Batch DL: bare LF (no CR) is accepted as a line terminator fallback.
    #[tokio::test]
    async fn read_request_line_lf_only_fallback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(b"GET /deadlock HTTP/1.1\nHost: x\n\n")
                .await
                .unwrap();
            stream.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        });

        let (mut server, _) = listener.accept().await.unwrap();
        let client_handle = client;
        let line = read_request_line(&mut server)
            .await
            .unwrap()
            .expect("lf-terminated line");
        assert_eq!(line, "GET /deadlock HTTP/1.1");
        let _ = client_handle.await;
    }

    /// Documented routing: non-GET on an **unknown** path is 404 (resource not
    /// found), not 405. 405 is reserved for non-GET on **known** admin paths.
    #[test]
    fn unknown_path_non_get_is_resource_not_found_semantics() {
        let r = parse_request_line("POST /nope HTTP/1.1").unwrap();
        assert!(!r.is_get());
        assert_eq!(r.path, "/nope");
    }
}
