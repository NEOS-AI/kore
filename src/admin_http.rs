//! Shared helpers for localhost-only admin HTTP endpoints (metrics, deadlock UI).
//!
//! Minimal HTTP/1.1 request-line parsing and response writing — not a full stack.
//! No auth, TLS, pipelining, or body handling; callers close the connection after one response.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Cap for reading the request line (and any trailing bytes in the same reads).
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

/// Read until the first `\r\n` of the request line (or [`MAX_REQUEST_BUF`]).
///
/// Returns `Ok(None)` on EOF with no data. Remaining header/body bytes (if any)
/// are left unread; callers that respond and close do not need them.
pub async fn read_request_line(stream: &mut TcpStream) -> std::io::Result<Option<String>> {
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

/// Write a simple HTTP/1.1 response (`Connection: close`).
pub async fn write_response(
    stream: &mut TcpStream,
    code: u16,
    reason: &str,
    content_type: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
) -> std::io::Result<()> {
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
pub async fn write_404(stream: &mut TcpStream) -> std::io::Result<()> {
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
pub async fn write_405_get_only(stream: &mut TcpStream) -> std::io::Result<()> {
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
pub async fn write_400(stream: &mut TcpStream) -> std::io::Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
