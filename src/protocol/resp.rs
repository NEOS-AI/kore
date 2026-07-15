use crate::error::{Error, Result};
use bytes::{Buf, Bytes, BytesMut};

/// RESP (REdis Serialization Protocol) value types
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// Simple string: +OK\r\n
    SimpleString(Bytes),
    /// Error: -Error message\r\n
    Error(Bytes),
    /// Integer: :1000\r\n
    Integer(i64),
    /// Bulk string: $6\r\nfoobar\r\n
    BulkString(Option<Bytes>),
    /// Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    Array(Vec<RespValue>),
    /// Null array: *-1\r\n (BLPOP/BRPOP/XREAD timeout in RESP2)
    NullArray,
    /// Multiple top-level RESP messages concatenated (used for Pub/Sub multi-channel responses).
    /// Each element is serialized as its own independent top-level frame — NOT wrapped in an array.
    Multiple(Vec<RespValue>),
}

impl RespValue {
    /// Serialize RESP value to bytes
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.write_to(&mut buf);
        buf.freeze()
    }

    fn write_to(&self, buf: &mut BytesMut) {
        use bytes::BufMut;

        match self {
            RespValue::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.extend_from_slice(s);
                buf.put_slice(b"\r\n");
            }
            RespValue::Error(e) => {
                buf.put_u8(b'-');
                buf.extend_from_slice(e);
                buf.put_slice(b"\r\n");
            }
            RespValue::Integer(i) => {
                buf.put_u8(b':');
                buf.extend_from_slice(i.to_string().as_bytes());
                buf.put_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                buf.put_slice(b"$-1\r\n");
            }
            RespValue::BulkString(Some(s)) => {
                buf.put_u8(b'$');
                buf.extend_from_slice(s.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                buf.extend_from_slice(s);
                buf.put_slice(b"\r\n");
            }
            RespValue::Array(arr) => {
                buf.put_u8(b'*');
                buf.extend_from_slice(arr.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                for val in arr {
                    val.write_to(buf);
                }
            }
            RespValue::NullArray => {
                buf.put_slice(b"*-1\r\n");
            }
            RespValue::Multiple(values) => {
                // Serialize each value as an independent top-level RESP frame.
                for val in values {
                    val.write_to(buf);
                }
            }
        }
    }

    /// Create an OK response
    pub fn ok() -> Self {
        RespValue::SimpleString(Bytes::from_static(b"OK"))
    }

    /// Create an error response
    pub fn error(msg: impl Into<Bytes>) -> Self {
        RespValue::Error(msg.into())
    }

    /// Create a null bulk string
    pub fn null() -> Self {
        RespValue::BulkString(None)
    }

    /// Create a null multi-bulk (array) reply — BLPOP/BRPOP timeout.
    pub fn null_array() -> Self {
        RespValue::NullArray
    }

    /// Convert to array if possible
    pub fn as_array(&self) -> Option<&Vec<RespValue>> {
        match self {
            RespValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Convert to bulk string if possible
    pub fn as_bulk_string(&self) -> Option<&Bytes> {
        match self {
            RespValue::BulkString(Some(s)) => Some(s),
            _ => None,
        }
    }

    /// Convert to integer if possible
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RespValue::Integer(i) => Some(*i),
            _ => None,
        }
    }
}

/// RESP protocol parser
pub struct RespParser {
    buffer: BytesMut,
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(8192),
        }
    }

    /// Add data to the parser buffer
    pub fn feed(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Try to parse a complete RESP value from the buffer
    pub fn parse(&mut self) -> Result<Option<RespValue>> {
        Ok(self.parse_with_consumed()?.map(|(v, _)| v))
    }

    /// Parse one RESP value and return how many buffer bytes it consumed.
    ///
    /// Used for exact replication-offset accounting (wire bytes, not re-serialize).
    pub fn parse_with_consumed(&mut self) -> Result<Option<(RespValue, usize)>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let mut cursor = std::io::Cursor::new(&self.buffer[..]);

        match self.parse_value(&mut cursor) {
            Ok(value) => {
                let pos = cursor.position() as usize;
                self.buffer.advance(pos);
                Ok(Some((value, pos)))
            }
            Err(Error::ParseError(_)) => {
                // Need more data
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn parse_value(&self, cursor: &mut std::io::Cursor<&[u8]>) -> Result<RespValue> {
        if !cursor.has_remaining() {
            return Err(Error::ParseError("incomplete data".into()));
        }

        let type_byte = cursor.get_u8();

        match type_byte {
            b'+' => self.parse_simple_string(cursor),
            b'-' => self.parse_error(cursor),
            b':' => self.parse_integer(cursor),
            b'$' => self.parse_bulk_string(cursor),
            b'*' => self.parse_array(cursor),
            _ => Err(Error::ParseError(format!(
                "invalid RESP type byte: {}",
                type_byte
            ))),
        }
    }

    fn parse_simple_string(&self, cursor: &mut std::io::Cursor<&[u8]>) -> Result<RespValue> {
        let line = self.read_line(cursor)?;
        Ok(RespValue::SimpleString(Bytes::copy_from_slice(line)))
    }

    fn parse_error(&self, cursor: &mut std::io::Cursor<&[u8]>) -> Result<RespValue> {
        let line = self.read_line(cursor)?;
        Ok(RespValue::Error(Bytes::copy_from_slice(line)))
    }

    fn parse_integer(&self, cursor: &mut std::io::Cursor<&[u8]>) -> Result<RespValue> {
        let line = self.read_line(cursor)?;
        let s = std::str::from_utf8(line)
            .map_err(|_| Error::ParseError("invalid UTF-8 in integer".into()))?;
        let num = s
            .parse::<i64>()
            .map_err(|_| Error::ParseError("invalid integer".into()))?;
        Ok(RespValue::Integer(num))
    }

    fn parse_bulk_string(&self, cursor: &mut std::io::Cursor<&[u8]>) -> Result<RespValue> {
        let line = self.read_line(cursor)?;
        let s = std::str::from_utf8(line)
            .map_err(|_| Error::ParseError("invalid UTF-8 in bulk string length".into()))?;
        let len = s
            .parse::<i64>()
            .map_err(|_| Error::ParseError("invalid bulk string length".into()))?;

        if len == -1 {
            return Ok(RespValue::BulkString(None));
        }

        if len < 0 {
            return Err(Error::ParseError("negative bulk string length".into()));
        }

        let len = len as usize;
        if len > 500 * 1024 * 1024 {
            // 500MB max
            return Err(Error::ParseError("bulk string too large".into()));
        }

        // Read the string data
        if cursor.remaining() < len + 2 {
            return Err(Error::ParseError("incomplete bulk string data".into()));
        }

        let data = &cursor.chunk()[..len];
        let bytes = Bytes::copy_from_slice(data);
        cursor.advance(len);

        // Read trailing \r\n
        if cursor.remaining() < 2 {
            return Err(Error::ParseError("missing bulk string terminator".into()));
        }

        let cr = cursor.get_u8();
        let lf = cursor.get_u8();
        if cr != b'\r' || lf != b'\n' {
            return Err(Error::ParseError("invalid bulk string terminator".into()));
        }

        Ok(RespValue::BulkString(Some(bytes)))
    }

    fn parse_array(&self, cursor: &mut std::io::Cursor<&[u8]>) -> Result<RespValue> {
        let line = self.read_line(cursor)?;
        let s = std::str::from_utf8(line)
            .map_err(|_| Error::ParseError("invalid UTF-8 in array length".into()))?;
        let len = s
            .parse::<i64>()
            .map_err(|_| Error::ParseError("invalid array length".into()))?;

        // RESP2 null multi-bulk (BLPOP/BRPOP/XREAD timeout): *-1\r\n
        if len == -1 {
            return Ok(RespValue::NullArray);
        }

        if len < 0 {
            return Err(Error::ParseError("negative array length".into()));
        }

        if len > 100_000 {
            // Max 100k elements
            return Err(Error::ParseError("array too large".into()));
        }

        let len = len as usize;
        let mut arr = Vec::with_capacity(len);

        for _ in 0..len {
            let value = self.parse_value(cursor)?;
            arr.push(value);
        }

        Ok(RespValue::Array(arr))
    }

    fn read_line<'a>(&self, cursor: &mut std::io::Cursor<&'a [u8]>) -> Result<&'a [u8]> {
        let start = cursor.position() as usize;
        let data = cursor.get_ref();

        // Find \r\n
        for i in start..data.len() - 1 {
            if data[i] == b'\r' && data[i + 1] == b'\n' {
                let line = &data[start..i];
                cursor.set_position((i + 2) as u64);
                return Ok(line);
            }
        }

        Err(Error::ParseError("incomplete line".into()))
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_string() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK\r\n");

        let value = parser.parse().unwrap().unwrap();
        assert_eq!(value, RespValue::SimpleString(Bytes::from("OK")));
    }

    #[test]
    fn test_error() {
        let mut parser = RespParser::new();
        parser.feed(b"-Error message\r\n");

        let value = parser.parse().unwrap().unwrap();
        assert_eq!(value, RespValue::Error(Bytes::from("Error message")));
    }

    #[test]
    fn test_integer() {
        let mut parser = RespParser::new();
        parser.feed(b":1000\r\n");

        let value = parser.parse().unwrap().unwrap();
        assert_eq!(value, RespValue::Integer(1000));
    }

    #[test]
    fn test_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$6\r\nfoobar\r\n");

        let value = parser.parse().unwrap().unwrap();
        assert_eq!(value, RespValue::BulkString(Some(Bytes::from("foobar"))));
    }

    #[test]
    fn test_null_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$-1\r\n");

        let value = parser.parse().unwrap().unwrap();
        assert_eq!(value, RespValue::BulkString(None));
    }

    #[test]
    fn test_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");

        let value = parser.parse().unwrap().unwrap();
        let arr = value.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("foo"))));
        assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("bar"))));
    }

    #[test]
    fn test_serialize() {
        let value = RespValue::ok();
        assert_eq!(value.serialize(), Bytes::from("+OK\r\n"));

        let value = RespValue::Integer(42);
        assert_eq!(value.serialize(), Bytes::from(":42\r\n"));

        let value = RespValue::BulkString(Some(Bytes::from("hello")));
        assert_eq!(value.serialize(), Bytes::from("$5\r\nhello\r\n"));
    }

    #[test]
    fn parse_with_consumed_matches_wire_len() {
        let samples: &[&[u8]] = &[
            b"+OK\r\n",
            b"-ERR x\r\n",
            b":42\r\n",
            b"$5\r\nhello\r\n",
            b"$-1\r\n",
            b"*2\r\n$3\r\nSET\r\n$1\r\nk\r\n",
            b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n",
        ];
        for raw in samples {
            let mut parser = RespParser::new();
            parser.feed(raw);
            let (val, consumed) = parser
                .parse_with_consumed()
                .unwrap()
                .expect("complete value");
            assert_eq!(
                consumed,
                raw.len(),
                "consumed {} != wire {} for {:?}",
                consumed,
                raw.len(),
                val
            );
            // Re-serialize may differ for some forms; wire bytes are authoritative.
            assert!(parser.parse_with_consumed().unwrap().is_none());
        }
    }

    #[test]
    fn parse_with_consumed_partial_then_complete() {
        let full = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n";
        let mut parser = RespParser::new();
        parser.feed(&full[..5]);
        assert!(parser.parse_with_consumed().unwrap().is_none());
        parser.feed(&full[5..]);
        let (_val, consumed) = parser.parse_with_consumed().unwrap().unwrap();
        assert_eq!(consumed, full.len());
    }

    #[test]
    fn parse_with_consumed_multiple_values() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"+OK\r\n");
        buf.extend_from_slice(b":1\r\n");
        buf.extend_from_slice(b"$3\r\nfoo\r\n");
        let mut parser = RespParser::new();
        parser.feed(&buf);
        let mut total = 0usize;
        while let Some((_v, n)) = parser.parse_with_consumed().unwrap() {
            total += n;
        }
        assert_eq!(total, buf.len());
    }

    #[test]
    fn test_null_array_serialize_and_parse() {
        let value = RespValue::null_array();
        assert_eq!(value.serialize(), Bytes::from("*-1\r\n"));

        let mut parser = RespParser::new();
        parser.feed(b"*-1\r\n");
        let parsed = parser.parse().unwrap().unwrap();
        assert_eq!(parsed, RespValue::NullArray);
    }

    #[test]
    fn test_fullresync_simple_string_then_bulk() {
        // PSYNC full handshake shape: +FULLRESYNC …\r\n$n\r\n…\r\n
        let mut parser = RespParser::new();
        let payload = b"+FULLRESYNC abcdef 42\r\n$3\r\nRDB\r\n";
        parser.feed(payload);
        let first = parser.parse().unwrap().unwrap();
        match first {
            RespValue::SimpleString(s) => {
                assert!(String::from_utf8_lossy(&s).starts_with("FULLRESYNC "));
            }
            other => panic!("expected simple string, {:?}", other),
        }
        let second = parser.parse().unwrap().unwrap();
        assert_eq!(
            second,
            RespValue::BulkString(Some(Bytes::from_static(b"RDB")))
        );
    }
}
