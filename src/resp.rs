use std::{ffi::OsString, os::unix::ffi::OsStringExt};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Frame {
    Int(i64),
    SimpleString(String),
    Error(String),
    Bulk(Vec<u8>),
    NullBulk,
    Array(Vec<Frame>),
    NullArray,
}

impl Frame {
    pub fn try_parse(s: &str) -> anyhow::Result<Self> {
        Self::recursive_parse(s).map(|(r, _cnt)| r)
    }
    fn recursive_parse(s: &str) -> anyhow::Result<(Self, usize)> {
        let resp_type = &s[..1];
        let val = &s[1..];
        let resp = match resp_type {
            // integers
            ":" => {
                let break_idx = val
                    .find("\r\n")
                    .ok_or(anyhow::anyhow!("No CRLF termination found"))?;
                let val: i64 = val[..break_idx].parse()?;
                (Frame::Int(val), break_idx + 2 + 1)
            }
            "+" => {
                let break_idx = val
                    .find("\r\n")
                    .ok_or(anyhow::anyhow!("No CRLF termination found"))?;
                (
                    Frame::SimpleString(val[..break_idx].to_string()),
                    break_idx + 2 + 1,
                )
            }
            "-" => {
                let break_idx = val
                    .find("\r\n")
                    .ok_or(anyhow::anyhow!("No CRLF termination found"))?;
                (
                    Frame::Error(val[..break_idx].to_string()),
                    break_idx + 2 + 1,
                )
            }
            // bulk string
            "$" => {
                let break_idx = val
                    .find("\r\n")
                    .ok_or(anyhow::anyhow!("No CRLF termination found"))?;
                let len: usize = val[..break_idx].parse()?;
                let remaining = &val[(2 + break_idx)..];
                let bulk_bytes = remaining[..len].as_bytes();
                (Frame::Bulk(bulk_bytes.to_vec()), len + break_idx + 4 + 1)
            }
            // array
            "*" => {
                let break_idx = val
                    .find("\r\n")
                    .ok_or(anyhow::anyhow!("No CRLF termination found"))?;
                let len: usize = val[..break_idx].parse()?;
                let mut vals = Vec::with_capacity(len);
                let mut offset = 2 + break_idx;
                for _ in 0..len {
                    let (v, bytes_read) = Self::recursive_parse(&val[offset..])?;
                    vals.push(v);
                    offset += bytes_read;
                }
                (Frame::Array(vals), offset + 1)
            }
            _ => return Err(anyhow::anyhow!("Unknown type {resp_type:?}")),
        };
        Ok(resp)
    }
    pub fn to_string(&self) -> String {
        match self {
            Self::Int(val) => format!(":{val}\r\n"),
            Self::SimpleString(val) => {
                format!("+{val}\r\n")
            }
            Self::Error(val) => {
                format!("-{val}\r\n")
            }
            Self::Array(val) => {
                let num_items = val.len();
                let s_val = val
                    .into_iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>()
                    .join("");
                format!("*{num_items}\r\n{s_val}")
            }
            Self::Bulk(bytes) => {
                format!(
                    "${}\r\n{}\r\n",
                    bytes.len(),
                    str::from_utf8(bytes).expect("expected valid bytes")
                )
            }
            Self::NullBulk => "$-1\r\n".to_string(),
            Self::NullArray => "*-1\r\n".to_string(),
        }
    }
    pub fn bulk_from_string(s: String) -> Self {
        Self::Bulk(s.into_bytes())
    }
    pub fn bulk_from_str(s: &str) -> Self {
        Self::bulk_from_string(s.to_string())
    }

    pub fn pong() -> Self {
        Self::SimpleString("PONG".to_string())
    }
    pub fn ok() -> Self {
        Self::SimpleString("OK".to_string())
    }
}

impl From<Frame> for OsString {
    fn from(value: Frame) -> Self {
        match value {
            Frame::SimpleString(v) => v.into(),
            // TODO: This is *nix specific code, maybe generalize to allow for Windows
            Frame::Bulk(bytes) => OsString::from_vec(bytes),
            _ => value.to_string().into(),
        }
    }
}

#[cfg(test)]
mod test_resp {
    use crate::resp::Frame;

    #[test]
    fn parse_int() {
        let val = ":1000\r\n";
        let resp = Frame::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::Int(1000));

        let val = ":+1000\r\n";
        let resp = Frame::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::Int(1000));

        let val = ":-1000\r\n";
        let resp = Frame::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::Int(-1000));

        // Test to string
        let resp_str = Frame::Int(1000).to_string();
        assert_eq!(":1000\r\n", resp_str);

        let resp_str = Frame::Int(-1000).to_string();
        assert_eq!(":-1000\r\n", resp_str);
    }

    #[test]
    fn parse_simple_string() {
        let val = "+OK\r\n";
        let resp = Frame::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::SimpleString("OK".to_string()));

        assert_eq!(val, resp.to_string());
    }

    #[test]
    fn parse_bulk_string() {
        let val = "$6\r\nhe\rllo\r\n";
        let resp = Frame::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::Bulk("he\rllo".as_bytes().to_vec()));
        assert_eq!(val, resp.to_string());

        // test empty string
        let val = "$0\r\n\r\n";
        let resp = Frame::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::Bulk("".as_bytes().to_vec()));

        // nil string?
    }

    #[test]
    fn parse_array() {
        let val = "*3\r\n+OK\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let resp = Frame::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(
            resp,
            Frame::Array(
                vec![
                    Frame::SimpleString("OK".to_string()),
                    Frame::Bulk("hello".as_bytes().to_vec()),
                    Frame::Bulk("world".as_bytes().to_vec()),
                ]
                .into()
            )
        );

        let val = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n";
        let resp = Frame::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(
            resp,
            Frame::Array(
                vec![
                    Frame::Array(vec![Frame::Int(1), Frame::Int(2), Frame::Int(3),].into()),
                    Frame::Array(
                        vec![
                            Frame::SimpleString("Hello".to_string()),
                            Frame::Error("World".to_string()),
                        ]
                        .into()
                    )
                ]
                .into()
            )
        );
        assert_eq!(val, resp.to_string())
    }
}
