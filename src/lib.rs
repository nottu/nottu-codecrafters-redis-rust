use std::{ffi::OsString, os::unix::ffi::OsStringExt};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RESP {
    Int(i64),
    SimpleString(String),
    Error(String),
    Bulk(Vec<u8>),
    Array(Vec<RESP>),
}

impl RESP {
    pub const NULL_BULK: &'static str = "$-1\r\n";

    pub fn try_parse(s: &str) -> anyhow::Result<Self> {
        eprintln!("Parsing: {s:?}");
        Self::recursive_parse(s).map(|(r, _cnt)| {
            // eprint!("Read {cnt} bytes, remaining str {}", &s[cnt..]);
            r
        })
    }
    fn recursive_parse(s: &str) -> anyhow::Result<(Self, usize)> {
        dbg!(s);
        let resp_type = &s[..1];
        let val = &s[1..];
        let resp = match resp_type {
            // integers
            ":" => {
                let break_idx = val
                    .find("\r\n")
                    .ok_or(anyhow::anyhow!("No CRLF termination found"))?;
                let val: i64 = val[..break_idx].parse()?;
                (RESP::Int(val), break_idx + 2 + 1)
            }
            "+" => {
                let break_idx = val
                    .find("\r\n")
                    .ok_or(anyhow::anyhow!("No CRLF termination found"))?;
                (
                    RESP::SimpleString(val[..break_idx].to_string()),
                    break_idx + 2 + 1,
                )
            }
            "-" => {
                let break_idx = val
                    .find("\r\n")
                    .ok_or(anyhow::anyhow!("No CRLF termination found"))?;
                (RESP::Error(val[..break_idx].to_string()), break_idx + 2 + 1)
            }
            // bulk string
            "$" => {
                let break_idx = val
                    .find("\r\n")
                    .ok_or(anyhow::anyhow!("No CRLF termination found"))?;
                let len: usize = val[..break_idx].parse()?;
                let remaining = &val[(2 + break_idx)..];
                let bulk_bytes = remaining[..len].as_bytes();
                (RESP::Bulk(bulk_bytes.to_vec()), len + break_idx + 4 + 1)
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
                    // dbg!(&v, bytes_read);
                    vals.push(v);
                    offset += bytes_read;
                }
                (RESP::Array(vals), offset + 1)
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
        }
    }

    pub fn empty_array() -> Self {
        Self::Array(vec![])
    }

    pub fn push(&mut self, val: String) -> anyhow::Result<usize> {
        if let Self::Array(arr) = self {
            arr.push(RESP::Bulk(val.into_bytes()));
            Ok(arr.len())
        } else {
            Err(anyhow::anyhow!("self is not an array type"))
        }
    }
    pub fn len(&self) -> anyhow::Result<usize> {
        if let Self::Array(arr) = self {
            Ok(arr.len())
        } else {
            Err(anyhow::anyhow!("self is not an array type"))
        }
    }
    pub fn range(&self, start: usize, end: usize) -> anyhow::Result<Self> {
        let Self::Array(arr) = self else {
            return Err(anyhow::anyhow!("self is not an array type"));
        };
        let sub_arr = if (start > arr.len()) || (end < start) {
            vec![]
        } else {
            arr[start..=((arr.len() - 1).min(end))].to_vec()
        };
        Ok(Self::Array(sub_arr))
    }
}

impl From<RESP> for OsString {
    fn from(value: RESP) -> Self {
        match value {
            RESP::SimpleString(v) => v.into(),
            // TODO: This is *nix specific code, maybe generalize to allow for Windows
            RESP::Bulk(bytes) => OsString::from_vec(bytes),
            _ => value.to_string().into(),
        }
    }
}

#[cfg(test)]
mod test_resp {
    use crate::RESP;

    #[test]
    fn parse_int() {
        let val = ":1000\r\n";
        let resp = RESP::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, RESP::Int(1000));

        let val = ":+1000\r\n";
        let resp = RESP::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, RESP::Int(1000));

        let val = ":-1000\r\n";
        let resp = RESP::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, RESP::Int(-1000));

        // Test to string
        let resp_str = RESP::Int(1000).to_string();
        assert_eq!(":1000\r\n", resp_str);

        let resp_str = RESP::Int(-1000).to_string();
        assert_eq!(":-1000\r\n", resp_str);
    }

    #[test]
    fn parse_simple_string() {
        let val = "+OK\r\n";
        let resp = RESP::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, RESP::SimpleString("OK".to_string()));

        assert_eq!(val, resp.to_string());
    }

    #[test]
    fn parse_bulk_string() {
        let val = "$6\r\nhe\rllo\r\n";
        let resp = RESP::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, RESP::Bulk("he\rllo".as_bytes().to_vec()));
        assert_eq!(val, resp.to_string());

        // test empty string
        let val = "$0\r\n\r\n";
        let resp = RESP::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, RESP::Bulk("".as_bytes().to_vec()));

        // nil string?
    }

    #[test]
    fn parse_array() {
        let val = "*3\r\n+OK\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let resp = RESP::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(
            resp,
            RESP::Array(vec![
                RESP::SimpleString("OK".to_string()),
                RESP::Bulk("hello".as_bytes().to_vec()),
                RESP::Bulk("world".as_bytes().to_vec()),
            ])
        );

        let val = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n";
        let resp = RESP::try_parse(&val).expect("Expected to parse valid RESP string");
        assert_eq!(
            resp,
            RESP::Array(vec![
                RESP::Array(vec![RESP::Int(1), RESP::Int(2), RESP::Int(3),]),
                RESP::Array(vec![
                    RESP::SimpleString("Hello".to_string()),
                    RESP::Error("World".to_string()),
                ])
            ])
        );
        assert_eq!(val, resp.to_string())
    }
}
