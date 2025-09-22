use std::{ffi::OsString, io::Cursor, os::unix::ffi::OsStringExt};

use bytes::Buf;

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
    pub fn try_parse(src: &mut Cursor<&[u8]>) -> anyhow::Result<Self> {
        let resp_type = src.get_u8();
        let resp = match resp_type {
            // integers
            b':' => {
                let val = Self::get_decimal(src)?;
                Frame::Int(val)
            }
            b'+' => {
                let val_bytes = Self::get_line(src)?;
                Frame::SimpleString(String::from_utf8(val_bytes.to_vec())?)
            }
            b'-' => {
                let val_bytes = Self::get_line(src)?;
                Frame::Error(String::from_utf8(val_bytes.to_vec())?)
            }
            // bulk string
            b'$' => {
                let len = Self::get_decimal(src)?;
                match len {
                    -1 => Frame::NullBulk,
                    0..=i64::MAX => {
                        let to_read = len as usize + 2;
                        if src.remaining() < to_read {
                            anyhow::bail!("Protocol error; Incomplete")
                        }
                        let data = src.chunk()[..(to_read - 2)].to_vec();
                        src.advance(to_read);
                        Frame::Bulk(data.to_vec())
                    }
                    _ => anyhow::bail!("Protocol error; non valid Bulk String length {len}"),
                }
            }
            // array
            b'*' => {
                let len = Self::get_decimal(src)?;
                match len {
                    -1 => Frame::NullBulk,
                    0..=i64::MAX => {
                        let mut out = Vec::with_capacity(len as usize);
                        for _ in 0..len {
                            let frame = Self::try_parse(src)?;
                            out.push(frame);
                        }
                        Frame::Array(out)
                    }
                    _ => anyhow::bail!("Protocol error; non valid Array length {len}"),
                }
            }
            _ => anyhow::bail!(
                "Unknown type {resp_type:?}, remainig_bytes {}",
                src.get_ref().len()
            ),
        };
        Ok(resp)
    }

    fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> anyhow::Result<&'a [u8]> {
        // Scan the bytes directly
        let start = src.position() as usize;
        // Scan to the second to last byte
        let end = src.get_ref().len() - 1;

        for i in start..end {
            if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
                // We found a line, update the position to be *after* the \n
                src.set_position((i + 2) as u64);

                // Return the line
                return Ok(&src.get_ref()[start..i]);
            }
        }
        anyhow::bail!("Protocol error; Incomplete")
    }

    fn get_decimal<'a>(src: &mut Cursor<&'a [u8]>) -> anyhow::Result<i64> {
        let val = Self::get_line(src)?;
        let val: i64 =
            atoi::atoi(val).ok_or_else(|| anyhow::anyhow!("Protocol Error; non valid decimal"))?;
        Ok(val)
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
    use std::io::Cursor;

    use crate::resp::Frame;

    #[test]
    fn parse_int() {
        let val = b":1000\r\n";
        let resp =
            Frame::try_parse(&mut Cursor::new(val)).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::Int(1000));

        let val = b":+1000\r\n";
        let resp =
            Frame::try_parse(&mut Cursor::new(val)).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::Int(1000));

        let val = b":-1000\r\n";
        let resp =
            Frame::try_parse(&mut Cursor::new(val)).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::Int(-1000));

        // Test to string
        let resp_str = Frame::Int(1000).to_string();
        assert_eq!(":1000\r\n", resp_str);

        let resp_str = Frame::Int(-1000).to_string();
        assert_eq!(":-1000\r\n", resp_str);
    }

    #[test]
    fn parse_simple_string() {
        let val = b"+OK\r\n";
        let resp =
            Frame::try_parse(&mut Cursor::new(val)).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::SimpleString("OK".to_string()));

        assert_eq!(String::from_utf8(val.to_vec()).unwrap(), resp.to_string());
    }

    #[test]
    fn parse_bulk_string() {
        let val = b"$6\r\nhe\rllo\r\n";
        let resp =
            Frame::try_parse(&mut Cursor::new(val)).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::Bulk("he\rllo".as_bytes().to_vec()));
        assert_eq!(String::from_utf8(val.to_vec()).unwrap(), resp.to_string());

        // test empty string
        let val = b"$0\r\n\r\n";
        let resp =
            Frame::try_parse(&mut Cursor::new(val)).expect("Expected to parse valid RESP string");
        assert_eq!(resp, Frame::Bulk("".as_bytes().to_vec()));

        // nil string?
    }

    #[test]
    fn parse_array() {
        let val = b"*3\r\n+OK\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let resp =
            Frame::try_parse(&mut Cursor::new(val)).expect("Expected to parse valid RESP string");
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

        let val = b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n";
        let resp =
            Frame::try_parse(&mut Cursor::new(val)).expect("Expected to parse valid RESP string");
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
        assert_eq!(String::from_utf8(val.to_vec()).unwrap(), resp.to_string())
    }
}
