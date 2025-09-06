#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RESP {
    Int(i64),
    String(String),
    Error(String),
    Array(Vec<RESP>),
}

impl RESP {
    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        eprintln!("Parsing: {s}");
        Self::recursive_parse(s).map(|(r, cnt)| {
            eprint!("Read {cnt} bytes, remaining str {}", &s[cnt..]);
            r
        })
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
                (RESP::Int(val), break_idx + 2 + 1)
            }
            "+" => {
                let break_idx = val
                    .find("\r\n")
                    .ok_or(anyhow::anyhow!("No CRLF termination found"))?;
                (
                    RESP::String(val[..break_idx].to_string()),
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
                (
                    RESP::String(remaining[..len].to_string()),
                    len + break_idx + 4 + 1,
                )
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
                    let (v, bytes_read) = Self::recursive_parse(dbg!(&val[offset..]))?;
                    dbg!(&v, bytes_read);
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
            Self::String(val) => {
                if val.contains("\r") || val.contains("\n") {
                    format!("${}\r\n{val}\r\n", val.len())
                } else {
                    format!("+{val}\r\n")
                }
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
        }
    }
}

#[cfg(test)]
mod test_resp {
    use crate::RESP;

    #[test]
    fn parse_int() {
        let val = ":1000\r\n";
        let resp = RESP::from_str(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, RESP::Int(1000));

        let val = ":+1000\r\n";
        let resp = RESP::from_str(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, RESP::Int(1000));

        let val = ":-1000\r\n";
        let resp = RESP::from_str(&val).expect("Expected to parse valid RESP string");
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
        let resp = RESP::from_str(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, RESP::String("OK".to_string()));

        assert_eq!(val, resp.to_string());
    }

    #[test]
    fn parse_bulk_string() {
        let val = "$6\r\nhe\rllo\r\n";
        let resp = RESP::from_str(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, RESP::String("he\rllo".to_string()));
        assert_eq!(val, resp.to_string());

        // test empty string
        let val = "$0\r\n\r\n";
        let resp = RESP::from_str(&val).expect("Expected to parse valid RESP string");
        assert_eq!(resp, RESP::String("".to_string()));

        // nil string?
    }

    #[test]
    fn parse_array() {
        let val = "*3\r\n+OK\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let resp = RESP::from_str(&val).expect("Expected to parse valid RESP string");
        assert_eq!(
            resp,
            RESP::Array(vec![
                RESP::String("OK".to_string()),
                RESP::String("hello".to_string()),
                RESP::String("world".to_string()),
            ])
        );

        let val = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n";
        let resp = RESP::from_str(&val).expect("Expected to parse valid RESP string");
        assert_eq!(
            resp,
            RESP::Array(vec![
                RESP::Array(vec![RESP::Int(1), RESP::Int(2), RESP::Int(3),]),
                RESP::Array(vec![
                    RESP::String("Hello".to_string()),
                    RESP::Error("World".to_string()),
                ])
            ])
        );
        assert_eq!(val, resp.to_string())
    }
}
