#[derive(Debug, Clone)]
pub struct Server {
    info: Info,
}

impl Server {
    pub fn new() -> Self {
        Self {
            info: Info {
                role: "master".to_string(),
            },
        }
    }
    pub fn replicate(_master: &str) -> Self {
        Self {
            info: Info {
                role: "slave".to_string(),
            },
        }
    }
    pub fn get_info(&self) -> String {
        (&self.info).into()
    }
}

#[derive(Debug, Clone)]
struct Info {
    role: String,
}

impl From<&Info> for String {
    fn from(value: &Info) -> Self {
        format!("role:{}", value.role)
    }
}
