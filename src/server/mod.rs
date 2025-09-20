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
    pub fn get_info(&self) -> String {
        (&self.info).into()
    }
}

#[derive(Debug)]
struct Info {
    role: String,
}

impl From<&Info> for String {
    fn from(value: &Info) -> Self {
        format!("role:{}", value.role)
    }
}
