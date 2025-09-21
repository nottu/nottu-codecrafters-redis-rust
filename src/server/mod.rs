use crate::server::db::Db;

pub mod db;

#[derive(Debug)]
pub struct Server {
    info: Info,
    pub db: Db,
}

impl Server {
    pub fn new() -> Self {
        Self {
            info: Info {
                role: "master".to_string(),
            },
            db: Db::new(),
        }
    }
    pub fn replicate(_master: &str) -> Self {
        Self {
            info: Info {
                role: "slave".to_string(),
            },
            db: Db::new(),
        }
    }
    pub fn get_info(&self) -> String {
        (&self.info).into()
    }
}

impl Clone for Server {
    fn clone(&self) -> Self {
        Self {
            info: self.info.clone(),
            db: self.db.clone(),
        }
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
