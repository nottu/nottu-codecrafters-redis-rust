use std::{collections::HashMap, sync::Arc};

use codecrafters_redis::RESP;
use tokio::{sync::Mutex, time::Instant};

#[derive(Debug)]
struct CacheEntry {
    value: RESP,
    expire_at: Option<Instant>,
}

/// Basic shared cache with passive expiry.
// TODO: Periodically remove expired entries
/// From the [resis docs](https://redis.io/docs/latest/commands/expire/#how-redis-expires-keys)
/// A key is passively expired simply when some client tries to access it,
/// and the key is found to be timed out.
#[derive(Debug)]
pub struct Db {
    data: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
    pub async fn set(&self, key: String, value: String, expire_at: Option<Instant>) {
        let entry = CacheEntry {
            value: RESP::Bulk(value.into_bytes()),
            expire_at,
        };
        let mut locked_cache = self.data.lock().await;
        locked_cache.insert(key, entry);
    }
    pub async fn get(&self, key: String) -> Option<RESP> {
        let mut locked_cache = self.data.lock().await;
        if let Some(cached_val) = locked_cache.get(&key) {
            match cached_val.expire_at {
                None => Some(cached_val.value.clone()),
                Some(expiry) => {
                    if Instant::now() >= expiry {
                        eprintln!("Key is expired");
                        locked_cache.remove(&key);
                        None
                    } else {
                        Some(cached_val.value.clone())
                    }
                }
            }
        } else {
            None
        }
    }
    pub async fn r_push(&self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let mut locked_cache = self.data.lock().await;
        let entry = locked_cache.entry(key).or_insert(CacheEntry {
            value: RESP::empty_array(),
            expire_at: None,
        });
        for value in values {
            let _ = entry.value.push(value);
        }
        entry.value.len()
    }
    pub async fn l_push(&self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let mut locked_cache = self.data.lock().await;
        let entry = locked_cache.entry(key).or_insert(CacheEntry {
            value: RESP::empty_array(),
            expire_at: None,
        });
        for value in values {
            let _ = entry.value.prepend(value);
        }
        entry.value.len()
    }
    pub async fn l_range(&self, key: String, start: i64, end: i64) -> Option<RESP> {
        let locked_cache = self.data.lock().await;
        locked_cache
            .get(&key)
            .map(|entry| entry.value.range(start, end).ok())
            .flatten()
    }
    pub async fn l_len(&self, key: String) -> Option<usize> {
        let locked_cache = self.data.lock().await;
        locked_cache
            .get(&key)
            .map(|entry| entry.value.len().ok())
            .flatten()
    }
}

#[cfg(test)]
mod db_tests {
    use codecrafters_redis::RESP;

    use crate::db::Db;

    #[tokio::test]
    async fn test_l_range() {
        let db = Db::new();

        let _ = db
            .l_push("orange".to_string(), vec!["blueberry".to_string()])
            .await;
        assert_eq!(Some(1), db.l_len("orange".to_string()).await);

        let _ = db
            .l_push(
                "orange".to_string(),
                vec!["pear".to_string(), "apple".to_string()],
            )
            .await;
        assert_eq!(Some(3), db.l_len("orange".to_string()).await);

        let result = db.l_range("orange".to_string(), 0, -1).await;
        let Some(RESP::Array(result)) = result else {
            assert!(false, "list is not valid");
            return;
        };
        let expected: Vec<_> = ["apple", "pear", "blueberry"]
            .map(|v| RESP::Bulk(v.as_bytes().to_vec()))
            .into_iter()
            .collect();
        assert_eq!(result, expected);
    }
}
