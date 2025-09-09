use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use anyhow::Ok;
use tokio::{
    sync::{Mutex, Notify},
    time::{sleep, Instant},
};

#[derive(Debug)]
struct CacheEntry {
    value: Entry,
    expire_at: Option<Instant>,
}

#[derive(Debug)]
enum Entry {
    Data(EntryData),
    List(VecDeque<EntryData>),
}

type EntryData = Vec<u8>;

impl Entry {
    fn push(&mut self, data: Vec<String>) -> anyhow::Result<usize> {
        if let Entry::List(list) = self {
            for elem in data {
                list.push_back(elem.into_bytes());
            }
            Ok(list.len())
        } else {
            Err(anyhow::anyhow!("Entry is not a List type"))
        }
    }
    fn append(&mut self, data: Vec<String>) -> anyhow::Result<usize> {
        if let Entry::List(list) = self {
            for elem in data {
                list.push_front(elem.into_bytes());
            }
            Ok(list.len())
        } else {
            Err(anyhow::anyhow!("Entry is not a List type"))
        }
    }
    fn range(&self, start: i64, end: i64) -> anyhow::Result<Vec<EntryData>> {
        let Entry::List(list) = self else {
            return Err(anyhow::anyhow!("Entry is not a List type"));
        };
        let start = if start >= 0 {
            start as usize
        } else {
            list.len().saturating_sub((start.abs()) as usize)
        };
        let end = if end >= 0 {
            end as usize
        } else {
            list.len().saturating_sub((end.abs()) as usize)
        };
        eprintln!(
            "list_len: {}, getting vals in range [{start},{end}]",
            list.len()
        );
        Ok(list
            .iter()
            .take(end + 1)
            .skip(start)
            .map(|r| r.to_owned())
            .collect())
    }
    fn len(&self) -> anyhow::Result<usize> {
        if let Entry::List(list) = self {
            Ok(list.len())
        } else {
            Err(anyhow::anyhow!("Entry is not a List type"))
        }
    }
    fn l_pop(&mut self, num_elems: usize) -> anyhow::Result<Option<Vec<EntryData>>> {
        if let Entry::List(list) = self {
            let mut data = Vec::with_capacity(num_elems);
            for _ in 0..num_elems {
                let Some(elem) = list.pop_front() else {
                    break;
                };
                data.push(elem);
            }
            Ok(if data.is_empty() { None } else { Some(data) })
        } else {
            Err(anyhow::anyhow!("Entry is not a List type"))
        }
    }
}

/// Basic shared cache with passive expiry.
// TODO: Periodically remove expired entries
/// From the [resis docs](https://redis.io/docs/latest/commands/expire/#how-redis-expires-keys)
/// A key is passively expired simply when some client tries to access it,
/// and the key is found to be timed out.
#[derive(Debug)]
pub struct Db {
    data: Arc<Mutex<HashMap<String, CacheEntry>>>,
    notify: Arc<Notify>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
        }
    }
    pub fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            notify: self.notify.clone(),
        }
    }
    pub async fn set(&self, key: String, value: String, expire_at: Option<Instant>) {
        let entry = CacheEntry {
            value: Entry::Data(value.into_bytes()),
            expire_at,
        };
        let mut locked_cache = self.data.lock().await;
        locked_cache.insert(key, entry);
    }
    // Get the value of key. If the key does not exist the special value nil is returned.
    // GET only handles string values.
    pub async fn get(&self, key: String) -> Option<Vec<u8>> {
        let mut locked_cache = self.data.lock().await;
        let Some(entry) = locked_cache.get(&key) else {
            return None;
        };
        let Entry::Data(value) = &entry.value else {
            panic!("should be a vec<u8>")
        };
        // return value if not expired
        if entry.expired() {
            eprintln!("Key is expired");
            locked_cache.remove(&key);
            None
        } else {
            Some(value.clone())
        }
    }
    pub async fn r_push(&self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let mut locked_cache = self.data.lock().await;
        let entry = locked_cache.entry(key).or_insert(CacheEntry {
            value: Entry::List(VecDeque::new()),
            expire_at: None,
        });
        let size = entry.value.push(values)?;
        self.notify.notify_waiters();
        Ok(size)
    }
    pub async fn l_push(&self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let mut locked_cache = self.data.lock().await;
        let entry = locked_cache.entry(key).or_insert(CacheEntry {
            value: Entry::List(VecDeque::new()),
            expire_at: None,
        });
        let size = entry.value.append(values)?;
        self.notify.notify_waiters();
        Ok(size)
    }
    pub async fn l_range(
        &self,
        key: String,
        start: i64,
        end: i64,
    ) -> anyhow::Result<Option<Vec<Vec<u8>>>> {
        let locked_cache = self.data.lock().await;
        locked_cache
            .get(&key)
            .map(|entry| entry.value.range(start, end))
            .transpose()
    }
    pub async fn l_len(&self, key: String) -> Option<usize> {
        let locked_cache = self.data.lock().await;
        locked_cache
            .get(&key)
            .map(|entry| entry.value.len().ok())
            .flatten()
    }
    pub async fn l_pop(
        &self,
        key: String,
        num_elems: usize,
    ) -> anyhow::Result<Option<Vec<EntryData>>> {
        let mut locked_cache = self.data.lock().await;
        let entry = locked_cache.entry(key);
        match entry {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().value.l_pop(num_elems)
            }
            std::collections::hash_map::Entry::Vacant(_) => Ok(None),
        }
    }
    /// `bl_pop` is a blocking variant of the `l_pop`.
    /// It allows clients to wait for an element to become available on one or more lists.
    ///
    /// If the list is empty, the command blocks until:
    /// - An element is pushed to the list
    /// - Or the specified timeout is reached (in seconds)
    ///         - It blocks indefinitely if the timeout specified is 0.
    pub async fn bl_pop(
        &self,
        key: String,
        timeout: Option<Instant>,
    ) -> anyhow::Result<Option<EntryData>> {
        loop {
            let val = self.l_pop(key.clone(), 1).await?;
            if let Some(val) = val {
                return Ok(val.into_iter().next());
            }
            if let Some(timeout) = timeout {
                let time_remaining = timeout - Instant::now();
                tokio::select! {
                    _ = self.notify.notified() => (),
                    _ = sleep(time_remaining) => {
                        eprintln!("bl_pop timed out");
                        return Ok(None);
                    }
                }
            } else {
                self.notify.notified().await;
            }
        }
    }

    pub async fn entry_type(&self, key: String) -> &'static str {
        let mut locked_cache = self.data.lock().await;
        let Some(entry) = locked_cache.get(&key) else {
            return "none";
        };

        if entry.expired() {
            eprintln!("Key is expired");
            locked_cache.remove(&key);
            return "none";
        }

        match entry.value {
            Entry::Data(_) => "string",
            Entry::List(_) => "list",
        }
    }
}

impl CacheEntry {
    fn expired(&self) -> bool {
        match self.expire_at {
            None => false,
            Some(expiry) => {
                if Instant::now() >= expiry {
                    true
                } else {
                    false
                }
            }
        }
    }
}

#[cfg(test)]
mod db_tests {

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

        let result = db
            .l_range("orange".to_string(), 0, -1)
            .await
            .expect("valid list");
        let Some(result) = result else {
            assert!(false, "list is not valid");
            return;
        };
        let expected: Vec<_> = ["apple", "pear", "blueberry"]
            .map(|v| v.to_string().into_bytes())
            .into_iter()
            .collect();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_l_pop() {
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

        let result = db.l_pop("orange".to_string(), 1).await.expect("valid list");
        let Some(result) = result else {
            assert!(false, "list is not valid");
            return;
        };
        assert_eq!(result[0], b"apple".to_vec());

        // test multi pop
        let result = db.l_pop("orange".to_string(), 2).await.expect("valid list");
        let Some(result) = result else {
            assert!(false, "list is not valid");
            return;
        };
        assert_eq!(result, vec![b"pear".to_vec(), b"blueberry".to_vec()]);
    }
}
