use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    ops::Bound,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::{oneshot, Mutex, Notify},
    time::Instant,
};

use crate::resp::Frame;

#[derive(Debug)]
enum Entry {
    Data(EntryData),
    List(NotifiedList),
    // TODO: look into better options
    Stream(BTreeMap<StreamId, BTreeMap<String, String>>),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct StreamId {
    millisecond_time: u128,
    sequence_number: usize,
}

impl StreamId {
    fn to_string(&self) -> String {
        format!("{}-{}", self.millisecond_time, self.sequence_number)
    }
}

impl TryFrom<String> for StreamId {
    type Error = &'static str;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut vals = value.split("-");
        let millis = vals.next().ok_or("expected millis")?;
        let seq = vals.next().ok_or("expected sequence")?;

        let millisecond_time: u128 = millis.parse().map_err(|_| "failed to parse millis")?;
        let sequence_number: usize = seq.parse().map_err(|_| "failed to parse sequence")?;
        Ok(Self {
            millisecond_time,
            sequence_number,
        })
    }
}

#[derive(Debug)]
struct NotifiedList {
    list: VecDeque<EntryData>,
    waiters: VecDeque<oneshot::Sender<()>>,
}

impl NotifiedList {
    fn new() -> Self {
        Self {
            list: VecDeque::new(),
            waiters: VecDeque::new(),
        }
    }
    fn notify_one(&mut self) {
        while let Some(waiter) = self.waiters.pop_front() {
            if waiter.send(()).is_ok() {
                break; // success
            }
            // If sending failed, receiver dropped; try the next one
        }
    }
    fn add_waiter(&mut self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.waiters.push_back(tx);
        rx
    }
}

#[derive(Debug)]
struct CacheEntry {
    value: Entry,
    expire_at: Option<Instant>,
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

type EntryData = Vec<u8>;

impl Entry {
    fn push(&mut self, data: Vec<String>) -> anyhow::Result<usize> {
        if let Entry::List(notify_list) = self {
            for elem in data {
                notify_list.list.push_back(elem.into_bytes());
                notify_list.notify_one();
            }
            Ok(notify_list.list.len())
        } else {
            Err(anyhow::anyhow!("Entry is not a List type"))
        }
    }
    fn append(&mut self, data: Vec<String>) -> anyhow::Result<usize> {
        if let Entry::List(notify_list) = self {
            for elem in data {
                notify_list.list.push_front(elem.into_bytes());
                notify_list.notify_one();
            }
            Ok(notify_list.list.len())
        } else {
            Err(anyhow::anyhow!("Entry is not a List type"))
        }
    }
    fn range(&self, start: i64, end: i64) -> anyhow::Result<Vec<EntryData>> {
        let Entry::List(notify_list) = self else {
            return Err(anyhow::anyhow!("Entry is not a List type"));
        };
        let start = if start >= 0 {
            start as usize
        } else {
            notify_list
                .list
                .len()
                .saturating_sub((start.abs()) as usize)
        };
        let end = if end >= 0 {
            end as usize
        } else {
            notify_list.list.len().saturating_sub((end.abs()) as usize)
        };
        eprintln!(
            "list_len: {}, getting vals in range [{start},{end}]",
            notify_list.list.len()
        );
        Ok(notify_list
            .list
            .iter()
            .take(end + 1)
            .skip(start)
            .map(|r| r.to_owned())
            .collect())
    }
    fn len(&self) -> anyhow::Result<usize> {
        if let Entry::List(notify_list) = self {
            Ok(notify_list.list.len())
        } else {
            Err(anyhow::anyhow!("Entry is not a List type"))
        }
    }
    fn l_pop(&mut self, num_elems: usize) -> anyhow::Result<Option<Vec<EntryData>>> {
        if let Entry::List(notify_list) = self {
            let mut data = Vec::with_capacity(num_elems);
            for _ in 0..num_elems {
                let Some(elem) = notify_list.list.pop_front() else {
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
            value: Entry::List(NotifiedList::new()),
            expire_at: None,
        });
        let size = entry.value.push(values)?;
        self.notify.notify_waiters();
        Ok(size)
    }
    pub async fn l_push(&self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let mut locked_cache = self.data.lock().await;
        let entry = locked_cache.entry(key).or_insert(CacheEntry {
            value: Entry::List(NotifiedList::new()),
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
            let mut locked_cache = self.data.lock().await;

            let entry = locked_cache.entry(key.clone()).or_insert(CacheEntry {
                value: Entry::List(NotifiedList::new()),
                expire_at: None,
            });

            let Entry::List(list) = &mut entry.value else {
                return Err(anyhow::anyhow!("Entry is not a list type"));
            };

            if let Some(data) = list.list.pop_front() {
                return Ok(Some(data));
            }

            // no data, add waiter to list
            let waiter = list.add_waiter();
            // release cache, so others operations can go through
            drop(locked_cache);

            if let Some(timeout) = timeout {
                match tokio::time::timeout_at(timeout, waiter).await {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("{e:?}");
                        return Ok(None);
                    }
                }
            } else {
                waiter.await?;
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
            Entry::Stream(_) => "stream",
        }
    }

    pub async fn x_add(
        &self,
        key: String,
        stream_id: String,
        field_values: &[String],
    ) -> anyhow::Result<String> {
        if field_values.len() % 2 != 0 {
            anyhow::bail!("Expected even number of field-value args");
        }
        let mut locked_cache = self.data.lock().await;

        let entry = locked_cache.entry(key).or_insert(CacheEntry {
            value: Entry::Stream(BTreeMap::new()),
            expire_at: None,
        });
        let Entry::Stream(stream_map) = &mut entry.value else {
            anyhow::bail!("Entry is not a stream");
        };

        let prev_entry_id = stream_map.last_key_value().map(|(k, _v)| k);
        let entry_id = Self::get_next_stream_entry_id(&stream_id, prev_entry_id)?;

        let stream_entry = stream_map.entry(entry_id).or_insert(BTreeMap::new());
        for (field, value) in field_values.chunks_exact(2).map(|c| (&c[0], &c[1])) {
            let _ = stream_entry.insert(field.clone(), value.clone());
        }
        Ok(format!(
            "{}-{}",
            entry_id.millisecond_time, entry_id.sequence_number
        ))
    }
    /// On top of the `key` for the stream. It takes two arguments: `start` and `end`. Both are entry IDs.
    ///
    /// The command returns all entries in the stream with IDs between the start and end IDs.
    /// This range is "inclusive", which means that the response will includes entries with IDs that are equal
    /// to the start and end IDs.
    ///
    /// The sequence number doesn't need to be included in the start and end IDs provided to the command.
    /// If not provided, `XRANGE` defaults to a sequence number of 0 for the start and the maximum sequence
    /// number for the end.
    pub async fn x_range(
        &self,
        key: String,
        lower_bound: String,
        upper_bound: String,
    ) -> anyhow::Result<Frame> {
        let mut locked_cache = self.data.lock().await;

        let entry = locked_cache.entry(key).or_insert(CacheEntry {
            value: Entry::Stream(BTreeMap::new()),
            expire_at: None,
        });
        let Entry::Stream(stream_map) = &mut entry.value else {
            anyhow::bail!("Entry is not a stream");
        };
        let lower_bound: StreamId = {
            if lower_bound.contains("-") {
                lower_bound
            } else {
                format!("{lower_bound}-0")
            }
        }
        .try_into()
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        let upper_bound: StreamId = {
            if upper_bound.contains("-") {
                upper_bound
            } else {
                format!("{upper_bound}-{}", usize::MAX)
            }
        }
        .try_into()
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        eprintln!("Getting values in range {lower_bound:?}, {upper_bound:?}");

        let mut output = vec![];
        for (stream_id, data) in
            stream_map.range((Bound::Included(lower_bound), Bound::Included(upper_bound)))
        {
            let values: Vec<Frame> = data
                .iter()
                .map(|(k, v)| [k, v])
                .flatten()
                .map(|s| Frame::bulk_from_str(s))
                .collect();
            output.push(Frame::Array(
                [
                    Frame::buld_from_string(stream_id.to_string()),
                    Frame::Array(values),
                ]
                .to_vec(),
            ));
        }
        Ok(Frame::Array(output))
    }
    /// Gets next entry_id and validates it.
    /// stream id are always composed of two integers: `<millisecondsTime>`-`<sequenceNumber>`
    /// Entry IDs are unique within a stream, and they're guaranteed to be incremental
    fn get_next_stream_entry_id(
        entry_id: &str,
        prev_entry_id: Option<&StreamId>,
    ) -> anyhow::Result<StreamId> {
        if entry_id == "0-0" {
            anyhow::bail!("ERR The ID specified in XADD must be greater than 0-0");
        }

        let mut id_parts = entry_id.splitn(2, "-");

        let millis_part = id_parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("ERR invalid stream ID format: missing millis"))?;
        let seq_part = id_parts.next();

        let millisecond_time = match millis_part {
            "*" => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("expected future time")
                .as_millis(),
            _ => millis_part
                .parse::<u128>()
                .map_err(|_| anyhow::anyhow!("ERR invalid millisecond timestamp in stream ID"))?,
        };

        if let Some(prev_entry_id) = prev_entry_id {
            if prev_entry_id.millisecond_time > millisecond_time {
                eprintln!("matching prev ids?");
                anyhow::bail!("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }

        let sequence_number = match seq_part {
            Some("*") | None => match prev_entry_id {
                Some(prev) if prev.millisecond_time == millisecond_time => {
                    // TODO: should this return an error? too may sequences?
                    prev.sequence_number.saturating_add(1)
                }
                _ => {
                    if millisecond_time == 0 {
                        1
                    } else {
                        0
                    }
                }
            },
            Some(seq) => seq
                .parse::<usize>()
                .map_err(|_| anyhow::anyhow!("ERR invalid sequence number in stream ID"))?,
        };

        let new_entry_id = StreamId {
            millisecond_time,
            sequence_number,
        };

        if let Some(prev) = prev_entry_id {
            if new_entry_id <= *prev {
                anyhow::bail!(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                );
            }
        }

        Ok(new_entry_id)
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

    #[tokio::test]
    async fn test_x_add() {
        let db = Db::new();
        let add_res = db
            .x_add(
                "mango".to_string(),
                "1-1".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;
        assert!(add_res.is_ok());
        let add_res = db
            .x_add(
                "mango".to_string(),
                "1-1".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;
        assert!(add_res.is_err());
        let err = add_res.unwrap_err();
        assert_eq!(
            err.to_string(),
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                .to_string()
        );

        let add_res = db
            .x_add(
                "mango".to_string(),
                "0-0".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;
        dbg!(&add_res);
        assert!(add_res.is_err());
        let err = add_res.unwrap_err();
        assert_eq!(
            err.to_string(),
            "ERR The ID specified in XADD must be greater than 0-0".to_string()
        )
    }

    #[tokio::test]
    async fn test_x_add_auto_seq() {
        let db = Db::new();
        let add_res = db
            .x_add(
                "mango".to_string(),
                "1-*".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;
        assert!(add_res.is_ok());
        assert_eq!("1-0", add_res.unwrap());

        let add_res = db
            .x_add(
                "mango".to_string(),
                "1-*".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;
        assert!(add_res.is_ok());
        assert_eq!("1-1", add_res.unwrap());

        let add_res = db
            .x_add(
                "mango".to_string(),
                "1-0".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;
        assert!(add_res.is_err());
        let err = add_res.unwrap_err();
        assert_eq!(
            err.to_string(),
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                .to_string()
        );

        let add_res = db
            .x_add(
                "blue".to_string(),
                "0-*".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;
        assert!(add_res.is_ok());
        assert_eq!("0-1", add_res.unwrap());

        let add_res = db
            .x_add(
                "blue".to_string(),
                "*".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;
        assert!(add_res.is_ok());
    }

    #[tokio::test]
    async fn test_x_range() {
        let db = Db::new();
        let _ = db
            .x_add(
                "mango".to_string(),
                "1-1".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;
        let _ = db
            .x_add(
                "mango".to_string(),
                "2-1".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;
        let _ = db
            .x_add(
                "mango".to_string(),
                "3-1".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;

        let x_range_res = db
            .x_range("mango".to_string(), "1".to_string(), "2".to_string())
            .await;
        assert!(x_range_res.is_ok());
        let x_range_res = db
            .x_range("mango".to_string(), "1-0".to_string(), "2-1".to_string())
            .await;
        assert!(x_range_res.is_ok())
    }
}
