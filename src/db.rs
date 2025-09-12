use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    ops::Bound,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::{oneshot, Mutex},
    time::Instant,
};

use crate::resp::Frame;

#[derive(Debug)]
enum Entry {
    Data(ExpireableData),
    // TODO: Abstract away notify logic...
    List(NotifiedList),
    Stream(NotifyStream),
}

#[derive(Debug, Default)]
struct NotifyStream {
    // TODO: look into better options, does the stream data need to be easily searchable?
    // Could we replace the inner BTreeMap with a Vec<(string, string)>
    data: BTreeMap<StreamId, BTreeMap<String, String>>,
    waiters: VecDeque<oneshot::Sender<()>>,
}

impl NotifyStream {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            waiters: VecDeque::new(),
        }
    }

    fn add_waiter(&mut self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.waiters.push_back(tx);
        rx
    }

    fn notify_one(&mut self) -> bool {
        while let Some(waiter) = self.waiters.pop_front() {
            if waiter.send(()).is_ok() {
                return true;
            }
            // If sending failed, receiver dropped; try the next one
        }
        return false;
    }

    fn notify_n(&mut self, num_notifications: usize) {
        for _ in 0..num_notifications {
            if !self.notify_one() {
                // No one left to notify
                break;
            }
        }
    }

    fn add(&mut self, entry_id: String, field_values: &[String]) -> anyhow::Result<String> {
        let entry_id = self.get_next_stream_entry_id(entry_id)?;

        let entry = self.data.entry(entry_id).or_insert(BTreeMap::default());
        let num_notifications = field_values.len() / 2;
        for (field, value) in field_values.chunks_exact(2).map(|c| (&c[0], &c[1])) {
            // should never have a value...since we check during get_next_stream_entry_id
            let _ = entry.insert(field.clone(), value.clone());
        }
        self.notify_n(num_notifications);
        Ok(entry_id.to_string())
    }

    /// Gets next entry_id and validates it.
    /// stream id are always composed of two integers: `<millisecondsTime>`-`<sequenceNumber>`
    /// Entry IDs are unique within a stream, and they're guaranteed to be incremental
    fn get_next_stream_entry_id(&self, entry_id: String) -> anyhow::Result<StreamId> {
        let prev_entry_id = self.data.last_key_value().map(|(k, _v)| k);
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

    fn push_back(&mut self, val: EntryData) {
        self.list.push_back(val);
        self.notify_one();
    }

    fn push_front(&mut self, val: EntryData) {
        self.list.push_front(val);
        self.notify_one();
    }

    fn pop_front(&mut self) -> Option<EntryData> {
        self.list.pop_front()
    }

    fn len(&self) -> usize {
        self.list.len()
    }

    fn range(&self, start: i64, end: i64) -> Vec<EntryData> {
        let start = if start >= 0 {
            start as usize
        } else {
            self.list.len().saturating_sub((start.abs()) as usize)
        };
        let end = if end >= 0 {
            end as usize
        } else {
            self.list.len().saturating_sub((end.abs()) as usize)
        };
        eprintln!(
            "list_len: {}, getting vals in range [{start},{end}]",
            self.list.len()
        );
        self.list
            .iter()
            .take(end + 1)
            .skip(start)
            .map(|r| r.to_owned())
            .collect()
    }
}

#[derive(Debug)]
struct ExpireableData {
    value: EntryData,
    expire_at: Option<Instant>,
}

impl ExpireableData {
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

/// Basic shared cache with passive expiry.
// TODO: Periodically remove expired entries
/// From the [resis docs](https://redis.io/docs/latest/commands/expire/#how-redis-expires-keys)
/// A key is passively expired simply when some client tries to access it,
/// and the key is found to be timed out.
#[derive(Debug)]
pub struct Db {
    data: Arc<Mutex<HashMap<String, Entry>>>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    /// Clones internal Arc references and returns a struct with said clones
    pub fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }

    pub async fn set(&self, key: String, value: String, expire_at: Option<Instant>) {
        let expirable_data = ExpireableData {
            value: value.into_bytes(),
            expire_at,
        };
        let mut locked_cache = self.data.lock().await;
        locked_cache.insert(key, Entry::Data(expirable_data));
    }

    /// Get the value of key. If the key does not exist the special value nil is returned.
    /// GET only handles string values.
    pub async fn get(&self, key: String) -> Option<Vec<u8>> {
        let mut locked_cache = self.data.lock().await;
        let Some(entry) = locked_cache.get(&key) else {
            return None;
        };
        let Entry::Data(expirable_data) = &entry else {
            panic!("should be a vec<u8>")
        };
        // return value if not expired
        if expirable_data.expired() {
            eprintln!("Key is expired");
            locked_cache.remove(&key);
            None
        } else {
            Some(expirable_data.value.clone())
        }
    }

    pub async fn r_push(&self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let mut locked_cache = self.data.lock().await;
        let entry = locked_cache
            .entry(key)
            .or_insert(Entry::List(NotifiedList::new()));

        let Entry::List(notify_list) = entry else {
            anyhow::bail!("Entry is not List type");
        };

        for val in values {
            notify_list.push_back(val.into_bytes());
        }
        Ok(notify_list.len())
    }

    pub async fn l_push(&self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let mut locked_cache = self.data.lock().await;
        let entry = locked_cache
            .entry(key)
            .or_insert(Entry::List(NotifiedList::new()));

        let Entry::List(notify_list) = entry else {
            anyhow::bail!("Entry is not List type");
        };

        for val in values {
            notify_list.push_front(val.into_bytes());
        }
        Ok(notify_list.len())
    }

    pub async fn l_range(&self, key: String, start: i64, end: i64) -> anyhow::Result<Vec<Vec<u8>>> {
        let locked_cache = self.data.lock().await;

        match locked_cache.get(&key) {
            None => Ok(vec![]),
            Some(Entry::List(list)) => Ok(list.range(start, end)),
            Some(_) => Err(anyhow::anyhow!("Entry is not List type")),
        }
    }

    pub async fn l_len(&self, key: String) -> anyhow::Result<usize> {
        let locked_cache = self.data.lock().await;
        locked_cache
            .get(&key)
            .map(|entry| match entry {
                Entry::List(list) => Ok(list.len()),
                _ => Err(anyhow::anyhow!("Entry is not List type")),
            })
            .unwrap_or(Ok(0))
    }

    pub async fn l_pop(&self, key: String, num_elems: usize) -> anyhow::Result<Vec<EntryData>> {
        let mut locked_cache = self.data.lock().await;
        match locked_cache.get_mut(&key) {
            None => Ok(vec![]),
            Some(Entry::List(notify_list)) => {
                let mut data = Vec::with_capacity(num_elems);
                for _ in 0..num_elems {
                    let Some(elem) = notify_list.pop_front() else {
                        break;
                    };
                    data.push(elem);
                }
                Ok(data)
            }
            Some(_) => Err(anyhow::anyhow!("Entry is not List type")),
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

            let entry = locked_cache
                .entry(key.clone())
                .or_insert(Entry::List(NotifiedList::new()));

            let Entry::List(notify_list) = entry else {
                return Err(anyhow::anyhow!("Entry is not a list type"));
            };

            if let Some(data) = notify_list.pop_front() {
                return Ok(Some(data));
            }

            // no data, add waiter to list
            let waiter = notify_list.add_waiter();
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

        match entry {
            Entry::Data(d) => {
                if d.expired() {
                    eprintln!("Key is expired");
                    locked_cache.remove(&key);
                    "none"
                } else {
                    "string"
                }
            }
            Entry::List(_) => "list",
            Entry::Stream(_) => "stream",
        }
    }

    pub async fn x_add(
        &self,
        stream_key: String,
        stream_entry_id: String,
        field_values: &[String],
    ) -> anyhow::Result<String> {
        if field_values.len() % 2 != 0 {
            anyhow::bail!("Expected even number of field-value args");
        }
        let mut locked_cache = self.data.lock().await;

        let stream_entry = locked_cache
            .entry(stream_key)
            .or_insert(Entry::Stream(NotifyStream::new()));
        let Entry::Stream(notify_stream) = stream_entry else {
            anyhow::bail!("Entry is not a stream");
        };

        notify_stream.add(stream_entry_id, field_values)
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
        let lower_bound: StreamId = {
            if lower_bound == "-" {
                "0-1".to_string()
            } else if lower_bound.contains("-") {
                lower_bound
            } else {
                format!("{lower_bound}-0")
            }
        }
        .try_into()
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        let upper_bound: StreamId = {
            if upper_bound == "+" {
                format!("{}-{}", u128::MAX, usize::MAX)
            } else if upper_bound.contains("-") {
                upper_bound
            } else {
                format!("{upper_bound}-{}", usize::MAX)
            }
        }
        .try_into()
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        self.read_stream(
            None,
            key,
            Bound::Included(lower_bound),
            Bound::Included(upper_bound),
        )
        .await
    }
    pub async fn x_read(
        &self,
        block: Option<u64>,
        key: String,
        lower_bound: String,
    ) -> anyhow::Result<Frame> {
        eprintln!("[x_read] key: {key}, lower_bound: {lower_bound}");
        let lower_bound: StreamId = {
            if lower_bound == "-" {
                "0-1".to_string()
            } else if lower_bound.contains("-") {
                lower_bound
            } else {
                format!("{lower_bound}-0")
            }
        }
        .try_into()
        .map_err(|e| anyhow::anyhow!("failed parsing lower_boud with {e}"))?;

        let stream_data = self
            .read_stream(
                block,
                key.clone(),
                Bound::Excluded(lower_bound),
                Bound::Unbounded,
            )
            .await?;

        if let Frame::NullArray = stream_data {
            Ok(Frame::NullArray)
        } else {
            let stream_data = Frame::Array([Frame::buld_from_string(key), stream_data].to_vec());
            Ok(stream_data)
        }
    }

    async fn read_stream(
        &self,
        block: Option<u64>,
        key: String,
        lower_bound: Bound<StreamId>,
        upper_bound: Bound<StreamId>,
    ) -> anyhow::Result<Frame> {
        loop {
            let mut locked_cache = self.data.lock().await;
            let entry = {
                locked_cache
                    .entry(key.clone())
                    .or_insert(Entry::Stream(NotifyStream::new()))
            };

            let Entry::Stream(stream_map) = entry else {
                anyhow::bail!("Entry is not a stream");
            };

            // Should some of this be moved to NotifyStream?
            let mut output = vec![];
            for (stream_id, data) in stream_map.data.range((lower_bound, upper_bound)) {
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
            if !output.is_empty() {
                return Ok(Frame::Array(output));
            }
            let waiter = stream_map.add_waiter();
            drop(locked_cache);
            if let Some(millis) = block {
                if millis == 0 {
                    waiter.await?;
                } else {
                    match tokio::time::timeout(Duration::from_millis(millis), waiter).await {
                        Ok(_) => (),
                        Err(e) => {
                            eprintln!("Timed out: {e:?}");
                            // only send null array when using block?
                            return Ok(Frame::NullArray);
                        }
                    }
                }
            } else {
                return Ok(Frame::Array(vec![]));
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
        assert_eq!(1, db.l_len("orange".to_string()).await.unwrap());

        let _ = db
            .l_push(
                "orange".to_string(),
                vec!["pear".to_string(), "apple".to_string()],
            )
            .await;
        assert_eq!(3, db.l_len("orange".to_string()).await.unwrap());

        let result = db.l_range("orange".to_string(), 0, -1).await.unwrap();

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
        assert_eq!(
            1,
            db.l_len("orange".to_string())
                .await
                .expect("expected a list")
        );

        let _ = db
            .l_push(
                "orange".to_string(),
                vec!["pear".to_string(), "apple".to_string()],
            )
            .await;
        assert_eq!(3, db.l_len("orange".to_string()).await.unwrap());

        let result = db.l_pop("orange".to_string(), 1).await.unwrap();
        assert_eq!(result[0], b"apple".to_vec());

        // test multi pop
        let result = db.l_pop("orange".to_string(), 2).await.unwrap();
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
                "0-1".to_string(),
                &["r".to_string(), "s".to_string()],
            )
            .await;
        let _ = db
            .x_add(
                "mango".to_string(),
                "0-2".to_string(),
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
            .x_range("mango".to_string(), "0".to_string(), "2".to_string())
            .await;
        assert!(x_range_res.is_ok());

        let x_range_res = db
            .x_range("mango".to_string(), "0-1".to_string(), "2-1".to_string())
            .await;
        assert!(x_range_res.is_ok());

        let x_range_res = db
            .x_range("mango".to_string(), "-".to_string(), "2-1".to_string())
            .await;
        assert!(x_range_res.is_ok());
        let x_range_res = db
            .x_range("mango".to_string(), "-".to_string(), "+".to_string())
            .await;
        assert!(x_range_res.is_ok());
    }
}
