use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    ops::Bound,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::bail;
use tokio::{
    sync::{oneshot, Mutex, MutexGuard},
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
    // TODO: can be changed to a Notify
    waiters: VecDeque<oneshot::Sender<()>>,
    // notify: Notify,
}

//
impl NotifyStream {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            waiters: VecDeque::new(),
            // notify: Notify::new(),
        }
    }

    fn add_waiter(&mut self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.waiters.push_back(tx);
        rx
    }

    fn notify_all(&mut self) {
        while let Some(waiter) = self.waiters.pop_front() {
            let _ = waiter.send(());
        }
    }

    fn add(&mut self, entry_id: String, field_values: &[String]) -> anyhow::Result<String> {
        let entry_id = self.generate_stream_entry_id(entry_id)?;

        let entry = self.data.entry(entry_id).or_insert(BTreeMap::default());
        for (field, value) in field_values.chunks_exact(2).map(|c| (&c[0], &c[1])) {
            // should never have a value...since we check during get_next_stream_entry_id
            let _ = entry.insert(field.clone(), value.clone());
        }
        self.notify_all();
        Ok(entry_id.to_string())
    }

    /// Generates entry_id and validates it.
    ///
    /// stream id are always composed of two integers: `<millisecondsTime>`-`<sequenceNumber>`
    /// Entry IDs are unique within a stream, and they're guaranteed to be incremental
    fn generate_stream_entry_id(&self, entry_id: String) -> anyhow::Result<StreamId> {
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

    fn get_last_stream_entry_id(&self) -> Option<StreamId> {
        self.data.last_key_value().map(|(k, _v)| k.clone())
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

    const fn zero() -> Self {
        Self {
            millisecond_time: 0,
            sequence_number: 0,
        }
    }
}

impl TryFrom<&str> for StreamId {
    type Error = &'static str;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
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
pub(super) struct Db {
    data: Arc<Mutex<HashMap<String, Entry>>>,
}

// TODO: Use custom error types
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

    pub async fn get_guarded<'a>(&'a mut self) -> GuardedDb<'a> {
        GuardedDb {
            data: self.data.lock().await,
        }
    }

    /// `bl_pop` is a blocking variant of the `l_pop`.
    /// It allows clients to wait for an element to become available on one or more lists.
    ///
    /// If the list is empty, the command blocks until:
    /// - An element is pushed to the list
    /// - Or the specified timeout is reached (in seconds)
    ///         - It blocks indefinitely if the timeout specified is 0.
    pub async fn blocking_list_pop(
        &self,
        key: &str,
        timeout: &Option<Instant>,
    ) -> anyhow::Result<Option<EntryData>> {
        loop {
            let mut locked_cache = self.data.lock().await;

            let entry = locked_cache
                .entry(key.to_string())
                .or_insert(Entry::List(NotifiedList::new()));

            let Entry::List(notify_list) = entry else {
                anyhow::bail!("Err value is not a list");
            };

            if let Some(data) = notify_list.pop_front() {
                return Ok(Some(data));
            }

            // no data, add waiter to list
            let waiter = notify_list.add_waiter();
            // release cache, so others operations can go through
            drop(locked_cache);

            if let Some(timeout) = timeout {
                // Instant is Copy
                match tokio::time::timeout_at(timeout.clone(), waiter).await {
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

    /// Blocking read of a stream, reading from the range `(lower_bound, end]`.
    /// Will wait upto `block_millis` for fata if no data exists in the stream for the given range.
    ///
    /// Parses `lower_bound` in the following manner:
    ///
    /// * `$` Read starting from the last entry
    /// * `%` Read all items in stream including all sequence numbers, equivalen to `"0-1"`
    /// * `<time>` Read all items after a give time stamp, returns all sequence numbers
    /// * `<time>-<sequence_number>`
    pub async fn blocking_x_read(
        &self,
        block_millis: u64,
        key: String,
        lower_bound: String,
    ) -> anyhow::Result<Frame> {
        let lower_bound: StreamId = {
            GuardedDb {
                data: self.data.lock().await,
            }
            .parse_xread_lower_bound(&key, &lower_bound)
            .await?
        };

        let stream_data = self
            .blocking_read_stream(
                block_millis,
                key.clone(),
                Bound::Excluded(lower_bound),
                Bound::Unbounded,
            )
            .await?;

        if stream_data.is_empty() {
            Ok(Frame::NullArray)
        } else {
            let stream_data =
                Frame::Array([Frame::bulk_from_string(key), Frame::Array(stream_data)].to_vec());
            Ok(stream_data)
        }
    }

    /// A wrapper over read_stream that blocks for the given `block_millis` time.
    ///
    /// If `block_millis` is `0` it will block indefinitly
    async fn blocking_read_stream(
        &self,
        block_millis: u64,
        key: String,
        lower_bound: Bound<StreamId>,
        upper_bound: Bound<StreamId>,
    ) -> anyhow::Result<Vec<Frame>> {
        eprintln!("Reading Stream from: {lower_bound:?}, to: {upper_bound:?}");
        loop {
            // TODO: Can we avoid cloning the key every time?
            let results = {
                GuardedDb {
                    data: self.data.lock().await,
                }
                .read_stream(&key, lower_bound, upper_bound)
                .await?
            };
            if !results.is_empty() {
                return Ok(results);
            }
            // TODO: Move this logic elsewhere...
            // On notified the waiter itself gets destroyed, so it's ok to create
            // a new one on each loop iteration
            let waiter = {
                let mut locked_cache = self.data.lock().await;
                let entry = {
                    locked_cache
                        .entry(key.clone())
                        .or_insert(Entry::Stream(NotifyStream::new()))
                };

                let Entry::Stream(stream_map) = entry else {
                    anyhow::bail!("Entry is not a stream");
                };

                stream_map.add_waiter()
            };

            if block_millis == 0 {
                eprintln!("waiting until notified");
                waiter.await?;
                eprintln!("notified!")
            } else {
                match tokio::time::timeout(Duration::from_millis(block_millis), waiter).await {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("Timed out: {e:?}");
                        // only send null array when using block?
                        return Ok(vec![]);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct GuardedDb<'a> {
    data: MutexGuard<'a, HashMap<String, Entry>>,
}

impl<'a> GuardedDb<'a> {
    pub async fn set(&mut self, key: String, value: String, expire_at: Option<Instant>) {
        let expirable_data = ExpireableData {
            value: value.into_bytes(),
            expire_at,
        };
        self.data.insert(key, Entry::Data(expirable_data));
    }

    /// Get the value of key. If the key does not exist the special value nil is returned.
    /// GET only handles string values.
    pub async fn get(&mut self, key: String) -> Option<Vec<u8>> {
        let Some(entry) = self.data.get(&key) else {
            return None;
        };
        let Entry::Data(expirable_data) = &entry else {
            panic!("Entry value is not a numeric value")
        };
        // return value if not expired
        if expirable_data.expired() {
            eprintln!("Key is expired");
            self.data.remove(&key);
            None
        } else {
            Some(expirable_data.value.clone())
        }
    }

    /// Increments counter stored in key.
    ///
    /// * If the `Entry` does not exist, the `Entry` is created, set to `0` and then incremented before returning.
    /// * If the `Entry` does not contain a numeric value it returns an error
    pub async fn increment(&mut self, key: String) -> anyhow::Result<u64> {
        let entry = self.data.entry(key).or_insert(Entry::Data(ExpireableData {
            value: "0".as_bytes().to_vec(),
            expire_at: None,
        }));
        let Entry::Data(expirable_data) = entry else {
            bail!("ERR value is not an integer or out of range")
        };

        if expirable_data.expired() {
            expirable_data.expire_at = None;
            expirable_data.value = "0".as_bytes().to_vec();
        }

        let str_val = str::from_utf8(&expirable_data.value)?;
        let val: u64 = str_val
            .parse()
            .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?;

        if val == i64::MAX as u64 {
            bail!("ERR value is not an integer or out of range")
        }

        expirable_data.value = format!("{}", val + 1).into_bytes();

        Ok(val + 1)
    }

    /// Appends a value to a list.
    /// * If the Entry does not exist, an empty list is created before appending elements to it.
    /// * If the entry is not a list, an error is returned
    pub async fn list_append(&mut self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let entry = self
            .data
            .entry(key)
            .or_insert(Entry::List(NotifiedList::new()));

        let Entry::List(notify_list) = entry else {
            anyhow::bail!("Err value is not a list");
        };

        for val in values {
            notify_list.push_back(val.into_bytes());
        }
        Ok(notify_list.len())
    }

    /// Prepends a value to a list.
    /// * If the Entry does not exist, an empty list is created before appending elements to it.
    /// * If the entry is not a list, an error is returned
    pub async fn list_prepend(
        &mut self,
        key: String,
        values: Vec<String>,
    ) -> anyhow::Result<usize> {
        let entry = self
            .data
            .entry(key)
            .or_insert(Entry::List(NotifiedList::new()));

        let Entry::List(notify_list) = entry else {
            anyhow::bail!("Err value is not a list");
        };

        for val in values {
            notify_list.push_front(val.into_bytes());
        }
        Ok(notify_list.len())
    }

    /// Returns elements in list range
    ///
    /// TODO: specify possible `start` and `end` values
    pub async fn list_range(
        &self,
        key: String,
        start: i64,
        end: i64,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        match self.data.get(&key) {
            None => Ok(vec![]),
            Some(Entry::List(list)) => Ok(list.range(start, end)),
            Some(_) => anyhow::bail!("Err value is not a list"),
        }
    }

    /// Returns lenght of list.
    ///
    /// If the list doesn't exist it returns 0 (list is not created)
    pub async fn list_len(&self, key: String) -> anyhow::Result<usize> {
        self.data
            .get(&key)
            .map(|entry| match entry {
                Entry::List(list) => Ok(list.len()),
                _ => anyhow::bail!("Err value is not a list"),
            })
            .unwrap_or(Ok(0))
    }

    /// Pops `num_elements` from list or all elements in list, whichever is smaller.
    ///
    /// If entry is not a list it returns an error
    pub async fn list_pop(
        &mut self,
        key: String,
        num_elems: usize,
    ) -> anyhow::Result<Vec<EntryData>> {
        match self.data.get_mut(&key) {
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
            Some(_) => anyhow::bail!("Err value is not a list"),
        }
    }

    /// Returns entry type
    pub async fn entry_type(&mut self, key: String) -> &'static str {
        let Some(entry) = self.data.get(&key) else {
            return "none";
        };

        match entry {
            Entry::Data(d) => {
                if d.expired() {
                    dbg!("Key {key} is expired");
                    self.data.remove(&key);
                    "none"
                } else {
                    "string"
                }
            }
            Entry::List(_) => "list",
            Entry::Stream(_) => "stream",
        }
    }

    /// Adds elements to a stream
    /// Field values should form key-value pairs
    pub async fn stream_add(
        &mut self,
        stream_key: String,
        stream_entry_id: String,
        field_values: &[String],
    ) -> anyhow::Result<String> {
        if field_values.len() % 2 != 0 {
            anyhow::bail!("Expected even number of field-value args");
        }

        let stream_entry = self
            .data
            .entry(stream_key)
            .or_insert(Entry::Stream(NotifyStream::new()));
        let Entry::Stream(notify_stream) = stream_entry else {
            anyhow::bail!("Err value is not a stream");
        };

        notify_stream.add(stream_entry_id, field_values)
    }

    async fn read_stream(
        &self,
        key: &str,
        lower_bound: Bound<StreamId>,
        upper_bound: Bound<StreamId>,
    ) -> anyhow::Result<Vec<Frame>> {
        eprintln!("Reading Stream from: {lower_bound:?}, to: {upper_bound:?}");

        let Some(entry) = self.data.get(key) else {
            return Ok(vec![]);
        };

        let Entry::Stream(stream_map) = entry else {
            anyhow::bail!("Err value is not a stream");
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
                    Frame::bulk_from_string(stream_id.to_string()),
                    Frame::Array(values),
                ]
                .to_vec(),
            ));
        }
        return Ok(output);
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
    pub async fn stream_range(
        &self,
        key: String,
        lower_bound: String,
        upper_bound: String,
    ) -> anyhow::Result<Frame> {
        let lower_bound: StreamId = {
            if lower_bound == "-" {
                "0-1".try_into()
            } else if lower_bound.contains("-") {
                lower_bound.as_str().try_into()
            } else {
                format!("{lower_bound}-0").as_str().try_into()
            }
        }
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        let upper_bound: StreamId = {
            if upper_bound == "+" {
                format!("{}-{}", u128::MAX, usize::MAX).as_str().try_into()
            } else if upper_bound.contains("-") {
                upper_bound.as_str().try_into()
            } else {
                format!("{upper_bound}-{}", usize::MAX).as_str().try_into()
            }
        }
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        self.read_stream(
            &key,
            Bound::Included(lower_bound),
            Bound::Included(upper_bound),
        )
        .await
        .map(|output| Frame::Array(output))
    }

    async fn get_last_stream_entry(&self, key: &str) -> anyhow::Result<StreamId> {
        let Some(stream_entry) = self.data.get(key) else {
            return Ok(StreamId::zero());
        };

        let Entry::Stream(notify_stream) = stream_entry else {
            anyhow::bail!("Err value is not a stream");
        };

        let stream_id = notify_stream
            .get_last_stream_entry_id()
            .unwrap_or(StreamId::zero());
        Ok(stream_id)
    }

    async fn parse_xread_lower_bound(
        &self,
        key: &str,
        lower_bound: &str,
    ) -> anyhow::Result<StreamId> {
        let lower_bound: StreamId = if lower_bound == "$" {
            self.get_last_stream_entry(&key).await?
        } else if lower_bound == "-" {
            "0-1".try_into().unwrap()
        } else if lower_bound.contains("-") {
            lower_bound
                .try_into()
                .map_err(|e| anyhow::anyhow!("failed parsing lower_boud with {e}"))?
        } else {
            format!("{lower_bound}-0")
                .as_str()
                .try_into()
                .map_err(|e| anyhow::anyhow!("failed parsing lower_boud with {e}"))?
        };
        Ok(lower_bound)
    }

    /// Non Blocking read of a stream, reading from the range `(lower_bound, end]`
    ///
    /// Parses `lower_bound` in the following manner:
    ///
    /// * `$` Read starting from the last entry, empty in non-blocking mode
    /// * `%` Read all items in stream including all sequence numbers, equivalen to `"0-1"`
    /// * `<time>` Read all items after a give time stamp, returns all sequence numbers
    /// * `<time>-<sequence_number>`
    pub async fn stream_read(&self, key: String, lower_bound: String) -> anyhow::Result<Frame> {
        let lower_bound: StreamId = self.parse_xread_lower_bound(&key, &lower_bound).await?;

        let stream_data = self
            .read_stream(&key, Bound::Excluded(lower_bound), Bound::Unbounded)
            .await?;

        if stream_data.is_empty() {
            Ok(Frame::NullArray)
        } else {
            let stream_data =
                Frame::Array([Frame::bulk_from_string(key), Frame::Array(stream_data)].to_vec());
            Ok(stream_data)
        }
    }
}
