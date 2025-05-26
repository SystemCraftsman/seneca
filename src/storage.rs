use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const MAX_SEGMENT_SIZE: usize = 1024 * 1024 * 10; // 10MB segment size

#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    current_segment: Arc<Mutex<Segment>>,
    segment_index: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub struct Segment {
    path: PathBuf,
    file: File,
    size: usize,
}

impl Log {
    /// Open or create a new log in the given directory
    pub fn open<P: AsRef<Path>>(dir: P) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;

        let segment_index = 0;
        let segment = Segment::new(&dir, segment_index)?;

        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            current_segment: Arc::new(Mutex::new(segment)),
            segment_index: Arc::new(AtomicUsize::new(segment_index)),
        })
    }

    /// Append a message to the log
    pub fn append(&self, message: &[u8]) -> std::io::Result<u64> {
        let mut segment = self.current_segment.lock().unwrap();

        if segment.size + message.len() > MAX_SEGMENT_SIZE {
            let next_index = self.segment_index.fetch_add(1, Ordering::SeqCst) + 1;
            let new_segment = Segment::new(&self.dir, next_index)?;
            *segment = new_segment;
        }

        segment.append(message)
    }

    /// Read all messages from the current segment
    pub fn read_all(&self) -> std::io::Result<Vec<Vec<u8>>> {
        let segment = self.current_segment.lock().unwrap();
        segment.read_all()
    }
}

impl Segment {
    fn new<P: AsRef<Path>>(dir: P, index: usize) -> std::io::Result<Self> {
        let path = dir.as_ref().join(format!("segment-{:04}.log", index));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let size = file.metadata()?.len() as usize;

        Ok(Self { path, file, size })
    }

    fn append(&mut self, message: &[u8]) -> std::io::Result<u64> {
        let offset = self.size as u64;

        // Write a u32 length header
        let len = message.len() as u32;
        self.file.write_all(&len.to_be_bytes())?;
        self.file.write_all(message)?;
        self.file.flush()?;

        self.size += 4 + message.len();
        Ok(offset)
    }

    fn read_all(&self) -> std::io::Result<Vec<Vec<u8>>> {
        let mut messages = Vec::new();
        let mut file = File::open(&self.path)?;
        let mut pos = 0;

        while let Ok(_) = file.seek(SeekFrom::Start(pos)) {
            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }

            let len = u32::from_be_bytes(len_buf) as usize;
            let mut msg_buf = vec![0u8; len];

            if file.read_exact(&mut msg_buf).is_err() {
                break;
            }

            messages.push(msg_buf);
            pos += 4 + len as u64;
        }

        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn setup_temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("seneca-log-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_log_append_and_read() {
        let dir = setup_temp_dir();
        let log = Log::open(&dir).expect("Failed to open log");

        log.append(b"message-1").expect("Failed to append");
        log.append(b"message-2").expect("Failed to append");

        let messages = log.read_all().expect("Failed to read messages");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], b"message-1");
        assert_eq!(messages[1], b"message-2");

        // Clean up temp directory
        fs::remove_dir_all(&dir).unwrap();
    }
}
