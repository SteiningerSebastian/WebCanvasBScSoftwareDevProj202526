use std::collections::HashMap;
use std::fmt::Display;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use serde_json;

#[derive(Debug)]
pub enum Error{
    MajorityUnreachable(Option<io::Error>),
    CommitFailed(io::Error),
    IoError(io::Error),
    Serialization(serde_json::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MajorityUnreachable(Some(e)) => write!(f, "Majority unreachable: {}", e),
            Error::MajorityUnreachable(None) => write!(f, "Majority unreachable: no replicas found"),
            Error::CommitFailed(e) => write!(f, "Commit failed: {}", e),
            Error::IoError(e) => write!(f, "IO error: {}", e),
            Error::Serialization(e) => write!(f, "Serialization error: {}", e),
        }
    }
}

pub trait KeyValueStore {
    /// Get the value for a given key.
    /// 
    /// # Arguments
    /// * 'key' - A string slice pointer that holds the key.
    ///
    /// # Return
    /// Returns an Option of string slice pointer to the value.
    /// 
    /// # Examples
    /// ``` ignore
    /// key_value_store = FileKeyValueStore::new("storage.txt")?;
    /// let my_value = key_value_store.get_value("my_string")
    /// ```
    fn get(&self, key: &str) -> Option<String>;

    /// Set the value for a given key.
    /// 
    /// # Arguments
    /// * 'key' - A string slice that holds the key.
    /// * 'value' - A string slice that holds the value.
    /// 
    /// # Examples
    /// ``` ignore
    /// key_value_store = FileKeyValueStore::new("storage.txt")?;
    /// key_value_store.set("MyKey", "MyValue")
    /// ```
    fn set(&mut self, key: &str, value: &str);
}

pub struct FileKeyValueStore {
    current: HashMap<String, String>,
    committed: Arc<ArcSwap<HashMap<String, String>>>,
    file_path: PathBuf
}

impl FileKeyValueStore {
    fn get_file_paths(path: &PathBuf) -> [PathBuf; 3] {
        [
            path.with_extension("1.kv"),
            path.with_extension("2.kv"),
            path.with_extension("3.kv"),
        ]
    }

    /// Creates a new FileKeyValueStore with the given path.
    /// If a file exists at the path, it will be loaded and parsed.
    /// The content of the file is expected to be in JSON format representing a HashMap<String, String>.
    /// 
    /// # Arguments
    /// * `path` - Any type that can be converted into a PathBuf (e.g., String, &str, Path)
    /// 
    /// # Returns
    /// * `io::Result<FileKeyValueStore>` - A new store instance or an IO error
    /// 
    /// # Examples
    /// ``` ignore
    /// let store = FileKeyValueStore::new("storage.txt")?;
    /// let store = FileKeyValueStore::new(String::from("/path/to/store.txt"))?;
    /// ```
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, Error> {
        let file_path = path.into();
        let mut current = HashMap::new();

        // Check if any replica files exist at all
        let replica_paths = Self::get_file_paths(&file_path);
        let mut any_exist = false;
        for path in &replica_paths {
            if path.exists() {
                any_exist = true;
                break;
            }
        }

        let committed_map = current.clone();
        let committed = Arc::new(ArcSwap::from_pointee(committed_map));

        // If no files exist, start with empty store
        if !any_exist {
            // No files exist, start fresh
            return Ok(Self {
                committed,
                current,
                file_path,
            });
        }

        // Try to read the majority file content
        let bytes = Self::majority_read(&file_path)?;
        let map: HashMap<String, String> = serde_json::from_slice(&bytes)
            .map_err(Error::Serialization)?;
        current = map;

        let mut this = Self {
            committed,
            current,
            file_path
        };

        // Ensure that the data in the files is consistent with what we loaded -> self-heal
        this.commit()?;

        Ok(this)
    }

    /// Get the staged (uncommitted) value for a given key.
    /// 
    /// # Arguments
    /// * 'key' - A string slice pointer that holds the key.
    /// 
    /// # Return
    /// Returns an Option of string slice pointer to the staged value.
    /// 
    /// # Examples
    /// ``` ignore
    /// key_value_store = FileKeyValueStore::new("storage.txt")?;
    /// let my_staged_value = key_value_store.get_staged("my_string")
    /// ```
    pub fn get_staged(&self, key: &str) -> Option<String> {
        self.current.get(key).cloned()
    }

    pub fn get_committed_arc(&self) -> Arc<ArcSwap<HashMap<String, String>>> {
        self.committed.clone()
    }

    /// Read replicas and return the majority bytes (or error if no majority and 3 disagree).
    fn majority_read(file_path: &PathBuf) -> Result<Vec<u8>, Error> {
        // Read the three replica paths and collect successful reads as Vec<Vec<u8>>
        let mut contents: Vec<Vec<u8>> = Vec::new();
        for path in FileKeyValueStore::get_file_paths(file_path) {
            if let Ok(bytes) = fs::read(&path) {
                contents.push(bytes);
            }
        }

        // No files found -> nothing to load
        if contents.is_empty() {
            return Err(Error::MajorityUnreachable(None));
        }

        // If only one replica exists, majority is impossible
        if contents.len() == 1 {
            return Err(Error::MajorityUnreachable(None));
        }

        // Count identical byte sequences
        let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
        for b in &contents {
            *counts.entry(b.clone()).or_insert(0) += 1;
        }

        // If a majority (count >= 2) exists, return it
        if let Some((bytes, count)) = counts.iter().max_by_key(|(_, c)| *c) {
            if *count >= 2 {
                return Ok(bytes.clone());
            }
        }

        Err(Error::MajorityUnreachable(None))
    }


    fn write_to_disk(&self) -> Result<(), Error> {
        // Ensure parent directory exists (if any)
        if let Some(parent) = self.file_path.parent() {
            // create_dir_all can fail for reasons we want to propagate,
            // but if parent is None we skip.
            fs::create_dir_all(parent).map_err(|e| Error::CommitFailed(e))?;
        }

        // Serialize the whole map at once
        let bytes = serde_json::to_vec(&self.current)
            .map_err(Error::Serialization)?;


        // Write the result to all files and atomic rename them.
        for path in FileKeyValueStore::get_file_paths(&self.file_path){
            // Create a temporary file in the same directory
            let tmp_path = path.with_extension("tmp");

            // Write all data to temporary file
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(|e| Error::CommitFailed(e))?;

            file.write_all(&bytes).map_err(|e| Error::CommitFailed(e))?;
            // Ensure data reaches the temporary file
            file.flush().map_err(|e| Error::CommitFailed(e))?;
            // best effort: try to sync the underlying file; propagate error if it fails
            file.sync_all().map_err(|e| Error::CommitFailed(e))?;

            // Best-effort directory sync before rename:
            // On some platforms (notably Windows) opening a directory as a File may fail.
            // We do not want such platform-specific inability to sync the directory
            // to cause the whole operation to fail, so treat this as best-effort.
            if let Some(parent) = tmp_path.parent() {
                if let Ok(dir_file) = File::open(parent) {
                    // ignore errors from sync_all here
                    let _ = dir_file.sync_all();
                }
            }

            // On Windows remove existing target to avoid rename-fail
            if path.exists() {
                fs::remove_file(&path).map_err(|e| Error::CommitFailed(e))?;
            }

            // Atomically replace target with temporary file
            fs::rename(&tmp_path, &path).map_err(|e| Error::CommitFailed(e))?;

            // Best-effort directory sync after rename (same rationale as above)
            if let Some(parent) = self.file_path.parent() {
                if let Ok(dir_file) = File::open(parent) {
                    let _ = dir_file.sync_all();
                }
            }
        }
        Ok(())
    }

    /// Persists the current state of the key-value store to disk atomically.
    /// This writes all data to a temporary file first, then atomically replaces the target file.
    /// Parent directories are synced to ensure durability.
    /// Either all changes are committed or none - partial writes are impossible.
    /// The hash map is converted to json and stored in the file.
    /// 
    /// # Returns
    /// * `io::Result<()>` - Success or an IO error if writing fails
    /// 
    /// # Examples
    /// ``` ignore
    /// let mut store = FileKeyValueStore::new("storage.txt")?;
    /// store.set("key", "value");
    /// store.commit()?;  // Atomically writes changes to disk
    /// ```
    pub fn commit(&mut self) -> Result<(), Error> {
        // Write to disk atomically
        self.write_to_disk()?;

        // Update committed state atomically
        let new_committed = self.current.clone();
        self.committed.store(Arc::new(new_committed));

        Ok(())
    }

    /// Rolls back any uncommitted changes, reverting the current state
    /// of the key-value store to the last committed state.
    /// 
    /// # Examples
    /// ``` ignore
    /// let mut store = FileKeyValueStore::new("storage.txt")?;
    /// store.set("key", "new_value");
    /// store.rollback();  // Reverts to last committed state
    /// ```
    pub fn rollback(&mut self) {
        self.current = self.committed.load().as_ref().clone();
    }
}

impl KeyValueStore for FileKeyValueStore {
    fn get(&self, key: &str) -> Option<String> {
        let arc_map = self.committed.load();
        arc_map.get(key).map(|s| s.to_owned())
    }

    fn set(&mut self, key: &str, value: &str) {
        self.current.insert(key.to_owned(), value.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    
    #[test]
    fn test_key_value_store() -> Result<(), Error> {
        let temp_path = "./tmp1/test_store.txt";
        let mut store = FileKeyValueStore::new(temp_path)?;
        
        store.set("test_key", "test_value");        
        store.commit()?;
        
        assert_eq!(store.get("test_key").unwrap(), "test_value");

        // createing a new store
        let store = FileKeyValueStore::new(temp_path)?;

        // checking if it persisted correctly
        assert_eq!(store.get("test_key").unwrap(), "test_value");

        // Clean up
        fs::remove_dir_all("./tmp1").map_err(|e| Error::IoError(e))?;
        Ok(())
    }

    #[test]
    fn test_linearizable_reads() -> Result<(), Error> {
        let temp_path = "./tmp2/test_store_linear.txt";
        let mut store = FileKeyValueStore::new(temp_path)?;
        
        // Write initial value
        store.set("key1", "value1");
        store.set("key2", "value2");
        
        // Before commit, reads should see old state (empty)
        assert_eq!(store.get("key1"), None);
        assert_eq!(store.get("key2"), None);
        
        // After commit, reads should see new state
        store.commit()?;
        assert_eq!(store.get("key1").unwrap(), "value1");
        assert_eq!(store.get("key2").unwrap(), "value2");

        store.set("key1", "value3");
        store.set("key2", "value4");
            
        // Before commit, reads should see old state (empty)
        assert_eq!(store.get("key1").unwrap(), "value1");
        assert_eq!(store.get("key2").unwrap(), "value2");

        store.commit()?;
        assert_eq!(store.get("key1").unwrap(), "value3");
        assert_eq!(store.get("key2").unwrap(), "value4");
        
        // Clean up
        fs::remove_dir_all("./tmp2").map_err(|e| Error::IoError(e))?;
        Ok(())
    }

    fn write_json_replica(path: &std::path::Path, entries: &[(&str, &str)]) {
        let mut map = std::collections::HashMap::new();
        for (k, v) in entries {
            map.insert(k.to_string(), v.to_string());
        }
        let bytes = serde_json::to_vec(&map).unwrap();
        std::fs::write(path, &bytes).unwrap();
    }

    #[test]
    fn test_recovery_from_majority_json() -> Result<(), Error> {
        let temp_dir = "./tmp3_json";
        let temp_path = format!("{}/test_store_recovery.json", temp_dir);
        let file_path = PathBuf::from(&temp_path);
        let replica_paths = FileKeyValueStore::get_file_paths(&file_path);

        // Create directory
        fs::create_dir_all(temp_dir).unwrap();

        // Write JSON data to each replica: two replicas agree on majority
        write_json_replica(&replica_paths[0], &[("key", "majority")]);
        write_json_replica(&replica_paths[1], &[("key", "majority")]);
        write_json_replica(&replica_paths[2], &[("key", "minority")]);

        // Loading should recover the majority value
        let store = FileKeyValueStore::new(&temp_path)?;
        assert_eq!(store.get("key"), Some("majority".to_string()));

        // Clean up
        fs::remove_dir_all(temp_dir).map_err(|e| Error::IoError(e))?;
        Ok(())
    }

    #[test]
    fn test_recovery_no_majority_json() -> Result<(), Error> {
        let temp_dir = "./tmp4_json";
        let temp_path = format!("{}/test_store_no_majority.json", temp_dir);
        let file_path = PathBuf::from(&temp_path);
        let replica_paths = FileKeyValueStore::get_file_paths(&file_path);

        // Create directory
        fs::create_dir_all(temp_dir).unwrap();

        // Write three different JSON contents (no majority)
        write_json_replica(&replica_paths[0], &[("key", "one")]);
        write_json_replica(&replica_paths[1], &[("key", "two")]);
        write_json_replica(&replica_paths[2], &[("key", "three")]);

        // Loading should fail with MajorityUnreachable
        let result = FileKeyValueStore::new(&temp_path);
        match result {
            Err(Error::MajorityUnreachable(_)) => {}
            _ => panic!("Expected MajorityUnreachable error"),
        }

        // Clean up
        fs::remove_dir_all(temp_dir).unwrap();
        Ok(())
    }
    

    #[test]
    fn test_commit_and_reload_multiple_keys() -> Result<(), Error> {
        let temp_path = "./tmp5/test_store_multi.txt";
        let mut store = FileKeyValueStore::new(temp_path)?;

        store.set("a", "1");
        store.set("b", "2");
        store.set("c", "3");
        store.commit()?;

        let store = FileKeyValueStore::new(temp_path)?;
        assert_eq!(store.get("a"), Some("1".to_owned()));
        assert_eq!(store.get("b"), Some("2".to_owned()));
        assert_eq!(store.get("c"), Some("3".to_owned()));

        fs::remove_dir_all("./tmp5").map_err(|e| Error::IoError(e))?;
        Ok(())
    }

    #[test]
    fn test_overwrite_and_commit() -> Result<(), Error> {
        let temp_path = "./tmp6/test_store_overwrite.txt";
        let mut store = FileKeyValueStore::new(temp_path)?;

        store.set("x", "old");
        store.commit()?;
        assert_eq!(store.get("x"), Some("old".to_owned()));

        store.set("x", "new");
        assert_eq!(store.get("x"), Some("old".to_owned())); // Not committed yet

        store.commit()?;
        assert_eq!(store.get("x"), Some("new".to_owned()));

        fs::remove_dir_all("./tmp6").map_err(|e| Error::IoError(e))?;
        Ok(())
    }
}