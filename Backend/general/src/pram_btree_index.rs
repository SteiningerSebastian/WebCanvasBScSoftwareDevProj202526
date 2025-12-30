use std::{fmt::Display, sync::{Arc}, u64};

use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};

use crate::persistent_random_access_memory::{self, Error as PRAMError, PersistentRandomAccessMemory, Pointer};

#[derive(Debug)]
pub enum Error {
    KeyNotFound,
    StorageError,
    InvalidOperation,
    UnspecifiedMemoryError(PRAMError),
    PersistenceError(PRAMError),
    SynchronizationError,
    RecursiveChangedNeeded,
    KeyAlreadyExists,

    /// A fatal error that cannot be recovered from.
    /// The caller must abort the process when encountering this error to prevent data corruption.
    /// After a FatalError, all guarantees about the state of the persistent memory are void.
    FatalError(Box<Error>),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::KeyNotFound => write!(f, "Key not found"),
            Error::StorageError => write!(f, "Storage error"),
            Error::InvalidOperation => write!(f, "Invalid operation"),
            Error::UnspecifiedMemoryError(e) => write!(f, "Unspecified memory error: {}", e),
            Error::PersistenceError(e) => write!(f, "Persistence error: {}", e),
            Error::SynchronizationError => write!(f, "Synchronization error"),
            Error::RecursiveChangedNeeded => write!(f, "Recursive change needed but prohibeded."),
            Error::KeyAlreadyExists => write!(f, "Key already exists"),
            Error::FatalError(e) => write!(f, "Fatal Error: {}", e),
        }
    }
}

pub trait BTreeIndex {
    /// Sets the value for the given key.
    /// 
    /// Parameters:
    /// - `key`: The key to set.
    /// - `value`: The value to associate with the key.
    /// 
    /// Returns:
    /// - `Ok(Some(old_value))` if the key already existed, where `old_value` is the previous value associated with the key.
    /// - `Err(Error)` if there was an error during the operation.
    fn set(&self, key: u64, value: u64) -> Result<Option<u64>, Error>;

    /// Gets the value associated with the given key.
    /// 
    /// Parameters:
    /// - `key`: The key to retrieve.
    /// 
    /// Returns:
    /// - `Ok(value)` if the key exists, where `value` is the associated value.
    fn get(&self, key: u64) -> Result<u64, Error>;

    /// Gets the value associated with the given key, or sets it to `new_value` if it does not exist.
    /// 
    /// Parameters:
    /// - `key`: The key to retrieve or set.
    /// - `new_value`: The value to set if the key does not exist.
    /// 
    /// Returns:
    /// - `Ok(Some(value))` if the key existed, where `value` is the associated value.
    /// - `Ok(None)` if the key did not exist and was set to `new_value`.
    fn get_or_set(&self, key: u64, new_value: u64) -> Result<u64, Error>;

    /// Checks if the given key exists in the B-tree index.
    /// 
    /// Parameters:
    /// - `key`: The key to check.
    /// 
    /// Returns:
    /// - `Ok(true)` if the key exists, `Ok(false)` otherwise.
    /// 
    /// Runtime Complexity: O(log n) 
    fn contains(&self, key: u64) -> Result<bool, Error>;

    /// Removes the value associated with the given key.
    ///
    /// Parameters:
    /// - `key`: The key to remove.
    /// 
    /// Returns:
    /// - `Ok(())` if the operation was successful.
    fn remove(&self, key: u64) -> Result<(), Error>;

    /// Persists the B-tree index to the underlying storage.
    /// 
    /// Returns:
    /// - `Ok(())` if the operation was successful.
    /// - `Err(Error)` if there was an error during the operation.
    fn persist(&self) -> Result<(), Error>;
}

// The order of the B-tree (maximum number of children per node) Should be odd for simplicity.
const BTREE_ORDER: usize = 255; // 255 - Assuming 4kb pages and u64 (8 bytes) for keys and values (16 bytes total per entry) + some overhead for length, is_leaf and padding
const ROOT_NODE_ADDRESS: u64 = 0; // Address of the root node in PRAM

#[repr(C)] // Keep the order of values in memory
struct BTreeNode {
    // 1. Largest alignment: u64 (8 bytes)
    keys: [u64; BTREE_ORDER - 1], 
    values: [u64; BTREE_ORDER], 
    
    // 2. Next largest alignment: usize (8 bytes on 64-bit)
    length: usize, 

    // 3. Smallest alignment: bool (1 byte)
    is_leaf: bool, 
}

// A struct containing an additional chlidren thats loaded in memory from the pram for debugging etc.
#[cfg(debug_assertions)]
#[allow(dead_code)]
struct BTreeMemCopyNode {
    // 1. Largest alignment: u64 (8 bytes)
    keys: [u64; BTREE_ORDER - 1], 
    values: [u64; BTREE_ORDER], 
    
    // 2. Next largest alignment: usize (8 bytes on 64-bit)
    length: usize, 

    // 3. Smallest alignment: bool (1 byte)
    is_leaf: bool,

    _children: Vec<BTreeMemCopyNode>,
}

impl Clone for BTreeNode {
    fn clone(&self) -> Self {
        BTreeNode {
            is_leaf: self.is_leaf,
            keys: self.keys,
            values: self.values,
            length: self.length,
        }
    }
}
impl Copy for BTreeNode {}

pub struct ConcurrentIterator<'a> {
    _guard: std::sync::MutexGuard<'a, BTreeIndexPRAM>,
    iter: Box<dyn Iterator<Item = (u64, u64)> + 'a>,
}

impl<'a> Iterator for ConcurrentIterator<'a>
{
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}


pub struct BTreeIndexPRAM {
    pram: Arc<PersistentRandomAccessMemory>,
    root: Pointer<BTreeNode>,
    lock: Arc<RwLock<()>>, // For concurrent access
}

impl Clone for BTreeIndexPRAM {
    fn clone(&self) -> Self {
        BTreeIndexPRAM {
            pram: Arc::clone(&self.pram),
            root: self.root.clone(),
            lock: Arc::clone(&self.lock),
        }
    }
}

struct BTreeNodeRemovePointerCache{
    pointer: Pointer<BTreeNode>,
    node: BTreeNode,
}

enum InsertMode {
    Update,
    GetOrSet,
}

impl BTreeIndexPRAM {
    pub fn new(pram: Arc<PersistentRandomAccessMemory>) -> Self {
        // Allocate root node in PRAM
        let root = pram.smalloc(ROOT_NODE_ADDRESS, std::mem::size_of::<BTreeNode>()).unwrap();

        let node: BTreeNode = root.deref().unwrap();

        // Write the default root to it if its the frist time we start.
        if node.length == 0 {
            let node = BTreeNode {
                is_leaf: true,
                keys: [0; BTREE_ORDER - 1],
                values: [0; BTREE_ORDER],
                length: 0,
            };

            root.set(&node).unwrap();
        }
        
        BTreeIndexPRAM { 
            pram,
            root,
            lock: Arc::new(RwLock::new(())),
        }
    }

    /// Finds the leaf node that should contain the given key.
    ///
    /// Parameters:
    /// - `key`: The key to search for.
    /// - `node_ptr`: The pointer to the current node being searched.
    /// 
    /// Returns:
    /// - `Ok(Pointer)` pointing to the leaf node.
    /// - `Err(Error)` if there was an error during the operation.
    fn find_leaf_node(&self, key: u64, node_ptr: &mut Pointer<BTreeNode>) -> Result<Pointer<BTreeNode>, Error> {
        let node: BTreeNode = node_ptr.deref().map_err(Error::UnspecifiedMemoryError)?; // For debugging, just deref without caching.

        // Can not find key in empty list.
        if node.length == 0 {
            return Err(Error::KeyNotFound);
        }

        if node.is_leaf {
            Ok((*node_ptr).clone()) // return the leaf node pointer
        }else {
            // find the child pointer to follow
            // Last element is used to point to any elements greater than the last key.
            let index = node.keys.as_slice()[0..node.length-1].iter().position(|e| *e >= key).unwrap_or(BTREE_ORDER-1);        

            let mut child_ptr = Pointer::from_address(node.values[index],self.pram.clone());
            self.find_leaf_node(key, &mut child_ptr)
        }
    }

    /// Upgrades a read lock to a write lock in the given LockGuard.
    /// 
    /// Parameters:
    /// - `lock_guard`: The LockGuard containing the locks to upgrade.
    fn upgrade_lock(lock_guard: &mut LockGuard) {
        if lock_guard.write_lock.is_none() {
            if let Some(read_lock) = lock_guard.read_lock.take() {
                let write_lock = RwLockUpgradableReadGuard::upgrade(read_lock);
                lock_guard.write_lock = Some(write_lock);
                lock_guard.read_lock = None;
            }
        }
    }

    /// Downgrades a write lock to a read lock in the given LockGuard.
    /// 
    /// Parameters:
    /// - `lock_guard`: The LockGuard containing the locks to downgrade.
    fn downgrade_lock(lock_guard: &mut LockGuard) {
        if lock_guard.read_lock.is_none() {
            if let Some(write_lock) = lock_guard.write_lock.take() {
                let read_lock = RwLockWriteGuard::downgrade_to_upgradable(write_lock);
                lock_guard.read_lock = Some(read_lock);
                lock_guard.write_lock = None;
            }
        }
    }


    /// Recursive helper function to insert or update a key-value pair.
    ///
    /// Parameters:
    /// - `key`: The key to insert or update.
    /// - `value`: The value to associate with the key.
    /// - `node_ptr`: The pointer to the current node being processed.
    /// 
    /// Returns:
    /// - `Ok((Some(old_value), Some(median_key, pointer)))` if the key already existed, where `old_value` is the previous value associated with the key.
    /// - `Ok(None)` if no split was needed.
    fn insert_or_update_recursive(&self, key: u64, value: u64, node: &mut BTreeNode, parent_key: u64, lock_guard: &mut LockGuard, mode: InsertMode) -> Result<(Option<u64>, Option<(u64, Pointer<BTreeNode>)>), Error> {
        if node.is_leaf {
            // Search for the correct position to insert the new key
            let index = node.keys.as_slice()[0..node.length].iter().position(|e| *e >= key);

            // If key already exists, update the value
            if let Some(index) = index {
                if node.keys[index] == key { // key exist and mode is not insert!
                    // Key exists, return old value
                    let old_value = node.values[index];

                    if let InsertMode::GetOrSet = mode {
                        // Downgrade back to read lock as no further modifications are needed.
                        Self::downgrade_lock(lock_guard);
                        return Ok((Some(old_value), None)); // Return existing value, no split needed
                    }

                    // Update existing value
                    node.values[index] = value;

                    // Downgrade back to read lock as no further modifications are needed.
                    Self::downgrade_lock(lock_guard);
                    return Ok((Some(value), None)); // No split needed
                }
            }

            // This may be none for the root is leaf node as before any splits we expect a compltly blank array, in this case insert in order.
            let index = index.unwrap_or(node.length);

            // Check if there is space in the node
            if node.length < BTREE_ORDER - 1 {
                // Shift keys and values to make space for the new key-value pair
                for i in (index..node.length).rev() {
                    node.keys[i + 1] = node.keys[i];
                    node.values[i + 1] = node.values[i];
                }

                // Insert the new key and value
                node.keys[index] = key;
                node.values[index] = value;
                node.length += 1;

                // Downgrade back to read lock as no further modifications are needed.
                Self::downgrade_lock(lock_guard);

                if let InsertMode::GetOrSet = mode {
                    Ok((Some(value), None)) // Return newly inserted value, no split needed
                }else {
                    Ok((None, None)) // No split needed
                }
            } else { // Need to split the node
                // Find median key
                let median_index = node.length / 2 - 1;
                let median_key = node.keys[median_index];

                // Create new node
                let mut right_node = BTreeNode {
                    is_leaf: true,
                    keys: [0; BTREE_ORDER - 1],
                    values: [0; BTREE_ORDER],
                    length: 0,
                };

                // Move second half of keys and values to new node
                for i in median_index+1..node.length {
                    right_node.keys[i - (median_index + 1)] = node.keys[i];
                    right_node.values[i - (median_index + 1)] = node.values[i];
                }
                right_node.length = node.length - (median_index + 1); // new_node gets the second half, excluding median

                // Update the poitner for transversal of the leafs
                right_node.values[BTREE_ORDER - 1] = node.values[BTREE_ORDER - 1]; // The new node points to what the original node was pointing to

                node.length = median_index + 1; // node = left half including median

                // Now actually add the value to the correct node
                if key <= median_key {
                    for i in (index..node.length).rev() {
                        node.keys[i + 1] = node.keys[i]; // Don't need to shif the pointer that points to the next leaf as it is always at the rightmost position by definition.
                        node.values[i + 1] = node.values[i];
                    }

                    node.keys[index] = key;
                    node.values[index] = value;
                    node.length += 1;
                } else {
                    let index = index - (median_index + 1); // adjust index for the right node

                    for i in (index..right_node.length).rev() {
                        right_node.keys[i + 1] = right_node.keys[i]; // Don't need to shif the pointer that points to the next leaf as it is always at the rightmost position by definition.
                        right_node.values[i + 1] = right_node.values[i];
                    }

                    right_node.keys[index] = key;
                    right_node.values[index] = value;
                    right_node.length += 1;
                }

                // Write back the updated nodes
                let right_node_ptr = self.pram.malloc::<BTreeNode>(std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;
                right_node_ptr.set(&right_node).map_err(Error::UnspecifiedMemoryError)?;

                // Update the pointer of the original node to point to the new node
                node.values[BTREE_ORDER - 1] = right_node_ptr.address;

                // The median key is actually not in the new node, it's the first key of the original node
                if let InsertMode::GetOrSet = mode {
                    return Ok((Some(value), Some((median_key, right_node_ptr)))); // Return median key and new node pointer
                } else {
                    return Ok((None, Some((median_key, right_node_ptr)))); // Return median key and new node pointer
                }
            }
        } else {
            // Search for the correct position to insert the new key
            let opt_index = node.keys.as_slice()[0..node.length-1].iter().position(|e| *e >= key);

            // find the child pointer to follow
            // Last element is used to point to any elements greater than the last key.
            let index: usize = opt_index.unwrap_or(BTREE_ORDER-1); 
            // If none found, its the rightmost element, so it contains all keys up to what this parent node contains.
            let child_key = node.keys.get(index).cloned().unwrap_or(parent_key);

            let child_ptr = Pointer::from_address(node.values[index],self.pram.clone());
            // The caller that dereferences the Node is responsible to write the value back.
            let mut child = child_ptr.deref().map_err(Error::UnspecifiedMemoryError)?;

            // we need to check if we need to upgrade the lock here because if we need to split the child node, we will need to modify this node as well.

            let (value, split_result) = self.insert_or_update_recursive(key, value, &mut child, child_key, lock_guard, mode)?;

            child_ptr.set(&child).map_err(Error::UnspecifiedMemoryError)?;

            // The marker must be inserted at the index position. The recursive call returns the left half of the split leaf so, the new left leaf. So we have to move index... to right
            // and insert the new element with the marker at the index position.
            if let Some((left_child_key, right_child_ptr)) = split_result { 
                let right_child_key = child_key; // The key the child had before being split up.

                // Need to insert median_key and new_child_ptr into this node
                if node.length < BTREE_ORDER { // Here we may actually use the last value as we are an internal node and can have BTREE_ORDER children
                    // If the node was the last element, we need to move it into the open position, the new right node will be new last pointer.
                    if index == BTREE_ORDER - 1 {
                        // move the last pointer to the new position
                        node.values[node.length-1] = node.values[BTREE_ORDER - 1];
                        // it contains all values smaller than the new marker
                        node.keys[node.length-1] = left_child_key;
                        // the new last pointer is the new right child
                        node.values[BTREE_ORDER - 1] = right_child_ptr.address;
                        node.length += 1;
                    }else{
                        // Shift keys and values to make space for the new key-value pair
                        for i in (index+1..node.length-1).rev() {
                            node.keys[i + 1] = node.keys[i];
                            node.values[i + 1] = node.values[i];
                        }

                        // Insert the new key and value
                        node.keys[index] = left_child_key;
                        node.keys[index+1] = right_child_key;
                        node.values[index+1] = right_child_ptr.address;
                        node.length += 1;
                    }

                    // Downgrade back to read lock as no further modifications are needed.
                    Self::downgrade_lock(lock_guard);

                    return Ok((value, None)); // No split needed
                } else {
                    // Need to split this internal node
                    let median_index = node.length / 2 -1;
                    let median_key = node.keys[median_index];

                    let mut right_node = BTreeNode {
                        is_leaf: false, // we are spliting an internal node so the new node is also internal.
                        keys: [0; BTREE_ORDER - 1],
                        values: [0; BTREE_ORDER],
                        length: 0,
                    };

                    // Move second half of keys and values to new node
                    for i in median_index+1..node.length-1 {
                        right_node.keys[i - (median_index + 1)] = node.keys[i];
                        right_node.values[i - (median_index + 1)] = node.values[i];
                    }

                    // Copy the last place in values
                    right_node.values[BTREE_ORDER - 1] = node.values[BTREE_ORDER - 1];

                    right_node.length = node.length - (median_index + 1); // new_node gets the second half including median
                    node.length = median_index + 1; // node = left half 

                    // Update the implicit pointer, its the median as its now the last element in node.
                    node.values[BTREE_ORDER - 1] = node.values[median_index];

                    // Now actually add the median key and new child pointer to the correct node
                    if right_child_key <= median_key {
                        if index == median_index {
                            // move the last pointer to the new position
                            node.values[node.length-1] = node.values[BTREE_ORDER - 1];
                            // it contains all values smaller than the new marker
                            node.keys[node.length-1] = left_child_key;
                            // the new last pointer is the new right child
                            node.values[BTREE_ORDER - 1] = right_child_ptr.address;
                            node.length += 1;
                        }
                        else{
                            // Insert into the original node
                            for i in (index+1..node.length-1).rev() {
                                node.keys[i + 1] = node.keys[i];
                                node.values[i + 1] = node.values[i]; // Don't need to move the last pointer, the inserted marker is always smaller than the last key.
                            }

                            // Insert the new key and value
                            node.keys[index] = left_child_key;
                            node.keys[index+1] = right_child_key;
                            node.values[index+1] = right_child_ptr.address;
                            node.length += 1;
                        }
                    } else {
                        if index == BTREE_ORDER - 1 {
                            // move the last pointer to the new position
                            right_node.values[right_node.length-1] = right_node.values[BTREE_ORDER - 1];

                            // it contains all values smaller than the new marker
                            right_node.keys[right_node.length-1] = left_child_key;
                            
                            // the new last pointer is the new right child
                            right_node.values[BTREE_ORDER - 1] = right_child_ptr.address;
                            right_node.length += 1;
                        } else {
                            let projected_index = index - (median_index + 1);

                            // Insert into the new node
                            for i in (projected_index+1..right_node.length-1).rev() {
                                right_node.keys[i + 1] = right_node.keys[i];
                                right_node.values[i + 1] = right_node.values[i]; // Don't need to move the last pointer, the inserted marker is always smaller than the last key.
                            }

                            right_node.keys[projected_index] = left_child_key;
                            right_node.keys[projected_index+1] = right_child_key;
                            right_node.values[projected_index+1] = right_child_ptr.address;
                            right_node.length += 1;
                        }
                    }

                    // Write back the updated nodes
                    let right_node_ptr = self.pram.malloc::<BTreeNode>(std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;
                    right_node_ptr.set(&right_node).map_err(Error::UnspecifiedMemoryError)?;

                    return Ok((value, Some((median_key, right_node_ptr)))); // Return median key and new node pointer
                }
            }
            Ok((value, None)) // No split needed
        }
    }

    /// Inserts or updates a key-value pair in the B-tree index.
    ///
    /// Parameters:
    /// - `key`: The key to insert or update.
    /// - `value`: The value to associate with the key.
    /// 
    /// Returns:
    /// - `Ok(old_value)` if the key already existed, where `old_value` is the previous value associated with the key.
    /// - `Err(Error)` if there was an error during the operation.
    fn insert_or_update(&self, key: u64, value: u64, lock_guard: &mut LockGuard) -> Result<Option<u64>, Error> {
        let mut root = self.root.deref().map_err(Error::UnspecifiedMemoryError)?;

        // Upgrade to write lock as we may need to insert.
        // The insert could back-propagate through the whole tree causing multiple splits.
        // We need to hold the write lock until we are certain no updates can back-propagate.
        Self::upgrade_lock(lock_guard);

        let (old_value, split) = self.insert_or_update_recursive(key, value, &mut root, u64::MAX, lock_guard, InsertMode::Update)?;

        if let Some((median_key, new_right_ptr)) = split {
            let left_child_ptr = self.pram.malloc::<BTreeNode>(std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;

            // Need to create a new root
            let mut new_root = BTreeNode {
                is_leaf: false,
                keys: [0; BTREE_ORDER - 1],
                values: [0; BTREE_ORDER],
                length: 2,
            };
            new_root.keys[0] = median_key;
            new_root.values[0] = left_child_ptr.address;
            new_root.values[BTREE_ORDER-1] = new_right_ptr.address; // For any elements greater than the available keys, search the last position of the array.

            // Write the root node
            left_child_ptr.set(&root).map_err(Error::UnspecifiedMemoryError)?;

            // Write the new root node to the PRAM
            self.root.set(&new_root).map_err(Error::UnspecifiedMemoryError)?;
        } else {
            // Write back the updated root
            self.root.set(&root).map_err(Error::UnspecifiedMemoryError)?;
        }

        // Downgrade back to read lock as no further modifications are needed. (if not happened in the recursive call)
        Self::downgrade_lock(lock_guard);

        Ok(old_value)
    }

    /// Inserts a key-value pair if the key does not already exist.
    /// 
    /// Parameters:
    /// - `key`: The key to insert.
    /// - `new_value`: The value to associate with the key if it does not exist.
    /// 
    /// Returns:
    /// - `Ok(value)` if the key existed, where `value` is the associated value, or the newly inserted value.
    /// - `Err(Error)` if there was an error during the operation.
    fn insert_or_get(&self, key: u64, new_value: u64, lock_guard: &mut LockGuard) -> Result<u64, Error> {
        let mut root = self.root.deref().map_err(Error::UnspecifiedMemoryError)?;

        // Upgrade to write lock as we may need to insert.
        // The insert could back-propagate through the whole tree causing multiple splits.
        // We need to hold the write lock until we are certain no updates can back-propagate.
        Self::upgrade_lock(lock_guard);

        let (value, split) = self.insert_or_update_recursive(key, new_value, &mut root, u64::MAX, lock_guard, InsertMode::GetOrSet)?;
        // In this insert mode, it's not possible that value is None. Either we got the existing value or we inserted the new value.
        let value = value.unwrap();

        if let Some((median_key, new_right_ptr)) = split {
            let left_child_ptr = self.pram.malloc::<BTreeNode>(std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;

            // Need to create a new root
            let mut new_root = BTreeNode {
                is_leaf: false,
                keys: [0; BTREE_ORDER - 1],
                values: [0; BTREE_ORDER],
                length: 2,
            };
            new_root.keys[0] = median_key;
            new_root.values[0] = left_child_ptr.address;
            new_root.values[BTREE_ORDER-1] = new_right_ptr.address; // For any elements greater than the available keys, search the last position of the array.

            // Write the root node
            left_child_ptr.set(&root).map_err(Error::UnspecifiedMemoryError)?;

            // Write the new root node to the PRAM
            self.root.set(&new_root).map_err(Error::UnspecifiedMemoryError)?;
        } else {
            // Write back the updated root
            self.root.set(&root).map_err(Error::UnspecifiedMemoryError)?;
        }

        Ok(value)
    }

    /// Borrows a key from the left sibling of the given node.
    ///
    /// Parameters:
    /// - 'node': The node that needs to borrow a key.
    /// - `parent_node`: The parent node of the node to borrow from.
    /// - `node_index`: The index of the node in the parent's children array.
    /// 
    /// Returns:
    /// - `Ok(true)` if a key was successfully borrowed.
    /// - `Ok(false)` if borrowing was not possible.
    fn try_borrow_left_sibling(&self, node: &mut BTreeNode, parent_node: &mut BTreeNode, node_index: usize) -> Result<bool, Error> {
        if node_index == 0 || parent_node.length < 2 {
            return Ok(false); // No left sibling, can't borrow from non-existent sibling
        }

        // Because we have defined the last elemtn to point to the leaf contianing all
        // values larger than any key in the current node, we need to calculate the left
        // node for this case individually.
        let left_node_index = if node_index == BTREE_ORDER-1 {
            parent_node.length - 2 
        } else { 
            node_index - 1 
        };

        // Try left sibling
        let left_sibling_ptr = Pointer::<BTreeNode>::from_address(parent_node.values[left_node_index], self.pram.clone());
        let mut left_sibling = left_sibling_ptr.deref().map_err(Error::UnspecifiedMemoryError)?;

        // The +1 is because we need to leave enough keeys in the sibling and with integer division e.g. 5/2 == 2 this could be true for len=3
        // Which would then lead to underflow in the sibling.
        if left_sibling.length >= BTREE_ORDER/2 + 1 {
            // Borrow from left sibling
            // Shift keys and values in current node to right
            for i in (0..node.length).rev() {
                node.keys[i + 1] = node.keys[i];
                node.values[i + 1] = node.values[i];
            }

            node.length += 1;

            if node.is_leaf {
                // Move key from left_sibling to current node
                node.keys[0] = left_sibling.keys[left_sibling.length - 1]; // the most left key from the left sibling
                node.values[0] = left_sibling.values[left_sibling.length - 1]; // and its associated value
            
                // if we are a leaf node, its enough to just decreas the length as this will effectively remove the last key and value.
                left_sibling.length -= 1;

                // Update parent key for left sibling
                parent_node.keys[left_node_index] = left_sibling.keys[left_sibling.length - 1]; // the last key in the new left sibling.
            } else { // But if we are a internal node we have borrowed the last key, the one without a value.
                node.keys[0] = parent_node.keys[left_node_index]; // the separator key from the parent
                node.values[0] = left_sibling.values[BTREE_ORDER - 1]; // the most right pointer from the left sibling

                // Move the last seperator key and value to end of node.
                left_sibling.length -= 1;
                left_sibling.values[BTREE_ORDER - 1] = left_sibling.values[left_sibling.length-1]; // Move the last pointer to the end position.
                // No need to move the key, it becomes stale after removing the length.

                // Update parent key
                parent_node.keys[left_node_index] = left_sibling.keys[left_sibling.length-1]; // the last previous key in the left sibling
            }

            // Write back updated nodes
            left_sibling_ptr.set(&left_sibling).map_err(Error::UnspecifiedMemoryError)?;
            return Ok(true); // Placeholder return value
        }else{
            return Ok(false);
        }
    }

    /// Borrows a key from the right sibling of the given node.
    /// 
    /// Parameters:
    /// - 'node': The node that needs to borrow a key.
    /// - `parent_node`: The parent node of the node to borrow from.
    /// - `node_index`: The index of the node in the parent's children array.
    /// 
    /// Returns:
    /// - `Ok(true)` if a key was successfully borrowed.
    /// - `Ok(false)` if borrowing was not possible.
    fn try_borrow_right_sibling(&self, node: &mut BTreeNode, parent_node: &mut BTreeNode, node_index: usize) -> Result<bool, Error> {
        if node_index + 1 >= BTREE_ORDER || parent_node.length < 2 {
            return Ok(false); // No right sibling
        }

        let right_sibling_index = if node_index + 2 >= parent_node.length {
            BTREE_ORDER - 1
        } else {
            node_index + 1
        };

        // Try right sibling
        let right_sibling_ptr = Pointer::<BTreeNode>::from_address(parent_node.values[right_sibling_index], self.pram.clone());
        let mut right_sibling = right_sibling_ptr.deref().map_err(Error::UnspecifiedMemoryError)?;

        // The +1 is because we need to leave enough keeys in the sibling and with integer division e.g. 5/2 == 2 this could be true for len=3
        // Which would then lead to underflow in the sibling.
        if right_sibling.length >= BTREE_ORDER/2 + 1 {  
            if node.is_leaf {                                  
                // Borrow from right sibling
                // Move key from right_sibling to current node
                let new_key = right_sibling.keys[0];
                node.keys[node.length] = new_key; // the most left key from the right sibling
                node.values[node.length] = right_sibling.values[0]; // and its associated value

                // Update parent key
                parent_node.keys[node_index] = new_key; // the last key in the current node

                node.length += 1;

                // Shift keys and values in right sibling to left
                for i in 0..right_sibling.length-1 {
                    right_sibling.keys[i] = right_sibling.keys[i + 1];
                    right_sibling.values[i] = right_sibling.values[i + 1];
                }

                right_sibling.length -= 1;
            }else {
                // Move the last pointer of the current node to the end of the current list.
                node.values[node.length-1] = node.values[BTREE_ORDER - 1];
                node.keys[node.length-1] = parent_node.keys[node_index]; // the separator key from the parent

                // Now take from the right sibling the first entry and move it to the end - update parent key
                node.values[BTREE_ORDER -1] = right_sibling.values[0]; // the most left pointer from the right sibling
                parent_node.keys[node_index] = right_sibling.keys[0]; // the most left key from the right sibling

                node.length += 1;
                right_sibling.length -= 1;

                // Now move the right sibling entries to the left - except for the last value it can stay there
                for i in 0..right_sibling.length-1 {
                    right_sibling.keys[i] = right_sibling.keys[i + 1];
                    right_sibling.values[i] = right_sibling.values[i + 1];
                }
            }
            
            // Write back updated nodes
            right_sibling_ptr.set(&right_sibling).map_err(Error::UnspecifiedMemoryError)?;

            return Ok(true); // Placeholder return value
        }else{
            return Ok(false);
        }
    }

    /// Merges the given node with its left sibling.
    ///
    /// Parameters:
    /// - 'node': The node to merge.
    /// - `parent_node`: The parent node of the node to merge.
    /// - `node_index`: The index of the node in the parent's children array.
    /// 
    /// Returns:
    /// - `Ok(bool)` if the merge was successful. True if merged, false if not.
    /// - `Err(Error)` if there was an error during the operation.
    fn merge_left_sibling(&self, node: &mut BTreeNodeRemovePointerCache, parent_node: &mut BTreeNode, node_index: usize) -> Result<bool, Error> {
        if node_index == 0 || parent_node.length < 2 {
            return Ok(false); // No left sibling
        }

        let left_sibling_index = if node_index == BTREE_ORDER-1 {
            parent_node.length - 2 
        } else { 
            node_index - 1 
        };

        let left_sibling_ptr = Pointer::<BTreeNode>::from_address(parent_node.values[left_sibling_index], self.pram.clone());
        let mut left_sibling = left_sibling_ptr.deref().map_err(Error::UnspecifiedMemoryError)?;
        
        Self::merge_right_sibling_inner(&mut left_sibling, &node.node, parent_node, left_sibling_index, node_index)?;

        // Free the memory of the right sibling
        self.pram.free(node.pointer.address, std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;

        // Now we can change the node we are working on the the left_sibling
        node.node = left_sibling;
        node.pointer = left_sibling_ptr;

        return Ok(true); // Placeholder return value
    }

    /// Merges the given node with its right sibling.
    ///
    /// Parameters:
    /// - 'node': The node to merge.
    /// - `parent_node`: The parent node of the node to merge.
    /// - `node_index`: The index of the node in the parent's children array.
    /// - `right_sibling_index`: The index of the right sibling in the parent's children array.
    /// 
    /// Returns:
    /// - `Ok(bool)` if the merge was successful. True if merged, false if not.
    /// - `Err(Error)` if there was an error during the operation.
    fn merge_right_sibling_inner(node: &mut BTreeNode, right_sibling: &BTreeNode, parent_node: &mut BTreeNode, node_index: usize, right_sibling_index: usize) -> Result<bool, Error> {
        if node.is_leaf{
            if right_sibling.length + node.length > BTREE_ORDER - 1 {
                return Ok(false); // Can't merge, would exceed max capacity
            }
        }else{
            // Check if they fit together
            if right_sibling.length + node.length > BTREE_ORDER {
                return Ok(false); // Can't merge, would exceed max capacity
            }
            
            // Move the separator key from the parent to the current node
            node.keys[node.length-1] = parent_node.keys[node_index];
            node.values[node.length-1] = node.values[BTREE_ORDER - 1]; // Move the last pointer to the end of the current list.
        }

        node.values[BTREE_ORDER - 1] = right_sibling.values[BTREE_ORDER - 1];

        // Move items from right sibling to node
        for i in 0..right_sibling.length {
            if node.length + i < BTREE_ORDER - 1 {
                node.keys[node.length + i] = right_sibling.keys[i];
            }
            node.values[node.length + i] = right_sibling.values[i];
        }

        if right_sibling_index == BTREE_ORDER -1 {
            parent_node.values[BTREE_ORDER -1] = parent_node.values[node_index]
        } else {
            // I do now contain all keys up to the rights key.
            parent_node.keys[node_index] = parent_node.keys[right_sibling_index];

            // Remove the right sibling pointer to the node from the parent node by shifting the entries left
            for i in node_index+1..parent_node.length-1 {
                if i + 1 < BTREE_ORDER - 1 {
                    parent_node.keys[i] = parent_node.keys[i + 1];
                }
                parent_node.values[i] = parent_node.values[i + 1];
            }
        }

        // Update the lengths
        parent_node.length -= 1;
        node.length += right_sibling.length;

        return Ok(true); // Placeholder return value
    }
    
    /// Merges the given node with its right sibling.
    ///
    /// Parameters:
    /// - 'node': The node to merge.
    /// - `parent_node`: The parent node of the node to merge.
    /// - `node_index`: The index of the node in the parent's children array.
    /// 
    /// Returns:
    /// - `Ok(bool)` if the merge was successful. True if merged, false if not.
    /// - `Err(Error)` if there was an error during the operation.
    fn merge_right_sibling(&self, node: &mut BTreeNode, parent_node: &mut BTreeNode, node_index: usize) -> Result<bool, Error> {
        if node_index >= parent_node.length -1 || node_index + 1 >= BTREE_ORDER || parent_node.length < 2 {
            return Ok(false); // No right sibling
        }

        let right_sibling_index = if node_index == parent_node.length - 2 {
            BTREE_ORDER - 1
        } else { 
            node_index + 1 
        };

        let right_sibling_ptr = Pointer::from_address(parent_node.values[right_sibling_index], self.pram.clone());
        let right_sibling = right_sibling_ptr.deref().map_err(Error::UnspecifiedMemoryError)?;
        
        Self::merge_right_sibling_inner(node, &right_sibling, parent_node, node_index, right_sibling_index)?;

        // Free the memory of the right sibling
        self.pram.free(right_sibling_ptr.address, std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;

        return Ok(true); // Placeholder return value
    }

    /// Recursive helper function to remove a key-value pair.
    /// 
    /// Parameters:
    /// - `key`: The key to remove.
    /// 
    /// Returns:
    /// - `Ok(value)` where `value` is the removed value if the operation was
    /// successful.
    /// - `Err(Error)` if there was an error during the operation.
    fn remove_recursive(&self, key: u64, mut node: &mut BTreeNodeRemovePointerCache, parent: Option<&mut BTreeNodeRemovePointerCache>, node_index: Option<usize>) -> Result<u64, Error> {        
        if node.node.is_leaf {
            // Search for the key to remove
            let key_index = node.node.keys.as_slice()[0..node.node.length].iter().position(|e| *e == key);
            if key_index.is_none() {
                return Err(Error::KeyNotFound);
            }
            let key_index = key_index.unwrap();
            let value = node.node.values[key_index];

            // Shift keys and values to remove the key-value pair
            for i in key_index..node.node.length-1 {
                node.node.keys[i] = node.node.keys[i + 1];
                node.node.values[i] = node.node.values[i + 1];
            }
            node.node.length -= 1;

            // Implementation for removing a key-value pair from a leaf node
            // Check if we need to handle the overflow
            if node.node.length < BTREE_ORDER/2 && parent.is_some() { 
                // Either borrow from sibling or merge with sibling
                // We can unwrap here because we checked for parent being None above.
                let parent = parent.unwrap();
                let node_index: usize = node_index.unwrap();

                if !self.try_borrow_right_sibling(&mut node.node, &mut parent.node, node_index)? { // Successfully borrowed from right sibling
                    if !self.try_borrow_left_sibling(&mut node.node, &mut parent.node, node_index)? { // Successfully borrowed from left sibling
                        // If neither sibling can lend a key, we need to handle underflow by merging nodes, this may lead to a underflow in the parent node which must be handled
                        if !self.merge_right_sibling(&mut node.node, &mut parent.node, node_index)? {
                            if !self.merge_left_sibling(&mut node, &mut parent.node, node_index)? {
                               
                                // Handle the case that the root now only has one child, upgrade the node to be root!
                                if parent.pointer.address == self.root.address && parent.node.length == 1 {  
                                    // promote current node to parent.

                                    // The root node must always be at address 0, so we need to copy it.
                                    parent.node.keys = node.node.keys;
                                    parent.node.values = node.node.values;
                                    parent.node.length = node.node.length;
                                    parent.node.is_leaf = node.node.is_leaf;
                                    // The pointer stays the same, overrwriting the root.
                                    
                                    // Free the current node.
                                    self.pram.free(node.pointer.address, std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;
                                    
                                    return Ok(value);
                                }

                                #[cfg(debug_assertions)]
                                {
                                    println!("{}", self.print_to_string());
                                    println!("Node index: {}", node_index);
                                    println!("Parent length: {}", parent.node.length);
                                }

                                panic!("Underflow could not be resolved!");
                            }
                        }
                    }
                };

            }
            return Ok(value);
        } else {
            let correct_index = node.node.keys.as_slice()[0..node.node.length-1].iter().position(|e| *e >= key);
            let index = if correct_index.is_none() {
                BTREE_ORDER -1
            } else {
                correct_index.unwrap()
            };

            let child_ptr = Pointer::from_address(node.node.values[index],self.pram.clone());
            let child = child_ptr.deref().map_err(Error::UnspecifiedMemoryError)?;

            let mut child = BTreeNodeRemovePointerCache {
                node: child,
                pointer: child_ptr.clone(),
            };

            // Recur into child node
            let result  = self.remove_recursive(key, &mut child, Some(&mut node), Some(index))?;
            
            child.pointer.set(&child.node).map_err(Error::UnspecifiedMemoryError)?;

            if node.node.length < BTREE_ORDER / 2 && parent.is_some() {
                // Handle underflow in internal nodes if needed
                // This part is left unimplemented for brevity
                let parent = parent.unwrap();
                let node_index = node_index.unwrap();

                if !self.try_borrow_right_sibling(&mut node.node, &mut parent.node, node_index)? { // Successfully borrowed from right sibling
                    if !self.try_borrow_left_sibling(&mut node.node, &mut parent.node, node_index)? { // Successfully borrowed from left sibling
                        // If neither sibling can lend a key, we need to handle underflow by merging nodes, this may lead to a underflow in the parent node which must be handled
                        if !self.merge_right_sibling(&mut node.node, &mut parent.node, node_index)? {
                            if !self.merge_left_sibling(&mut node, &mut parent.node, node_index)? {
                                panic!("Underflow could not be resolved!");
                            }
                        }
                    }
                };
            }

            return Ok(result);
        }
    }

    /// Removes a key-value pair from the B-tree index.
    /// 
    /// Parameters:
    /// - `key`: The key to remove.\
    /// 
    /// Returns:
    /// - `Ok(value)` where `value` is the removed value if the operation was successful.
    /// - `Err(Error)` if there was an error during the operation.
    fn remove(&self, key: u64) -> Result<u64, Error> {
        let root = self.root.deref().map_err(Error::UnspecifiedMemoryError)?;
        let mut root = BTreeNodeRemovePointerCache {
            node: root,
            pointer: self.root.clone(),
        };
        let res = self.remove_recursive(key, &mut root, None, None,)?;
        root.pointer.set(&root.node).map_err(Error::UnspecifiedMemoryError)?;
        Ok(res)
    }

    /// Recursive helper function to create an in-memory copy of the B-tree.
    /// 
    /// Parameters:
    /// - `node`: The current node being processed.
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    fn _reslove_recursive(&self, node: &mut BTreeMemCopyNode) {
        if node.is_leaf {
            return;
        }
        for i in 0..node.length {
            let child_i = if i +1 == node.length{
                (BTREE_ORDER -1) as usize
            } else { i };

            let child_ptr = Pointer::<BTreeNode>::from_address(node.values[child_i], self.pram.clone());
            let child_node = child_ptr.deref().map_err(Error::UnspecifiedMemoryError).unwrap();

            let mut mem_child_node = BTreeMemCopyNode {
                is_leaf: child_node.is_leaf,
                keys: child_node.keys,
                values: child_node.values,
                length: child_node.length,
                _children: Vec::new(),
            };

            self._reslove_recursive(&mut mem_child_node);

            node._children.push(mem_child_node);
        }
        
    }

    /// For Debugging, creates an in-memory copy of the entire B-tree.
    /// 
    /// Returns:
    /// - `BTreeMemCopyNode`: The root of the in-memory B-tree copy for debugging.
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    fn get_mem_copy(&self) -> BTreeMemCopyNode {
        let root = self.root.deref().map_err(Error::UnspecifiedMemoryError).unwrap();

        let mut mem_root = BTreeMemCopyNode {
            is_leaf: root.is_leaf,
            keys: root.keys,
            values: root.values,
            length: root.length,
            _children: Vec::new(), // This should be populated with children if needed
        };

        self._reslove_recursive(&mut mem_root);
        
        mem_root
    }

    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    fn print_to_string(&self) -> String {
        let root = self.get_mem_copy();

        fn print_node(node: &BTreeMemCopyNode, depth: usize, output: &mut String) {
            let indent = if node.is_leaf { "  ".repeat(depth) } else { "  ".repeat(depth) };
            output.push_str(&format!("{}{{ Node (is_leaf: {}, length: {})", indent, node.is_leaf, node.length));
            output.push_str(&format!(" Keys: {:?}", &node.keys));
            output.push_str(&format!(" Values: {:?} }}\n", &node.values));

            if !node.is_leaf {
                for child in node._children.iter() {
                    print_node(&child, depth + 1, output);
                }
            }
        }

        let mut output = String::new();
        print_node(&root, 0, &mut output);
        output
    }
}

struct LockGuard<'a> {
    read_lock: Option<RwLockUpgradableReadGuard<'a, ()>>,
    write_lock: Option<RwLockWriteGuard<'a, ()>>,
}

impl BTreeIndex for BTreeIndexPRAM {
    fn set(&self, key: u64, value: u64) -> Result<Option<u64>, Error> {
        let lock = self.lock.clone();
        // Acquire write lock as we are modifying the structure. (Locked until dropped)
        let upgradable_read_lock = lock.upgradable_read();
        let mut lock_guard = LockGuard {
            read_lock: Some(upgradable_read_lock),
            write_lock: None,
        };
        self.insert_or_update(key, value, &mut lock_guard)
    }

    fn get(&self, key: u64) -> Result<u64, Error> {
        // Acquire read lock as we are only reading the structure. (Locked until dropped)
        let _read_lock = self.lock.read();
        let leaf = self.find_leaf_node(key, &mut self.root.clone())?;

        // search for key in leafe node.
        let node = leaf.deref().map_err(Error::UnspecifiedMemoryError)?;
        assert!(node.is_leaf); // should be leaf node

        let key_index = node.keys.as_slice()[0..node.length].iter().position(|e| *e == key);
        if key_index.is_none() {
            Err(Error::KeyNotFound)
        }else{
            Ok(node.values[key_index.unwrap()])
        }
    }

    fn get_or_set(&self, key: u64, new_value: u64) -> Result<u64, Error> {
        let lock = self.lock.clone();
        // Acquire write lock as we are modifying the structure. (Locked until dropped)
        let upgradable_read_lock = lock.upgradable_read();
        let mut lock_guard = LockGuard {
            read_lock: Some(upgradable_read_lock),
            write_lock: None,
        };
        self.insert_or_get(key, new_value, &mut lock_guard)
    }

    fn contains(&self, key: u64) -> Result<bool, Error> {
        match self.get(key) {
            Ok(_) => Ok(true),
            Err(Error::KeyNotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn remove(&self, key: u64) -> Result<(), Error> {
        self.remove(key).map(|_| ())
    }

    fn persist(&self) -> Result<(), Error> {
        // Acquire write lock as we are persisting the structure. (Locked until dropped)
        let _read_lock = self.lock.read();
        let error = self.pram.persist();
        if let Err(persistent_random_access_memory::Error::FatalError(e)) = error {
            return Err(Error::FatalError(Box::new(Error::PersistenceError(*e))));
        }
        error.map_err(Error::PersistenceError)
    }
}

pub struct BTreeIndexPRAMIterator {
    index: BTreeIndexPRAM,
    lock: Arc<RwLock<()>>,
    next_key: u64,
}

impl Iterator for BTreeIndexPRAMIterator {
    // We are actually batching the itmes as it allows for more efficient sequential access.
    type Item = Vec<(u64, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let _guard: parking_lot::lock_api::RwLockReadGuard<'_, parking_lot::RawRwLock, ()> = self.lock.read(); // Acquire read lock as we are only reading the structure. (Locked until dropped)

        let current_node = self.index.find_leaf_node(self.next_key, &mut self.index.root.clone()).ok()?;
        let current_node: BTreeNode = current_node.deref().ok()?;

        // Now return the first element of the new leaf
        let res = Some(current_node.keys.iter().zip(current_node.values.iter())
            // The filter is needed in case a concurrent merge or borrow happened and the next_key is in the previous iterated node now.
            // without it we may return keys that have already been returned.
            .filter(|(k, _)| **k >= self.next_key)
            .take(current_node.length as usize)
            .map(|(&k, &v)| (k, v))
            .collect());

        let max_key = if current_node.length > 0 {
            current_node.keys[current_node.length as usize - 1]
        } else {
            return None; // No more keys or broken state.
        };

        if max_key < self.next_key {
            return None; // No more keys or broken state.
        } else {
            // Set next key to one more than the maximum key in the current node.
            self.next_key = max_key +1;
        }

        res
    }
}

impl BTreeIndexPRAM {
    /// Creates an iterator over the B-tree index.
    /// 
    /// Returns:
    /// - `BTreeIndexPRAMIterator`: An iterator over the key-value pairs in the B-tree.
    pub fn iter(&self) -> Result<BTreeIndexPRAMIterator, Error> {      
        let me = self.clone(); 
        let lock = Arc::clone(&self.lock); 
        Ok(BTreeIndexPRAMIterator {
            index: me,
            lock: lock,
            next_key: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistent_random_access_memory::PersistentRandomAccessMemory;
    use std::{path::PathBuf, time::Instant};

    fn temp_path(name: &str) -> String {
        let mut p = PathBuf::from(std::env::temp_dir());
        p.push(format!("webcanvas_btree_{}", name));
        p.to_string_lossy().into_owned()
    }

    fn make_pram(id: &str) -> Arc<PersistentRandomAccessMemory> {
        // 4 MiB total, 4 KiB pages, moderate LRU settings
        let path = temp_path(&format!("pram_test{}", id));
        PersistentRandomAccessMemory::new(16 * 1024 * 1024, &path)
    }

    fn new_index(id: &str) -> BTreeIndexPRAM {
        let pram = make_pram(id);
        BTreeIndexPRAM::new(pram)
    }

    #[test]
    fn set_and_get_single_key() {
        let idx = new_index("set_and_get_single_key");
        let key = 42u64;
        let val = 777u64;
        idx.set(key, val).expect("set should succeed");
        let got = idx.get(key).expect("get should return value");
        assert_eq!(got, val);
    }

    #[test]
    fn update_existing_key() {
        let idx = new_index("update_existing_key");
        let key = 10u64;
        idx.set(key, 1).unwrap();
        idx.set(key, 2).unwrap();
        assert_eq!(idx.get(key).unwrap(), 2);
    }

    #[test]
    fn missing_key_returns_error() {
        let idx = new_index("missing_key_returns_error");
        match idx.get(999) { 
            Err(Error::KeyNotFound) => {},
            other => panic!("expected KeyNotFound, got {:?}", other),
        }
    }

    #[test]
    fn many_inserts_cause_splits_and_remain_searchable() {
        let idx = new_index("many_inserts_cause_splits_and_remain_searchable");
        // Insert more than BTREE_ORDER to force splits
        let count = BTREE_ORDER as u64 * 32; // exceed order
        for k in 0..count {
            let v = k * 3;
            idx.set(k, v).unwrap();
        }

        // Verify
        for k in 0..count {
            let expected = k * 3;
            let got = idx.get(k).unwrap();
            assert_eq!(got, expected, "mismatch at key {}", k);
        }
    }

    #[test]
    fn remove_existing_key() {
        let idx = new_index("remove_existing_key");
        for k in 0..100u64 { idx.set(k, k).unwrap(); }
        idx.remove(50).unwrap();
        match idx.get(50) {
            Err(Error::KeyNotFound) => {},
            e => panic!("expected KeyNotFound after remove {}", e.unwrap_err()),
        }
        // Ensure neighbors still readable
        assert_eq!(idx.get(49).unwrap(), 49);
        assert_eq!(idx.get(51).unwrap(), 51);
    }

    #[test]
    fn remove_underflow_borrow_or_merge_paths() {
        let idx = new_index("remove_underflow_borrow_or_merge_paths");
        // Construct distribution to potentially trigger borrow/merge.
        // Insert enough keys to fill multiple nodes, then remove many to cause underflow.
        let total = (BTREE_ORDER as u64)*16+100;
        for k in 0..total { idx.set(k, k).unwrap(); }

        // Remove alternating keys to stress sibling logic
        for r in (0..total).step_by(2) {
            idx.remove(r).unwrap();
        }
        // Remaining odd keys should still be present
        for g in (1..total).step_by(2) {
            assert_eq!(idx.get(g).unwrap(), g);
            assert!(idx.get(g - 1).is_err());
        }
    }
 
    #[test]
    fn persist_does_not_panic() {
        let idx = new_index("persist_does_not_panic");
        idx.persist().expect("persist should succeed");
    }

    #[test]
    fn stress_randomized_small_range() {
        let idx = new_index("stress_randomized_small_range");
        // Deterministic pseudo-random without external crates
        let mut seed: u64 = 0xDEADBEEFCAFEBABE;
        let mut keys = Vec::new();

        // Stopwatch
        let start: Instant = Instant::now();
        for _ in 0..100_000 {
            // xorshift64*
            seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
            let k = (seed % 16_384) as u64;
            let v = seed ^ 0xA5A5A5A5A5A5A5A5;

            idx.set(k, v).unwrap();
            keys.push((k, v));
        }

        println!("Inserted 100k entries in {:?}", start.elapsed());

        // Persist
        let start = Instant::now();
        idx.persist().unwrap();
        println!("Persisted index in {:?}", start.elapsed());

        let start = Instant::now();
        // Verify latest values for each key
        use std::collections::HashMap;
        let mut last: HashMap<u64, u64> = HashMap::new();
        for (k, v) in keys { last.insert(k, v); }
        for (k, v) in last { assert_eq!(idx.get(k).unwrap(), v); }
        println!("Verified 100k entries in {:?}", start.elapsed());
    }

    /// Deterministic pseudo-random generator (xorshift64*).
    fn xorshift64(mut s: u64) -> u64 {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    }

    /// Reusable helper: generate a random B+ tree with mixed insert/update/delete operations.
    /// Returns the built index and a map of expected final key->value pairs.
    ///
    /// Parameters:
    /// - seed: deterministic seed for PRNG
    /// - ops: number of total operations to perform
    /// - key_range: keys are in [0, key_range)
    /// - delete_rate: approximate probability (0..=100) to delete on a step
    /// - update_rate: approximate probability (0..=100) to update on a step (if key exists)
    fn generate_random_tree(
        seed: u64,
        ops: usize,
        key_range: u64,
        delete_rate: u32,
        update_rate: u32,
        id: &str,
    ) -> (super::BTreeIndexPRAM, std::collections::HashMap<u64, u64>) {
        let idx = new_index(&format!("generate_random_tree_{}", id));
        use std::collections::HashMap;
        let mut expected: HashMap<u64, u64> = HashMap::new();
        let mut s = seed;

        for _ in 0..ops {
            s = xorshift64(s);
            let k = (s % key_range) as u64;
            let v = s ^ 0xC3C3C3C3C3C3C3C3;

            let roll = (s & 0xFFFF) as u32 % 100;

            if roll < delete_rate {
                // attempt delete
                if expected.remove(&k).is_some() {
                    let res = idx.remove(k);
                    if let Err(e) = res {
                        println!("Error during delete: {:?}", e);
                        panic!("Expected successful delete for key {}, got error {:?}", k, e);
                    }
                } else {
                    // deleting non-existing is allowed but should error; ignore result
                    assert!(idx.remove(k).is_err());
                }
            } else if roll < delete_rate + update_rate {
                // update if exists, else insert
                expected.insert(k, v);
                idx.set(k, v).unwrap();
            } else {
                // insert new value (acts as upsert)
                expected.insert(k, v);
                idx.set(k, v).unwrap();
            }
        }

        (idx, expected)
    }

    #[test]
    fn randomized_mixed_insert_update_delete_reusable() {
        let seed: u64 = 0x0123_4567_89AB_CDEF;
        let ops: usize = 100_000;
        let key_range: u64 = 10_000;
        let delete_rate: u32 = 20; // 20%
        let update_rate: u32 = 30; // 30%

        let (idx, expected) = generate_random_tree(seed, ops, key_range, delete_rate, update_rate, "randomized_mixed_insert_update_delete_reusable");

        // Verify all expected keys exist with latest values
        for (k, v) in expected.iter() {
            assert_eq!(idx.get(*k).unwrap(), *v);
        }

        // Spot-check a few keys outside expected should error (probabilistic)
        for probe in [key_range + 1, key_range + 2, key_range + 3] {
            assert!(idx.get(probe).is_err());
        }
    }

    #[test]
    fn integration_test_pram(){
        let seed: u64 = 0x0123_4567_89AB_CDEF;
        let ops: usize = 100_000;
        let key_range: u64 = 10_000;
        let delete_rate: u32 = 20; // 20%
        let update_rate: u32 = 30; // 30%

        let (idx, expected) = generate_random_tree(seed, ops, key_range, delete_rate, update_rate, "integration_test_pram");

        // Verify all expected keys exist with latest values
        for (k, v) in expected.iter() {
            assert_eq!(idx.get(*k).unwrap(), *v);
        }

        // Spot-check a few keys outside expected should error (probabilistic)
        for probe in [key_range + 1, key_range + 2, key_range + 3] {
            assert!(idx.get(probe).is_err());
        }
    }

    #[test]
    fn iter_test(){
        let seed: u64 = 0x0123_4567_89AB_CDEF;
        let ops: usize = 100_000;
        let key_range: u64 = 10_000;
        let delete_rate: u32 = 20; // 20%
        let update_rate: u32 = 30; // 30%

        let (idx, expected) = generate_random_tree(seed, ops, key_range, delete_rate, update_rate, "iter_test");

        println!("Expected length: {}", expected.len());
        println!("Index length: {}", idx.iter().unwrap().count());
        let mut last_key = 0;
        for (k, v) in idx.iter().unwrap().flatten() {
            assert_eq!(expected.get(&k).unwrap(), &v);

            // Check ordering
            assert!(k >= last_key);
            last_key = k;
        }
    }

    #[test]
    fn iterator_returns_sorted_and_latest_values() {
        let idx = new_index("iterator_returns_sorted_and_latest_values");

        let inputs = vec![
            (9u64, 90u64),
            (3, 30),
            (7, 70),
            (1, 10),
            (3, 33), // update existing key to ensure latest value is surfaced
        ];

        for (k, v) in inputs.iter().copied() {
            idx.set(k, v).unwrap();
        }

        let collected: Vec<(u64, u64)> = idx.iter().unwrap().flatten().collect();
        let expected = vec![(1u64, 10u64), (3, 33), (7, 70), (9, 90)];

        assert_eq!(collected, expected);
    }
}