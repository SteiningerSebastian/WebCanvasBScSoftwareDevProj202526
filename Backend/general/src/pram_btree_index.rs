use std::{fmt::Display, rc::Rc};

use crate::persistent_random_access_memory::{Error as PRAMError, PersistentRandomAccessMemory, Pointer};

#[derive(Debug)]
pub enum Error {
    KeyNotFound,
    StorageError,
    InvalidOperation,
    UnspecifiedMemoryError(PRAMError),
    PersistenceError(PRAMError),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::KeyNotFound => write!(f, "Key not found"),
            Error::StorageError => write!(f, "Storage error"),
            Error::InvalidOperation => write!(f, "Invalid operation"),
            Error::UnspecifiedMemoryError(e) => write!(f, "Unspecified memory error: {}", e),
            Error::PersistenceError(e) => write!(f, "Persistence error: {}", e),
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
    /// - `Ok(())` if the operation was successful.
    /// - `Err(Error)` if there was an error during the operation.
    fn set(&mut self, key: u64, value: &u64) -> Result<(), Error>;

    /// Gets the value associated with the given key.
    /// 
    /// Parameters:
    /// - `key`: The key to retrieve.
    /// 
    /// Returns:
    /// - `Ok(value)` if the key exists, where `value` is the associated value.
    fn get(&self, key: u64) -> Result<u64, Error>;

    /// Removes the value associated with the given key.
    ///
    /// Parameters:
    /// - `key`: The key to remove.
    /// 
    /// Returns:
    /// - `Ok(())` if the operation was successful.
    fn remove(&mut self, key: u64) -> Result<(), Error>;

    /// Persists the B-tree index to the underlying storage.
    /// 
    /// Returns:
    /// - `Ok(())` if the operation was successful.
    /// - `Err(Error)` if there was an error during the operation.
    fn persist(&self) -> Result<(), Error>;
}

// The order of the B-tree (maximum number of children per node)
const BTREE_ORDER: usize = 1000; // Assuming 16kb pages and u64 (8 bytes) for keys and values (16 bytes total per entry) + some overhead for length, is_leaf and padding
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

pub struct BTreeIndexPRAM {
    pram: Rc<dyn PersistentRandomAccessMemory>,
    root: Pointer,
}

impl BTreeIndexPRAM {
    pub fn new(pram: Rc<dyn PersistentRandomAccessMemory>) -> Self {
        // Allocate root node in PRAM
        let mut root = pram.smalloc(ROOT_NODE_ADDRESS, std::mem::size_of::<BTreeNode>()).unwrap();

        let node = root.deref::<BTreeNode>().unwrap();

        // Write the default root to it if its the frist time we start.
        if node.length == 0 {
            let node = BTreeNode {
                is_leaf: true,
                keys: [0; BTREE_ORDER - 1],
                values: [0; BTREE_ORDER],
                length: 0,
            };

            root.write(&node).unwrap();
        }
        
        BTreeIndexPRAM { 
            pram,
            root,
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
    fn find_leaf_node(&self, key: u64, node_ptr: &mut Pointer) -> Result<Pointer, Error> {
        let unsafe_node = node_ptr.unsafe_deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

        let node = if unsafe_node.is_some() {
            *unsafe_node.unwrap()
        }else {
            *(node_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?)
        };

        // Can not find key in empty list.
        if node.length == 0 {
            return Err(Error::KeyNotFound);
        }

        if node.is_leaf {
            Ok((*node_ptr).clone()) // return the leaf node pointer
        }else {
            // find the child pointer to follow
            let correct_index = node.keys.as_slice()[0..node.length].iter().position(|e| *e >= key);
                        
            let index = if correct_index.is_none() {
                node.length
            } else {
                correct_index.unwrap()
            };

            let mut child_ptr = Pointer::from_address(node.values[index],self.pram.clone());
            self.find_leaf_node(key, &mut child_ptr)
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
    /// - `Ok(Some((median_key, new_node_ptr)))` if the node was split, where `median_key` is the key to promote and `new_node_ptr` is the pointer to the new node.
    /// - `Ok(None)` if no split was needed.
    fn insert_or_update_recursive(&mut self, key: u64, value: &u64, node_ptr: &mut Pointer) -> Result<Option<(u64, Pointer)>, Error> {
        let mut node = node_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;
        if node.is_leaf {
            // Check if there is space in the node
            if node.length < BTREE_ORDER - 1 {
                // Search for the correct position to insert the new key
                let insert_index = node.keys.as_slice()[0..node.length].iter().position(|e| *e >= key);
                
                // If key already exists, update the value
                if let Some(idx) = insert_index {
                    if node.keys[idx] == key {
                        node.values[idx] = *value;

                        // Write back the updated node
                        node_ptr.write(&(*node)).map_err(Error::UnspecifiedMemoryError)?;

                        return Ok(None); // No split needed
                    }
                }

                // Shift keys and values to make space for the new key-value pair
                let index = insert_index.unwrap_or(node.length);
                for i in (index..=node.length).rev() {
                    if i < node.length {
                        node.keys[i + 1] = node.keys[i];
                    }
                    node.values[i + 1] = node.values[i];
                }

                // Insert the new key and value
                node.keys[index] = key;
                node.values[index] = *value;
                node.length += 1;

                // Write back the updated node
                node_ptr.write(&(*node)).map_err(Error::UnspecifiedMemoryError)?;

                Ok(None) // No split needed
            } else { // Need to split the node
                // Find median key
                let median_index = node.length / 2;
                let median_key = node.keys[median_index];

                // Create new node
                let mut new_node = BTreeNode {
                    is_leaf: true,
                    keys: [0; BTREE_ORDER - 1],
                    values: [0; BTREE_ORDER],
                    length: 0,
                };

                // Move first half of keys and values to new node
                for i in 0..=median_index {
                    new_node.keys[i] = node.keys[i];
                    new_node.values[i] = node.values[i];
                }
                new_node.length = median_index + 1; // new_node gets the first half including median

                // Shift keys and values in the original node to left.
                for i in median_index+1..node.length {
                    node.keys[i-median_index-1] = node.keys[i];
                    node.values[i-median_index-1] = node.values[i];
                }
                node.length = node.length - median_index - 1; // node = right half

                // Now actually add the value to the correct node
                if key > median_key {
                    // Insert into the original node
                    let insert_index = node.keys[0..node.length].iter().position(|e| *e >= key);
                    let index = insert_index.unwrap_or(node.length);

                    for i in (index..=node.length).rev() {
                        if i < node.length {
                            node.keys[i + 1] = node.keys[i];
                        }
                        node.values[i + 1] = node.values[i];
                    }

                    node.keys[index] = key;
                    node.values[index] = *value;
                    node.length += 1;
                } else {
                    // Insert into the new node
                    let insert_index = new_node.keys.as_slice()[0..new_node.length].iter().position(|e| *e >= key);
                    let index = insert_index.unwrap_or(new_node.length);

                    for i in (index..=new_node.length).rev() {
                        if i < new_node.length {
                            new_node.keys[i + 1] = new_node.keys[i];
                        }
                        new_node.values[i + 1] = new_node.values[i];
                    }

                    new_node.keys[index] = key;
                    new_node.values[index] = *value;
                    new_node.length += 1;
                }

                // Write back the updated nodes
                let mut new_node_ptr = self.pram.malloc(std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;
                new_node_ptr.write(&new_node).map_err(Error::UnspecifiedMemoryError)?;
                node_ptr.write(&(*node)).map_err(Error::UnspecifiedMemoryError)?;

                // The median key is actually not in the new node, it's the first key of the original node
                return Ok(Some((median_key, new_node_ptr))); // Return median key and new node pointer
            }
        } else {
            // find the child pointer to follow
            let correct_index = node.keys.as_slice()[0..node.length].iter().position(|e| *e >= key);
            let index = if correct_index.is_none() {
                node.length
            } else {
                correct_index.unwrap()
            };

            let mut child_ptr = Pointer::from_address(node.values[index],self.pram.clone());

            let split_result = self.insert_or_update_recursive(key, value, &mut child_ptr)?;
            
            if let Some((marker, new_child_ptr)) = split_result {
                // Need to insert median_key and new_child_ptr into this node
                if node.length < BTREE_ORDER - 1 {
                    // Shift keys and values to make space for the new key-value pair
                    for i in (index..=node.length).rev() {
                        if i < node.length {
                            node.keys[i + 1] = node.keys[i];
                        }
                        node.values[i + 1] = node.values[i];
                    }

                    // Insert the new key and value
                    node.keys[index] = marker;
                    node.values[index] = new_child_ptr.pointer;
                    node.length += 1;

                    // Write back the updated node
                    node_ptr.write(node.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

                    return Ok(None); // No split needed
                } else {
                    // Need to split this internal node
                    let median_index = node.length / 2;
                    let median_key = node.keys[median_index];

                    let mut new_node = BTreeNode {
                        is_leaf: false, // we are spliting an internal node so the new node is also internal.
                        keys: [0; BTREE_ORDER - 1],
                        values: [0; BTREE_ORDER],
                        length: 0,
                    };

                    // Move first half of keys and values to new node
                    for i in 0..=median_index {
                        new_node.keys[i] = node.keys[i];
                        new_node.values[i] = node.values[i];
                    }
                    new_node.length = median_index + 1; // new_node gets the first half including median

                    // Shift keys and values in the original node to left.
                    for i in median_index+1..=node.length {
                        if i < node.length {
                            node.keys[i-median_index-1] = node.keys[i];
                        }
                        node.values[i-median_index-1] = node.values[i];
                    }
                    node.length = node.length - median_index - 1; // node = right half 

                    // Now actually add the median key and new child pointer to the correct node
                    if marker > median_key {
                        let insert_index = node.keys[0..node.length].iter().position(|e| *e >= marker);
                        let index = insert_index.unwrap_or(node.length);

                        // Insert into the original node
                        for i in (index..=node.length).rev() {
                            if i < node.length {
                                node.keys[i + 1] = node.keys[i];
                            }
                            node.values[i + 1] = node.values[i];
                        }

                        node.keys[index] = marker;
                        node.values[index] = new_child_ptr.pointer;

                        node.length += 1;
                    } else {
                        let insert_index = new_node.keys[0..new_node.length].iter().position(|e| *e >= marker);
                        let index = insert_index.unwrap_or(new_node.length);

                        // Insert into the new node
                        for i in (index..=new_node.length).rev() {
                            if i < new_node.length {
                                new_node.keys[i + 1] = new_node.keys[i];
                            }
                            new_node.values[i + 1] = new_node.values[i];
                        }
                        new_node.keys[index] = marker;
                        new_node.values[index] = new_child_ptr.pointer;
                        new_node.length += 1;
                    }

                    // Write back the updated nodes
                    let mut new_node_ptr = self.pram.malloc(std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;
                    new_node_ptr.write(&new_node).map_err(Error::UnspecifiedMemoryError)?;
                    node_ptr.write(node.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

                    return Ok(Some((median_key, new_node_ptr))); // Return median key and new node pointer
                }
            }
            Ok(None) // No split needed
        }
    }

    /// Inserts or updates a key-value pair in the B-tree index.
    ///
    /// Parameters:
    /// - `key`: The key to insert or update.
    /// - `value`: The value to associate with the key.
    /// 
    /// Returns:
    /// - `Ok(())` if the operation was successful.
    /// - `Err(Error)` if there was an error during the operation.
    fn insert_or_update(&mut self, key: u64, value: &u64) -> Result<(), Error> {
        let split = self.insert_or_update_recursive(key, value, &mut self.root.clone())?;

        if let Some((median_key, new_node_ptr)) = split {
            let root = self.root.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

            let mut new_child_ptr = self.pram.malloc(std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;

            // Need to create a new root
            let mut new_root = BTreeNode {
                is_leaf: false,
                keys: [0; BTREE_ORDER - 1],
                values: [0; BTREE_ORDER],
                length: 1,
            };
            new_root.keys[0] = median_key;
            new_root.values[0] = new_node_ptr.pointer;
            new_root.values[1] = new_child_ptr.pointer;

            // Write the root node
            new_child_ptr.write(root.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

            // Write the new root node to the PRAM
            self.root.write(&new_root).map_err(Error::UnspecifiedMemoryError)?;
        }

        Ok(())
    }

    /// Borrows a key from the left sibling of the given node.
    ///
    /// Parameters:
    /// - `node_ptr`: The pointer to the node that needs to borrow a key.
    /// - `parent_ptr`: The pointer to the parent node.
    /// - `node_index`: The index of the node in the parent's children array.
    /// 
    /// Returns:
    /// - `Ok(true)` if a key was successfully borrowed.
    /// - `Ok(false)` if borrowing was not possible.
    fn try_borrow_left_sibling(&mut self, node_ptr: &mut Pointer, parent_ptr: &mut Pointer, node_index: usize) -> Result<bool, Error> {
        let mut node = node_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;
        let mut parent_node = parent_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

        // Try left sibling
        let mut left_sibling_ptr = Pointer::from_address(parent_node.values[node_index - 1], self.pram.clone());
        let mut left_sibling = left_sibling_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

        if left_sibling.length > BTREE_ORDER/2 {
            // Borrow from left sibling
            // Shift keys and values in current node to right
            for i in (0..node.length).rev() {
                node.keys[i + 1] = node.keys[i];
                node.values[i + 1] = node.values[i];
            }

            // Move key from left_sibling to current node
            node.keys[0] = left_sibling.keys[left_sibling.length - 1]; // the most left key from the left sibling
            node.values[0] = left_sibling.values[left_sibling.length - 1]; // and its associated value

            node.length += 1;
            left_sibling.length -= 1;

            // Update parent key
            parent_node.keys[node_index - 1] = left_sibling.keys[left_sibling.length - 1]; // the last key in the new left sibling

            // Write back updated nodes
            left_sibling_ptr.write(left_sibling.as_ref()).map_err(Error::UnspecifiedMemoryError)?;
            node_ptr.write(node.as_ref()).map_err(Error::UnspecifiedMemoryError)?;
            parent_ptr.write(parent_node.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

            return Ok(true); // Placeholder return value
        }else{
            return Ok(false);
        }
    }

    /// Borrows a key from the right sibling of the given node.
    /// 
    /// Parameters:
    /// - `node_ptr`: The pointer to the node that needs to borrow a key.
    /// - `parent_ptr`: The pointer to the parent node.
    /// - `node_index`: The index of the node in the parent's children array.
    /// 
    /// Returns:
    /// - `Ok(true)` if a key was successfully borrowed.
    /// - `Ok(false)` if borrowing was not possible.
    fn try_borrow_right_sibling(&mut self, node_ptr: &mut Pointer, parent_ptr: &mut Pointer, node_index: usize) -> Result<bool, Error> {
        // Try right sibling
        let mut node = node_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;
        let mut parent_node = parent_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

        let mut right_sibling_ptr = Pointer::from_address(parent_node.values[node_index + 1], self.pram.clone());
        let mut right_sibling = right_sibling_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

        if right_sibling.length > BTREE_ORDER/2 {
            // Borrow from right sibling
            // Move key from right_sibling to current node
            node.keys[node.length] = right_sibling.keys[0]; // the most left key from the right sibling
            node.values[node.length] = right_sibling.values[0]; // and its associated value

            node.length += 1;

            // Shift keys and values in right sibling to left
            for i in 0..right_sibling.length-1 {
                right_sibling.keys[i] = right_sibling.keys[i + 1];
                right_sibling.values[i] = right_sibling.values[i + 1];
            }
            right_sibling.length -= 1;

            // Update parent key
            parent_node.keys[node_index] = right_sibling.keys[0]; // the first key in the new right sibling

            // Write back updated nodes
            right_sibling_ptr.write(right_sibling.as_ref()).map_err(Error::UnspecifiedMemoryError)?;
            node_ptr.write(node.as_ref()).map_err(Error::UnspecifiedMemoryError)?;
            parent_ptr.write(parent_node.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

            return Ok(true); // Placeholder return value
        }else{
            return Ok(false);
        }
    }

    /// Merges the given node with its left sibling.
    /// 
    /// Parameters:
    /// - `node_ptr`: The pointer to the node to merge.
    /// - `parent_ptr`: The pointer to the parent node.
    /// - `node_index`: The index of the node in the parent's children array.
    /// 
    /// Returns:
    /// - `Ok(())` if the merge was successful.
    /// - `Err(Error)` if there was an error during the operation.
    fn merge_left_sibling(&mut self, node_ptr: &mut Pointer, parent_ptr: &mut Pointer, node_index: usize) -> Result<(), Error> {
        let node = node_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;
        let mut parent_node = parent_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

        let mut left_sibling_ptr = Pointer::from_address(parent_node.values[node_index - 1], self.pram.clone());
        let mut left_sibling = left_sibling_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

        // Move all keys and values from current node to left sibling
        for i in 0..node.length {
            left_sibling.keys[left_sibling.length + i] = node.keys[i];
            left_sibling.values[left_sibling.length + i] = node.values[i];
        }
        left_sibling.length += node.length;

        // Update parent key
        parent_node.keys[node_index - 1] = left_sibling.keys[left_sibling.length - 1]; // the last key in the new left sibling

        // Remove the key and pointer from the parent
        for i in node_index..parent_node.length {
            parent_node.keys[i] = parent_node.keys[i + 1];
            parent_node.values[i] = parent_node.values[i + 1];
        }
        parent_node.length -= 1;

        // Write back updated nodes
        left_sibling_ptr.write(left_sibling.as_ref()).map_err(Error::UnspecifiedMemoryError)?;
        parent_ptr.write(parent_node.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

        // Free the memory of the current node
        self.pram.free(node_ptr.clone(),  std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;

        return Ok(()); // Placeholder return value
    }

    /// Merges the given node with its right sibling.
    ///
    /// Parameters:
    /// - `node_ptr`: The pointer to the node to merge.
    /// - `parent_ptr`: The pointer to the parent node.
    /// - `node_index`: The index of the node in the parent's children array.
    /// 
    /// Returns:
    /// - `Ok(())` if the merge was successful.
    /// - `Err(Error)` if there was an error during the operation.
    fn merge_right_sibling(&mut self, node_ptr: &mut Pointer, parent_ptr: &mut Pointer, node_index: usize) -> Result<(), Error> {
        let mut node = node_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;
        let mut parent_node = parent_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

        let right_sibling_ptr = Pointer::from_address(parent_node.values[node_index + 1], self.pram.clone());
        let right_sibling = right_sibling_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

        // Move all keys and values from right sibling to current node
        for i in 0..right_sibling.length {
            node.keys[node.length + i] = right_sibling.keys[i];
            node.values[node.length + i] = right_sibling.values[i];
        }
        node.length += right_sibling.length;

        // Update parent key
        parent_node.keys[node_index] = node.keys[node.length - 1]; // the last key in the current node

        // Remove the key and pointer from the parent
        for i in node_index+1..parent_node.length {
            parent_node.keys[i] = parent_node.keys[i + 1];
            parent_node.values[i] = parent_node.values[i + 1];
        }
        parent_node.length -= 1;

        // Write back updated nodes
        node_ptr.write(node.as_ref()).map_err(Error::UnspecifiedMemoryError)?;
        parent_ptr.write(parent_node.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

        // Free the memory of the right sibling
        self.pram.free(right_sibling_ptr.clone(),  std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;

        return Ok(()); // Placeholder return value
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
    fn remove_recursive(&mut self, key: u64, node_ptr: &mut Pointer, parent: Option<&mut Pointer>, node_index: Option<usize>) -> Result<u64, Error> {
        let mut node = node_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;
        if node.is_leaf {
            // Search for the key to remove
            let key_index = node.keys.as_slice()[0..node.length].iter().position(|e| *e == key);
            if key_index.is_none() {
                return Err(Error::KeyNotFound);
            }
            let key_index = key_index.unwrap();
            let value = node.values[key_index];

            // Shift keys and values to remove the key-value pair
            for i in key_index..node.length-1 {
                node.keys[i] = node.keys[i + 1];
                node.values[i] = node.values[i + 1];
            }
            node.length -= 1;

            // Implementation for removing a key-value pair from a leaf node
            // Check if there is enough keys to remove without underflow or if it's the root
            if node.length >= BTREE_ORDER/2 || parent.is_none() { 
                // Write back the updated node
                node_ptr.write(node.as_ref()).map_err(Error::UnspecifiedMemoryError)?;
                return Ok(value);
            } else { // Need to handle underflow
                // Either borrow from sibling or merge with sibling
                // We can unwrap here because we checked for parent being None above
                let parent_ptr = parent.unwrap();
                let node_index = node_index.unwrap();

                let merge_right = !self.try_borrow_right_sibling(node_ptr, parent_ptr, node_index)?;
                let merge_left = !self.try_borrow_left_sibling(node_ptr, parent_ptr, node_index)?;

                // If neither sibling can lend a key, we need to handle underflow by merging nodes, this may lead to a underflow in the parent node which must be handled

                // Check if we can merge with the left sibling.
                if merge_left {
                    self.merge_left_sibling(node_ptr, parent_ptr, node_index)?;
                    return Ok(value);
                } else if merge_right { // Merge with right sibling
                    self.merge_right_sibling(node_ptr, parent_ptr, node_index)?;
                    return Ok(value);
                }

                // If we get here the parent broke its gurantees.
                panic!("Underflow in internal nodes not expected here - should be handled by caller!");
            }
        }else {
            let correct_index = node.keys.as_slice()[0..node.length].iter().position(|e| *e >= key);
            let index = if correct_index.is_none() {
                node.length
            } else {
                correct_index.unwrap()
            };

            let mut child_ptr = Pointer::from_address(node.values[index],self.pram.clone());

            // Recur into child node
            let result  = self.remove_recursive(key, &mut child_ptr, Some(node_ptr), Some(index))?;

            if node.length < BTREE_ORDER / 2 {
                // Handle underflow in internal nodes if needed
                // This part is left unimplemented for brevity
                let parent_ptr = parent.unwrap();
                let node_index = node_index.unwrap();

                let merge_right = !self.try_borrow_right_sibling(node_ptr, parent_ptr, node_index)?;
                let merge_left = !self.try_borrow_left_sibling(node_ptr, parent_ptr, node_index)?;

                // If neither sibling can lend a key, we need to handle underflow by merging nodes, this may lead to a underflow in the parent node which must be handled

                // Check if we can merge with the left sibling.
                if merge_left {
                    self.merge_left_sibling(node_ptr, parent_ptr, node_index)?;
                    return Ok(result);
                } else if merge_right { // Merge with right sibling
                    self.merge_right_sibling(node_ptr, parent_ptr, node_index)?;
                    return Ok(result);
                }
            }

            Ok(result)
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
    fn remove(&mut self, key: u64) -> Result<u64, Error> {
        self.remove_recursive(key, &mut self.root.clone(), None, None)
    }
}

impl BTreeIndex for BTreeIndexPRAM {
    fn set(&mut self, key: u64, value: &u64) -> Result<(), Error> {
        self.insert_or_update(key, value)
    }

    fn get(&self, key: u64) -> Result<u64, Error> {
        let leaf = self.find_leaf_node(key, &mut self.root.clone())?;

        // search for key in leafe node.
        let node = leaf.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;
        assert!(node.is_leaf); // should be leaf node

        let key_index = node.keys.as_slice()[0..node.length].iter().position(|e| *e == key);
        if key_index.is_none() {
            Err(Error::KeyNotFound)
        }else{
            Ok(node.values[key_index.unwrap()])
        }
    }

    fn remove(&mut self, key: u64) -> Result<(), Error> {
        self.remove(key).map(|_| ())
    }

    fn persist(&self) -> Result<(), Error> {
        self.pram.persist().map_err(Error::PersistenceError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistent_random_access_memory::FilePersistentRandomAccessMemory;
    use std::{path::PathBuf, time::Instant};

    fn temp_path(name: &str) -> String {
        let mut p = PathBuf::from(std::env::temp_dir());
        p.push(format!("webcanvas_btree_{}", name));
        p.to_string_lossy().into_owned()
    }

    fn make_pram() -> Rc<dyn PersistentRandomAccessMemory> {
        // 4 MiB total, 4 KiB pages, moderate LRU settings
        let path = temp_path("pram_test");
        FilePersistentRandomAccessMemory::new(4 * 1024 * 1024, &path, 16 * 1024, 256, 8, 2)
    }

    fn new_index() -> BTreeIndexPRAM {
        let pram = make_pram();
        BTreeIndexPRAM::new(pram)
    }

    #[test]
    fn set_and_get_single_key() {
        let mut idx = new_index();
        let key = 42u64;
        let val = 777u64;
        idx.set(key, &val).expect("set should succeed");
        let got = idx.get(key).expect("get should return value");
        assert_eq!(got, val);
    }

    #[test]
    fn update_existing_key() {
        let mut idx = new_index();
        let key = 10u64;
        idx.set(key, &1).unwrap();
        idx.set(key, &2).unwrap();
        assert_eq!(idx.get(key).unwrap(), 2);
    }

    #[test]
    fn missing_key_returns_error() {
        let idx = new_index();
        match idx.get(999) { 
            Err(Error::KeyNotFound) => {},
            other => panic!("expected KeyNotFound, got {:?}", other),
        }
    }

    #[test]
    fn many_inserts_cause_splits_and_remain_searchable() {
        let mut idx = new_index();
        // Insert more than BTREE_ORDER to force splits
        let count = BTREE_ORDER as u64 + 100; // exceed order
        for k in 0..count {
            let v = k * 3;
            idx.set(k, &v).unwrap();
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
        let mut idx = new_index();
        for k in 0..100u64 { idx.set(k, &k).unwrap(); }
        idx.remove(50).unwrap();
        match idx.get(50) {
            Err(Error::KeyNotFound) => {},
            _ => panic!("expected KeyNotFound after remove"),
        }
        // Ensure neighbors still readable
        assert_eq!(idx.get(49).unwrap(), 49);
        assert_eq!(idx.get(51).unwrap(), 51);
    }

    #[test]
    fn remove_underflow_borrow_or_merge_paths() {
        let mut idx = new_index();
        // Construct distribution to potentially trigger borrow/merge.
        // Insert enough keys to fill multiple nodes, then remove many to cause underflow.
        let total = (BTREE_ORDER as u64) * 3;
        for k in 0..total { idx.set(k, &k).unwrap(); }
        // Remove alternating keys to stress sibling logic
        for k in (0..total).step_by(2) {
            idx.remove(k).unwrap();
        }
        // Remaining odd keys should still be present
        for k in (1..total).step_by(2) {
            assert_eq!(idx.get(k).unwrap(), k);
        }
    }

    #[test]
    fn persist_does_not_panic() {
        let idx = new_index();
        idx.persist().expect("persist should succeed");
    }

    #[test]
    fn stress_randomized_small_range() {
        let mut idx = new_index();
        // Deterministic pseudo-random without external crates
        let mut seed: u64 = 0xDEADBEEFCAFEBABE;
        let mut keys = Vec::new();

        // Stopwatch
        let start = Instant::now();
        for _ in 0..131_072 {
            // xorshift64*
            seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
            let k = (seed % 16_384) as u64;
            let v = seed ^ 0xA5A5A5A5A5A5A5A5;
            idx.set(k, &v).unwrap();
            keys.push((k, v));
        }
        println!("Inserted 131072 entries in {:?}", start.elapsed());

        let start = Instant::now();
        // Verify latest values for each key
        use std::collections::HashMap;
        let mut last: HashMap<u64, u64> = HashMap::new();
        for (k, v) in keys { last.insert(k, v); }
        for (k, v) in last { assert_eq!(idx.get(k).unwrap(), v); }
        println!("Verified 131072 entries in {:?}", start.elapsed());
    }
}

