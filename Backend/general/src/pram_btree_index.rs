use std::{fmt::Display, rc::Rc, u64};

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
    fn set(&mut self, key: u64, value: u64) -> Result<(), Error>;

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

// The order of the B-tree (maximum number of children per node) Should be odd for simplicity.
const BTREE_ORDER: usize = 5; // 248 Assuming 16kb pages and u64 (8 bytes) for keys and values (16 bytes total per entry) + some overhead for length, is_leaf and padding
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
        let unsafe_node: Option<&BTreeNode> = node_ptr.unsafe_deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?; // For debugging, just deref without caching.

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
            // Last element is used to point to any elements greater than the last key.
            let index = node.keys.as_slice()[0..node.length-1].iter().position(|e| *e >= key).unwrap_or(BTREE_ORDER-1);        

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
    fn insert_or_update_recursive(&mut self, key: u64, value: u64, node: &mut Box<BTreeNode>, parent_key: u64) -> Result<Option<(u64, Pointer)>, Error> {
        if node.is_leaf {
            // Search for the correct position to insert the new key
            let index = node.keys.as_slice()[0..node.length].iter().position(|e| *e >= key);

            // If key already exists, update the value
            if let Some(index) = index {
                if node.keys[index] == key { // key exist and mode is not insert!
                    node.values[index] = value;

                    return Ok(None); // No split needed
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

                Ok(None) // No split needed
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
                let mut right_node_ptr = self.pram.malloc(std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;
                right_node_ptr.write(&right_node).map_err(Error::UnspecifiedMemoryError)?;

                // Update the pointer of the original node to point to the new node
                node.values[BTREE_ORDER - 1] = right_node_ptr.pointer;

                // The median key is actually not in the new node, it's the first key of the original node
                return Ok(Some((median_key, right_node_ptr))); // Return median key and new node pointer
            }
        } else {
            // Search for the correct position to insert the new key
            let opt_index = node.keys.as_slice()[0..node.length-1].iter().position(|e| *e >= key);

            // find the child pointer to follow
            // Last element is used to point to any elements greater than the last key.
            let index: usize = opt_index.unwrap_or(BTREE_ORDER-1); 
            // If none found, its the rightmost element, so it contains all keys up to what this parent node contains.
            let child_key = node.keys.get(index).cloned().unwrap_or(parent_key);

            let mut child_ptr = Pointer::from_address(node.values[index],self.pram.clone());
            // The caller that dereferences the Node is responsible to write the value back.
            let mut child = child_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

            let split_result = self.insert_or_update_recursive(key, value, &mut child, child_key)?;

            child_ptr.write(child.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

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
                        node.values[BTREE_ORDER - 1] = right_child_ptr.pointer;
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
                        node.values[index+1] = right_child_ptr.pointer;
                        node.length += 1;
                    }

                    return Ok(None); // No split needed
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
                            node.values[BTREE_ORDER - 1] = right_child_ptr.pointer;
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
                            node.values[index+1] = right_child_ptr.pointer;
                            node.length += 1;
                        }
                    } else {
                        if index == BTREE_ORDER - 1 {
                            // move the last pointer to the new position
                            right_node.values[right_node.length-1] = right_node.values[BTREE_ORDER - 1];

                            // it contains all values smaller than the new marker
                            right_node.keys[right_node.length-1] = left_child_key;
                            
                            // the new last pointer is the new right child
                            right_node.values[BTREE_ORDER - 1] = right_child_ptr.pointer;
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
                            right_node.values[projected_index+1] = right_child_ptr.pointer;
                            right_node.length += 1;
                        }
                    }

                    // Write back the updated nodes
                    let mut right_node_ptr = self.pram.malloc(std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;
                    right_node_ptr.write(&right_node).map_err(Error::UnspecifiedMemoryError)?;

                    return Ok(Some((median_key, right_node_ptr))); // Return median key and new node pointer
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
    fn insert_or_update(&mut self, key: u64, value: u64) -> Result<(), Error> {
        let mut root = self.root.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

        let split = self.insert_or_update_recursive(key, value, &mut root, u64::MAX)?;

        if let Some((median_key, new_right_ptr)) = split {
            let mut left_child_ptr = self.pram.malloc(std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;

            // Need to create a new root
            let mut new_root = BTreeNode {
                is_leaf: false,
                keys: [0; BTREE_ORDER - 1],
                values: [0; BTREE_ORDER],
                length: 2,
            };
            new_root.keys[0] = median_key;
            new_root.values[0] = left_child_ptr.pointer;
            new_root.values[BTREE_ORDER-1] = new_right_ptr.pointer; // For any elements greater than the available keys, search the last position of the array.

            // Write the root node
            left_child_ptr.write(root.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

            // Write the new root node to the PRAM
            self.root.write(&new_root).map_err(Error::UnspecifiedMemoryError)?;
        } else {
            // Write back the updated root
            self.root.write(root.as_ref()).map_err(Error::UnspecifiedMemoryError)?;
        }

        Ok(())
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
    fn try_borrow_left_sibling(&mut self, node: &mut Box<BTreeNode>, parent_node: &mut Box<BTreeNode>, node_index: usize) -> Result<bool, Error> {
        if node_index == 0 {
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
        let mut left_sibling_ptr = Pointer::from_address(parent_node.values[left_node_index], self.pram.clone());
        let mut left_sibling = left_sibling_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

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
            left_sibling_ptr.write(left_sibling.as_ref()).map_err(Error::UnspecifiedMemoryError)?;
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
    fn try_borrow_right_sibling(&mut self, node: &mut Box<BTreeNode>, parent_node: &mut Box<BTreeNode>, node_index: usize) -> Result<bool, Error> {
        if node_index + 1 >= BTREE_ORDER {
            return Ok(false); // No right sibling
        }

        let right_sibling_index = if node_index + 2 >= parent_node.length {
            BTREE_ORDER - 1
        } else {
            node_index + 1
        };

        // Try right sibling
        let mut right_sibling_ptr = Pointer::from_address(parent_node.values[right_sibling_index], self.pram.clone());
        let mut right_sibling = right_sibling_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

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
            right_sibling_ptr.write(right_sibling.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

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
    fn merge_left_sibling(&mut self, node: &mut Box<BTreeNode>, parent_node: &mut Box<BTreeNode>, node_index: usize) -> Result<bool, Error> {
        if node_index == 0 {
            return Ok(false); // No left sibling
        }

        let left_sibling_index = if node_index == BTREE_ORDER-1 {
            parent_node.length - 2 
        } else { 
            node_index - 1 
        };

        let mut left_sibling_ptr = Pointer::from_address(parent_node.values[left_sibling_index], self.pram.clone());
        let mut left_sibling = left_sibling_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;
        
        Self::merge_right_sibling_inner(&mut left_sibling, node, parent_node, left_sibling_index, node_index)?;

        left_sibling_ptr.write(left_sibling.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

        // The node is no longer used.
        let note_ptr = Pointer::from_address(parent_node.values[node_index], self.pram.clone());

        // Free the memory of the right sibling
        self.pram.free(note_ptr,  std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;

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
    fn merge_right_sibling_inner(node: &mut Box<BTreeNode>, right_sibling: &Box<BTreeNode>, parent_node: &mut Box<BTreeNode>, node_index: usize, right_sibling_index: usize) -> Result<bool, Error> {
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
    fn merge_right_sibling(&mut self, node: &mut Box<BTreeNode>, parent_node: &mut Box<BTreeNode>, node_index: usize) -> Result<bool, Error> {
        if node_index >= parent_node.length -1 || node_index + 1 >= BTREE_ORDER {
            return Ok(false); // No right sibling
        }

        let right_sibling_index = if node_index == parent_node.length - 2 {
            BTREE_ORDER - 1
        } else { 
            node_index + 1 
        };

        let right_sibling_ptr = Pointer::from_address(parent_node.values[right_sibling_index], self.pram.clone());
        let right_sibling = right_sibling_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;
        
        Self::merge_right_sibling_inner(node, &right_sibling, parent_node, node_index, right_sibling_index)?;

        // Free the memory of the right sibling
        self.pram.free(right_sibling_ptr.clone(),  std::mem::size_of::<BTreeNode>()).map_err(Error::UnspecifiedMemoryError)?;

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
    fn remove_recursive(&mut self, key: u64, node: &mut Box<BTreeNode>, parent: Option<&mut Box<BTreeNode>>, node_index: Option<usize>) -> Result<u64, Error> {        
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
            // Check if we need to handle the overflow
            if node.length < BTREE_ORDER/2 && parent.is_some() { 
                // Either borrow from sibling or merge with sibling
                // We can unwrap here because we checked for parent being None above.
                let parent = parent.unwrap();
                let node_index = node_index.unwrap();

                if !self.try_borrow_right_sibling(node, parent, node_index)? { // Successfully borrowed from right sibling
                    if !self.try_borrow_left_sibling(node, parent, node_index)? { // Successfully borrowed from left sibling
                        // If neither sibling can lend a key, we need to handle underflow by merging nodes, this may lead to a underflow in the parent node which must be handled
                        if !self.merge_right_sibling(node, parent, node_index)? {
                            if !self.merge_left_sibling(node, parent, node_index)? {
                                panic!("Underflow could not be resolved!");
                            }
                        }
                        // can't merge with left, no need to check right as we already tried to borrow from both sides.
                    }
                };
            }

            return Ok(value);
        } else {
            let correct_index = node.keys.as_slice()[0..node.length-1].iter().position(|e| *e >= key);
            let index = if correct_index.is_none() {
                BTREE_ORDER -1
            } else {
                correct_index.unwrap()
            };

            let mut child_ptr = Pointer::from_address(node.values[index],self.pram.clone());
            let mut child = child_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;

            // Recur into child node
            let result  = self.remove_recursive(key, &mut child, Some(node), Some(index))?;

            // Write back the updated child node
            // The child may be freed inside the recursive callstack, the problem?
            // If it is freed, while we may still write it back, it may override an newly dynamic allocated
            // memory and behaviour becomes undefined, as we don't use dynamically allocation between freeing and
            // this write - it is generally considered safe.
            child_ptr.write(child.as_ref()).map_err(Error::UnspecifiedMemoryError)?;

            if node.length < BTREE_ORDER / 2 {
                // Handle underflow in internal nodes if needed
                // This part is left unimplemented for brevity
                let parent = parent.unwrap();
                let node_index = node_index.unwrap();

                 if !self.try_borrow_right_sibling(node, parent, node_index)? { // Successfully borrowed from right sibling
                    if !self.try_borrow_left_sibling(node, parent, node_index)? { // Successfully borrowed from left sibling
                        // If neither sibling can lend a key, we need to handle underflow by merging nodes, this may lead to a underflow in the parent node which must be handled
                        if !self.merge_right_sibling(node, parent, node_index)? {
                            if !self.merge_left_sibling(node, parent, node_index)? {
                                panic!("Underflow could not be resolved!");
                            }
                        }
                        // can't merge with left, no need to check right as we already tried to borrow from both sides.
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
    fn remove(&mut self, key: u64) -> Result<u64, Error> {
        let mut root = self.root.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError)?;
        let res = self.remove_recursive(key, &mut root, None, None)?;
        self.root.write(root.as_ref()).map_err(Error::UnspecifiedMemoryError)?;
        Ok(res)
    }

    /// Recursive helper function to create an in-memory copy of the B-tree.
    /// 
    /// Parameters:
    /// - `node`: The current node being processed.
    fn _reslove_recursive(&mut self, node: &mut BTreeMemCopyNode) {
        if node.is_leaf {
            return;
        }
        for i in 0..node.length {
            let child_i = if i +1 == node.length{
                (BTREE_ORDER -1) as usize
            } else { i };

            let child_ptr = Pointer::from_address(node.values[child_i], self.pram.clone());
            let child_node = child_ptr.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError).unwrap();

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
    fn get_mem_copy(&mut self) -> BTreeMemCopyNode {
        let root = self.root.deref::<BTreeNode>().map_err(Error::UnspecifiedMemoryError).unwrap();

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
    fn print_to_string(&mut self) -> String {
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

impl BTreeIndex for BTreeIndexPRAM {
    fn set(&mut self, key: u64, value: u64) -> Result<(), Error> {
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

    fn make_pram(id: &str) -> Rc<dyn PersistentRandomAccessMemory> {
        // 4 MiB total, 4 KiB pages, moderate LRU settings
        let path = temp_path(&format!("pram_test{}", id));
        FilePersistentRandomAccessMemory::new(16 * 1024 * 1024, &path, 4 * 1024, 64, 2, 8)
    }

    fn new_index(id: &str) -> BTreeIndexPRAM {
        let pram = make_pram(id);
        BTreeIndexPRAM::new(pram)
    }

    #[test]
    fn set_and_get_single_key() {
        let mut idx = new_index("set_and_get_single_key");
        let key = 42u64;
        let val = 777u64;
        idx.set(key, val).expect("set should succeed");
        let got = idx.get(key).expect("get should return value");
        assert_eq!(got, val);
    }

    #[test]
    fn update_existing_key() {
        let mut idx = new_index("update_existing_key");
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
        let mut idx = new_index("many_inserts_cause_splits_and_remain_searchable");
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
        let mut idx = new_index("remove_existing_key");
        for k in 0..100u64 { idx.set(k, k).unwrap(); }
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
        let mut idx = new_index("remove_underflow_borrow_or_merge_paths");
        // Construct distribution to potentially trigger borrow/merge.
        // Insert enough keys to fill multiple nodes, then remove many to cause underflow.
        let total = (BTREE_ORDER as u64)*16+100;
        for k in 0..total { idx.set(k, k).unwrap(); }

        println!("{}", idx.print_to_string());

        // Remove alternating keys to stress sibling logic
        for r in (0..total).step_by(2) {
            idx.remove(r).unwrap();
            println!("{}", idx.print_to_string());
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
        let mut idx = new_index("stress_randomized_small_range");
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

        let start = Instant::now();
        // Verify latest values for each key
        use std::collections::HashMap;
        let mut last: HashMap<u64, u64> = HashMap::new();
        for (k, v) in keys { last.insert(k, v); }
        for (k, v) in last { assert_eq!(idx.get(k).unwrap(), v); }
        println!("Verified 100k entries in {:?}", start.elapsed());
    }
}