// Rust sketch (place where you prefer)
use std::{collections::{BinaryHeap, HashMap, HashSet}, fmt::Display, slice::{IterMut}};

type Clock = u64;
const INF_SCORE: u64 = u64::MAX / 4;

#[derive(Debug)]
pub enum Error {
    NoEvictableSlot,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NoEvictableSlot => write!(f, "No Evictable Slot Available"),
        }
    }
}

pub struct Slot {
    pub dirty: bool,
    pub page_index: usize,
    epoch: u64, // increments when slot changes to invalidate heap entries
    history_len: usize,
    history: [Clock; 8], // newest at front, len <= K
    pub data: Option<Vec<u8>>,
}

impl Clone for Slot {
    fn clone(&self) -> Self {
        panic!("Slot clone should not be used, it is only implemented to satisfy trait bounds for Option::None().");
    }
}

struct HeapEntry {
    score: u64,
    epoch: u64,
    slot_id: usize,
}

// BinaryHeap by largest score first
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| self.epoch.cmp(&other.epoch))
            .then_with(|| self.slot_id.cmp(&other.slot_id))
    }
}
impl PartialOrd for HeapEntry { 
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(o)) } 
}

impl PartialEq for HeapEntry { 
    fn eq(&self, o: &Self) -> bool { 
        self.score==o.score && self.epoch==o.epoch && self.slot_id==o.slot_id 
    } 
}

impl Eq for HeapEntry {}

pub struct LruKCache {
    k: usize,
    pardon: usize,
    clock: Clock,
    slots: Vec<Option<Slot>>,
    heap: BinaryHeap<HeapEntry>, // lazy-updated
    pardoned: Vec<HeapEntry>,
    pages_in_cache: HashSet<usize>,
    free_slots: Vec<usize>,
    page_map: HashMap<usize, usize>, // page_index -> slot_id
}

impl LruKCache {
    /// Create a new LRU-K cache
    /// 
    /// Parameters:
    /// - `k`: The K parameter for LRU-K
    /// - `slot_count`: The number of slots in the cache
    /// - `pardon`: The pardon period during which pages with insufficient history are not evicted
    pub fn new(k: usize, slot_count: usize, pardon: usize) -> Self {
        assert!(slot_count > pardon && k >= 1, "slot_count must be greater than pardon and at least 1.");
        assert!(k <= 8, "k must be at most 8 due to SmallVec size.");

        Self {
            k,
            pardon,
            clock: 0,
            slots: vec![None; slot_count],
            heap: BinaryHeap::new(),
            pardoned: Vec::new(),
            pages_in_cache: HashSet::new(),
            free_slots: (0..slot_count).collect(), // all slots free initially
            page_map: HashMap::new(),
        }
    }

    /// Check if a page is in the cache
    /// 
    /// Parameters:
    /// - `page_index`: The index of the page to check
    /// 
    /// Returns:
    /// - `bool`: true if the page is in the cache, false otherwise
    pub fn contains_page(&self, page_index: usize) -> bool {
        self.pages_in_cache.contains(&page_index)
    }

    /// Store a page in the cache
    /// 
    /// Parameters:
    /// - `page_index`: The index of the page to store
    /// - `data`: The data of the page to store
    /// - `dirty`: Whether the page is dirty (modified)
    /// 
    /// Returns:
    /// - `Result<Option<Slot>, Error>`: Ok(Some(evicted_slot)) if a slot was evicted, Ok(None) if no eviction, Err(Error) on failure
    pub fn store_page(&mut self, page_index: usize, data: Vec<u8>, dirty: bool) -> Result<Option<Slot>, Error> {        
        // If page already in cache, hand back the data to it and update dirty flag.
        if self.contains_page(page_index) {
            // Page already in cache, update it
            let slot_id = *self.page_map.get(&page_index).unwrap();
            let slot = self.slots[slot_id].as_mut().unwrap();
            slot.data = Some(data); // Hand back the data.
            slot.dirty = dirty || slot.dirty; // once dirty always dirty until flushed
            return Ok(None); // no eviction
        }
       
        let mut evicted_slot: Option<Slot> = None;

        // Evict if needed
        if self.free_slots.is_empty() {
            if let Some(es) = self.evict_one() {
                evicted_slot = Some(es);
            } else {
                return Err(Error::NoEvictableSlot); // No slot could be evicted
            }
        }

        // Allocate a free slot
        let slot_id = self.free_slots.pop().unwrap();
        let slot = Slot {
            page_index,
            data: Some(data),
            dirty: dirty,
            history: [0; 8],
            epoch: 0,
            history_len: 0,
        };
        self.slots[slot_id] = Some(slot);

        // Record in page_map and pages_in_cache
        self.page_map.insert(page_index, slot_id);
        self.pages_in_cache.insert(page_index);

        // Touch the slot to initialize its history and score
        self.touch(slot_id);

        Ok(evicted_slot) // return evicted slot if any for the caller to handle
    }
        
    /// Get a page from cache, transferring its buffer to the caller and returning its dirty flag.
    /// After this call, the slot remains tracked but holds no data until the page is stored again.
    ///
    /// Parameters:
    /// - `page_index`: The index of the page to retrieve
    /// 
    /// Returns:
    /// - `Option<&mut Slot>`: Some(&mut Slot) if found, None otherwise
    pub fn fetch_page(&mut self, page_index: usize) -> Option<Vec<u8>> {
        if let Some(&slot_id) = self.page_map.get(&page_index) {
            // Touch the slot to update its history and score
            self.touch(slot_id);
            let slot = self.slots[slot_id].as_mut();
            if let Some(s) = slot {
                return s.data.take(); // return the data and remove it from the slot
                // Note: Caller must put it back after use using store_page, else data will be lost.
            }
        }
        None
    }

    /// Check pardoned slots to see if any can be reinserted into the eviction heap
    #[inline]
    fn check_pardons(&mut self) {
        for i in 0..self.pardoned.len() {
            let entry = &self.pardoned[i];
            if let Some(slot) = &self.slots[entry.slot_id] {
                // Check if pardon conditions are met, if not reinsert into heap
                if slot.history_len >= self.k || self.clock - slot.history[0] >= self.pardon as u64 {
                    // No longer pardoned, reinsert into heap
                    self.heap.push(HeapEntry { score: entry.score, epoch: entry.epoch, slot_id: entry.slot_id });

                    // Remove from pardoned list by swapping with last and popping
                    self.pardoned.swap_remove(i);
                }
            }
        }
    }

    /// Update the access history of a slot
    #[inline]
    fn update_history(k:usize, clock: u64, slot: &mut Slot) {
        // Shift history to the left - remove oldest if full
        slot.history[..k].rotate_left(1);
        slot.history[k-1] = clock;

        // Update history length
        if slot.history_len < k {
            slot.history_len += 1;
        }
    }

    /// Touch a slot (record an access)
    /// 
    /// Parameters:
    /// - `slot_id`: The ID of the slot being accessed
    #[inline]
    fn touch(&mut self, slot_id: usize) {
        // Increment clock and update slot history
        self.clock += 1;

        // Update the slot's history
        let s = self.slots[slot_id].as_mut().unwrap();
        Self::update_history(self.k, self.clock, s);

        // Compute new score
        let score = if s.history_len >= self.k {
            self.clock - s.history[self.k - 1]
        } else { 
            INF_SCORE 
        };

        s.epoch += 1;

        // Push new entry onto heap
        self.heap.push(HeapEntry { score, epoch: s.epoch, slot_id });
    }

    /// Find an eviction candidate without evicting
    /// 
    /// Parameters:
    /// - `page_index`: The index of the page to check
    /// 
    /// Returns:
    /// - `Option<&mut Slot>`: Some(&mut Slot) if an eviction candidate is found, None if no eviction expected
    pub fn eviction_candidate(&mut self, page_index: usize) -> Option<&mut Slot> {
        if self.contains_page(page_index) || self.free_slots.len() > 0 {
            None
        }else{
            self.check_pardons(); // Check pardoned slots first if they need to be reinserted

            // Remove stale heap entries until the top matches the current epoch.
            while let Some(top) = self.heap.peek() {
                if let Some(slot) = &self.slots[top.slot_id] {
                    if slot.epoch == top.epoch {
                        return Some(self.slots[top.slot_id].as_mut().unwrap()); // valid entry
                    } 
                }

                // Move to next entry - remove stale entry
                let _ = self.heap.pop();
            }

            None
        }
    }

    /// Evict one slot based on LRU-K policy
    /// 
    /// Returns:
    /// - `Option<Slot>`: The ID of the evicted slot, or None if no valid slot found
    #[inline]
    fn evict_one(&mut self) -> Option<Slot> {
        self.check_pardons(); // Check pardoned slots first if they need to be reinserted

        while let Some(top) = self.heap.pop() {
            if let Some(slot) = &self.slots[top.slot_id] {
                // For stall entries, we won't return them just remove them from the heap.
                if slot.epoch == top.epoch {
                    // valid entry
                    let evict_id = top.slot_id;
                    self.free_slots.push(evict_id); // mark slot as free

                    // Remove from page_map
                    self.page_map.remove(&self.slots[evict_id].as_ref().unwrap().page_index);
                    self.pages_in_cache.remove(&self.slots[evict_id].as_ref().unwrap().page_index);

                    return self.slots[evict_id].take(); // remove and return the slot
                }
            }
            // else stale entry -> continue popping
        }

        // No evictable slot found
        None
    }

    pub fn iter_mut(&'_ mut  self) -> IterMut<'_, Option<Slot>> {
       self.slots.iter_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(n: u8, len: usize) -> Vec<u8> {
        std::iter::repeat(n).take(len).collect()
    }

    #[test]
    fn store_and_contains_without_eviction() {
        let mut cache = LruKCache::new(2, 2, 1);

        assert!(!cache.contains_page(1));
        let ev = cache.store_page(1, bytes(1, 3), false).expect("store ok");
        assert!(ev.is_none());
        assert!(cache.contains_page(1));

        let ev = cache.store_page(2, bytes(2, 3), false).expect("store ok");
        assert!(ev.is_none());
        assert!(cache.contains_page(2));
    }

    #[test]
    fn update_existing_page_updates_data_and_dirty() {
        let mut cache = LruKCache::new(2, 2, 1);

        cache.store_page(1, bytes(1, 2), false).expect("store ok");
        // Update same page with new data and dirty=true
        cache
            .store_page(1, bytes(9, 4), true)
            .expect("update ok");

        let slot_id = *cache.page_map.get(&1).expect("slot exists");
        let slot = cache.slots[slot_id].as_ref().expect("slot present");
        assert_eq!(slot.data.as_ref().unwrap(), &bytes(9, 4));
        assert!(slot.dirty, "dirty flag should be updated on existing page");
    }

    #[test]
    fn eviction_happens_when_full_and_returns_evicted_slot() {
        let mut cache = LruKCache::new(2, 2, 1);

        cache.store_page(1, bytes(1, 1), false).expect("store ok");
        assert!(cache.contains_page(1));

        cache.store_page(2, bytes(1, 1), false).expect("store ok");
        assert!(cache.contains_page(2));

        let ev = cache
            .store_page(3, bytes(2, 1), false)
            .expect("store with eviction ok");

        let ev = ev.expect("should evict one slot");
        assert_eq!(ev.page_index, 1, "oldest/worst page should be evicted");
        assert!(!cache.contains_page(1));
        assert!(cache.contains_page(2));
    }

    #[test]
    fn eviction_candidate_is_none_when_free_slots_available() {
        let mut cache = LruKCache::new(2, 2, 1);
        cache.store_page(1, bytes(1, 1), false).expect("store ok");

        // There is still at least one free slot, so no eviction expected
        assert!(
            cache.eviction_candidate(2).is_none(),
            "no eviction candidate expected when free slots exist"
        );
    }

    #[test]
    fn repeated_store_does_not_allocate_new_slot() {
        let mut cache = LruKCache::new(2, 2, 1);

        cache.store_page(1, bytes(1, 2), false).expect("store ok");
        let free_before = cache.free_slots.len();

        // Store same page again, should not need another slot nor evict
        let ev = cache
            .store_page(1, bytes(2, 2), true)
            .expect("update ok");
        assert!(ev.is_none());
        assert_eq!(
            cache.free_slots.len(),
            free_before,
            "updating an existing page must not consume or free slots"
        );

        let slot_id = *cache.page_map.get(&1).expect("slot exists");
        let slot = cache.slots[slot_id].as_ref().expect("slot present");
        assert_eq!(slot.data.as_ref().unwrap(), &bytes(2, 2));
        assert!(slot.dirty);
    }
}