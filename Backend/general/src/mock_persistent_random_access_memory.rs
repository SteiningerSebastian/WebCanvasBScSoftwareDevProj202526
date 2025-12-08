use std::{cell::RefCell, collections::HashMap,  rc::{Rc, Weak}};

use crate::persistent_random_access_memory::{PersistentRandomAccessMemory, Pointer};

static mut ADDR: u64 = 0;

pub struct MockPRAM {
    me: std::rc::Weak<MockPRAM>,
    map: RefCell<HashMap<u64, Vec<u8>>>,
}

impl MockPRAM {
    pub fn new() -> Rc<Self> {
        let me = Self {
            me: Weak::new(),
            map: RefCell::new(HashMap::new()),    
        };

        let rc_me = Rc::new(me);

        let weak_me = Rc::downgrade(&rc_me);

        let ptr = Rc::into_raw(rc_me);

        unsafe {
            let mut_ref = (ptr as *mut Self).as_mut().unwrap();
            mut_ref.me = weak_me;
            Rc::from_raw(ptr)
        }
    }
}

impl PersistentRandomAccessMemory for MockPRAM {
    fn malloc(&self, len: usize) -> Result<crate::persistent_random_access_memory::Pointer, crate::persistent_random_access_memory::Error> {
        let my_addr = unsafe {
            ADDR += 1;
            ADDR
        };
        self.map.borrow_mut().insert(my_addr, vec![0u8; len]);
        return Ok(Pointer::from_address(my_addr, self.me.upgrade().unwrap()));
    }

    fn smalloc(&self, pointer: u64, len: usize) -> Result<crate::persistent_random_access_memory::Pointer, crate::persistent_random_access_memory::Error> {
        self.map.borrow_mut().insert(pointer, vec![0u8; len]);
        return Ok(Pointer::from_address(pointer, self.me.upgrade().unwrap()));
    }

    #[allow(unused_variables)]
    fn free(&self, pointer: crate::persistent_random_access_memory::Pointer, len: usize) -> Result<(), crate::persistent_random_access_memory::Error> {
        self.map.borrow_mut().remove(&pointer.address);
        Ok(())
    }

    fn persist(&self) -> Result<(), crate::persistent_random_access_memory::Error> {
        Ok(())
    }

    fn read(&self, pointer: u64, buf: &mut [u8]) -> Result<(), crate::persistent_random_access_memory::Error> {
        let map = self.map.borrow();
        let data = map.get(&pointer).ok_or(crate::persistent_random_access_memory::Error::ReadError)?;
        buf.copy_from_slice(&data[..buf.len()]);
        Ok(())
    }

    fn unsafe_read(&self, pointer: u64, len: usize) -> Result<&[u8], crate::persistent_random_access_memory::Error> {
        let map = self.map.borrow();
        let data = map.get(&pointer).ok_or(crate::persistent_random_access_memory::Error::ReadError)?;

        unsafe {
            Ok(std::slice::from_raw_parts(data.as_ptr(), len))
        }
    }

    fn unsafe_read_mut(&self, pointer: u64, len: usize) -> Result<&mut [u8], crate::persistent_random_access_memory::Error> {
        let mut map = self.map.borrow_mut();
        let data = map.get_mut(&pointer).ok_or(crate::persistent_random_access_memory::Error::ReadError)?;
        unsafe {
            Ok(std::slice::from_raw_parts_mut(data.as_ptr() as *mut u8, len))
        }
    }

    fn write(&self, pointer: u64, buf: &[u8]) -> Result<(), crate::persistent_random_access_memory::Error> {
        let mut map = self.map.borrow_mut();
        let data = map.get_mut(&pointer).ok_or(crate::persistent_random_access_memory::Error::WriteError)?;
        data[..buf.len()].copy_from_slice(buf);
        Ok(())
    }
}