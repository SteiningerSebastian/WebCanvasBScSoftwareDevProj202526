pub mod file_key_value_store;
pub mod concurrent_file_key_value_store;
pub mod dns;
pub mod thread_pool;
pub mod persistent_random_access_memory;
pub mod write_ahead_log;
pub mod pram_btree_index;

#[cfg(test)]
mod persistent_random_access_memory_tests;

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn it_works() {
//         let result = add(2, 2);
//         assert_eq!(result, 4);
//     }
// }
