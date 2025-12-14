use std::sync::Arc;

use general::concurrent_file_key_value_store::ConcurrentFileKeyValueStore;
use general::persistent_random_access_memory::PersistentRandomAccessMemory;
use general::pram_btree_index::{BTreeIndex, BTreeIndexPRAM};
use general::write_ahead_log::{self, WriteAheadLog};
use noredb::database_server::{Database};
use noredb::{Data, DataRequest, DataResponse, GraveStone, Commit};

pub mod noredb {
    tonic::include_proto!("noredb");
}

pub struct MyDatabaseServer {
    config : ConcurrentFileKeyValueStore,
}

impl MyDatabaseServer {
    pub fn new(config: ConcurrentFileKeyValueStore) -> Self {
        MyDatabaseServer {
            config
        }
    }
}

#[tonic::async_trait]
impl Database for MyDatabaseServer {
    // Implement NoReDB service methods here
    async fn set(
        &self,
        request: tonic::Request<Data>,
    ) -> Result<tonic::Response<Commit>, tonic::Status> {
        let data = request.into_inner();
        // TODO: persist `data`
        let resp = Commit {
            index: data.index,
            status: 1, // success
        };
        Ok(tonic::Response::new(resp))
    }

    async fn force_set(
        &self,
        request: tonic::Request<Data>,
    ) -> Result<tonic::Response<Commit>, tonic::Status> {
        let data = request.into_inner();
        // TODO: forcefully persist `data` (override tombstones/entries)
        let resp = Commit {
            index: data.index,
            status: 1, // success
        };
        Ok(tonic::Response::new(resp))
    }

    async fn remove(
        &self,
        request: tonic::Request<GraveStone>,
    ) -> Result<tonic::Response<DataResponse>, tonic::Status> {
        let gs = request.into_inner();
        // TODO: place gravestone for key and return previous value if any
        let resp = DataResponse {
            index: gs.index,
            key: gs.key,
            value: Vec::new(), // empty by default
        };
        Ok(tonic::Response::new(resp))
    }

    async fn get(
        &self,
        request: tonic::Request<DataRequest>,
    ) -> Result<tonic::Response<DataResponse>, tonic::Status> {
        let req = request.into_inner();
        // TODO: look up the entry by req.key and req.index
        let resp = DataResponse {
            index: req.index,
            key: req.key,
            value: Vec::new(), // return actual value if found
        };
        Ok(tonic::Response::new(resp))
    }
}