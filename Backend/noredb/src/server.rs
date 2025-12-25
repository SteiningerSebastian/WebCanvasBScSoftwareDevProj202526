use general::concurrent_file_key_value_store::ConcurrentFileKeyValueStore;
use noredb::database_server::{Database};
use noredb::{Data, DataRequest, DataResponse, GraveStone, Commit};

use crate::canvasdb::{CanvasDB, CanvasDBTrait, Pixel, PixelEntry, TimeStamp};

use tracing::{error, warn, debug};

pub mod noredb {
    tonic::include_proto!("noredb");
}

pub struct MyDatabaseServer {
    config : ConcurrentFileKeyValueStore,
    db: CanvasDB,
}

impl MyDatabaseServer {
    pub fn new(config: ConcurrentFileKeyValueStore, db: CanvasDB) -> Self {
        MyDatabaseServer {
            config,
            db
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
        let data: Data = request.into_inner();

        // r,g,b values
        let mut color = [0u8; 3];
        color.copy_from_slice(&data.value[..3]);

        // The timestamp of this entry as u128.
        let mut time = [0u8; 16];
        time.copy_from_slice(&data.timestamp[..16]);

        let pixel = PixelEntry {
            pixel: Pixel {
                key: data.key,
                color: color,
            },
            timestamp: TimeStamp {
                bytes: time
            },
        };

        let wait = tokio::sync::oneshot::channel();
        
        self.db.set_pixel(pixel, Some(Box::new(|| {
                wait.0.send(()).unwrap();
        })));

        // Wait for the pixel to be permanently stored
        let res = wait.1.await;

        let resp =match res {
            Ok(_) => {
                Commit {
                    index: data.index,
                    status: 0b0001, // success
                }
            }
            Err(e) => {
                warn!("Failed to persist pixel data: {:?}", e);
                Commit {
                    index: data.index,
                    status: 0b0010, // success
                }
            }
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
        
        let pixel_option = self.db.get_pixel(req.key);
        if let Some(pixel_entry) = pixel_option {
            let mut value = Vec::new();
            value.extend_from_slice(&pixel_entry.0.color);

            let resp = DataResponse {
                index: req.index,
                key: req.key,
                value, // return actual value if found
            };
            return Ok(tonic::Response::new(resp));
        }

        let resp = DataResponse {
            index: req.index,
            key: req.key,
            value: Vec::new(), // return actual value if found
        };
        Ok(tonic::Response::new(resp))
    }
}