use noredb::database_server::{Database};
use noredb::{Data, DataRequest, DataResponse, Commit, Empty};
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;

use crate::canvasdb::{CanvasDB, CanvasDBTrait, Pixel, PixelEntry, TimeStamp};

use tracing::{warn};

pub mod noredb {
    tonic::include_proto!("noredb");
}

pub struct MyDatabaseServer {
    db: CanvasDB,
}

impl MyDatabaseServer {
    pub fn new(db: CanvasDB) -> Self {
        MyDatabaseServer {
            db
        }
    }
}

#[tonic::async_trait]
impl Database for MyDatabaseServer {
    // associated stream type for get_all_keys
    type GetAllStream = ReceiverStream<Result<DataResponse, tonic::Status>>;

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

    async fn get_all(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Self::GetAllStream>, tonic::Status> {
        // Return an empty stream for now; populate with real keys if CanvasDB provides an iterator.
        let (tx, rx) = mpsc::channel(4);

        let mut iter = self.db.iterate_pixels().map_err(|_| {
            tonic::Status::internal("Failed to create pixel iterator")
        })?;
        
        tokio::spawn(async move {
            while let Some(pixel) = iter.next() {
                let resp = DataResponse {
                    index: 0, // index is not relevant here
                    key: pixel.key,
                    value: pixel.color.to_vec(), 
                };
                if let Err(_) = tx.send(Ok(resp)).await {
                    // Receiver dropped
                    break;
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(stream))
    }
}