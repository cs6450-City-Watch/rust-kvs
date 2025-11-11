//! gRPC integration module for SomeTime service
//!
//! This module provides integration with the SomeTime distributed timestamp service,
//! including local time service implementation and timestamp conversion utilities.

use prost_types::Timestamp;
use std::sync::OnceLock;
use std::time::SystemTime;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

pub mod sometime {
    tonic::include_proto!("sometime");
}

use sometime::Interval;
use sometime::some_time_client::SomeTimeClient;
use sometime::some_time_server::SomeTime;

/// Cached client connection to the SomeTime service
/// `Channel`s are ultimately `tower::Buffer`s,
/// which can be safely cloned and shared without additional overhead.
static SOMETIME_CLIENT: OnceLock<SomeTimeClient<Channel>> = OnceLock::new();

/// Gets or creates a connection to the SomeTime service
async fn get_sometime_client()
-> Result<&'static SomeTimeClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(client) = SOMETIME_CLIENT.get() {
        return Ok(client);
    }

    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    let client = SomeTimeClient::new(channel);

    let _ = SOMETIME_CLIENT.set(client); // One thread will set it; others will fail silently.
    Ok(SOMETIME_CLIENT.get().unwrap())
}

/// Represents a SomeTime timestamp interval.
#[derive(Copy, Clone)]
pub struct SomeTimeTS {
    /// The earliest possible time this timestamp could represent
    pub earliest: SystemTime,
    /// The latest possible time this timestamp could represent
    pub latest: SystemTime,
}

/// Returns the current SomeTime timestamp.
/// Makes an RPC call to the SomeTime service to get proper uncertainty bounds.
pub async fn now() -> SomeTimeTS {
    match get_sometime_client().await {
        Ok(client) => {
            // see `SOMETIME_CLIENT` doc comment.
            match client.clone().now(()).await {
                Ok(response) => {
                    let interval = response.into_inner();

                    // Convert protobuf timestamps to SystemTime
                    let earliest = interval
                        .earliest
                        .and_then(|ts| SystemTime::try_from(ts).ok())
                        .unwrap_or(SystemTime::now());

                    let latest = interval
                        .latest
                        .and_then(|ts| SystemTime::try_from(ts).ok())
                        .unwrap_or(earliest + std::time::Duration::from_secs(1)); // default uncertainty on error cases

                    SomeTimeTS { earliest, latest }
                }
                Err(_) => {
                    // Fall back to local time with default uncertainty
                    let current_time = SystemTime::now();
                    SomeTimeTS {
                        earliest: current_time,
                        latest: current_time + std::time::Duration::from_secs(1),
                    }
                }
            }
        }
        Err(_) => {
            // Fall back to local time with default uncertainty
            // println!("Failed to reach SomeTime service. Make sure SomeTime is running. Falling back to default uncertainty");

            let current_time = SystemTime::now();
            SomeTimeTS {
                earliest: current_time,
                latest: current_time + std::time::Duration::from_secs(1),
            }
        }
    }
}

/// Localtime implementation of the SomeTime service.
#[derive(Default)]
pub struct LocalTimeService;

#[tonic::async_trait]
impl SomeTime for LocalTimeService {
    /// Returns the current timestamp as a SomeTime interval.
    async fn now(&self, _request: Request<()>) -> Result<Response<Interval>, Status> {
        let current_time = now().await;

        let earliest = Some(Timestamp::from(current_time.earliest));
        let latest = Some(Timestamp::from(current_time.latest));

        let interval = Interval { earliest, latest };
        Ok(Response::new(interval))
    }
}
