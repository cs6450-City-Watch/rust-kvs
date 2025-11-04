//! gRPC integration module for SomeTime service
//!
//! This module provides integration with the SomeTime distributed timestamp service,
//! including local time service implementation and timestamp conversion utilities.

use prost_types::Timestamp;
use std::time::SystemTime;
use tonic::{Request, Response, Status};

pub mod sometime {
    tonic::include_proto!("sometime");
}

use sometime::Interval;
use sometime::some_time_server::SomeTime;

/// Represents a SomeTime timestamp interval.
#[derive(Copy, Clone)]
pub struct SomeTimeTS {
    /// The earliest possible time this timestamp could represent
    pub earliest: SystemTime,
    /// The latest possible time this timestamp could represent
    pub latest: SystemTime,
}

/// Returns the current SomeTime timestamp.
/// TODO: Replace this with an actual SomeTime RPC call that provides proper uncertainty bounds.
pub fn now() -> SomeTimeTS {
    SomeTimeTS {
        earliest: SystemTime::now(),
        latest: SystemTime::now(),
    }
}

/// Localtime implementation of the SomeTime service.
#[derive(Default)]
pub struct LocalTimeService;

#[tonic::async_trait]
impl SomeTime for LocalTimeService {
    /// Returns the current timestamp as a SomeTime interval.
    async fn now(&self, _request: Request<Timestamp>) -> Result<Response<Interval>, Status> {
        let current_time = now();

        let earliest = Some(Timestamp::from(current_time.earliest));
        let latest = Some(Timestamp::from(current_time.latest));

        let interval = Interval { earliest, latest };
        Ok(Response::new(interval))
    }
}
