use prost_types::Timestamp;
use std::time::SystemTime;
use tonic::{Request, Response, Status};

pub mod sometime {
    tonic::include_proto!("sometime");
}

use sometime::Interval;
use sometime::some_time_server::SomeTime;

#[derive(Copy, Clone)]
pub struct SomeTimeTS {
    pub earliest: SystemTime,
    pub latest: SystemTime,
}

/// TODO: replace this with an actual ST RPC. Should of course reflect our semantics on what earliest and latest are.
pub fn now() -> SomeTimeTS {
    SomeTimeTS {
        earliest: SystemTime::now(),
        latest: SystemTime::now(),
    }
}

#[derive(Default)]
pub struct LocalTimeService;

#[tonic::async_trait]
impl SomeTime for LocalTimeService {
    async fn now(&self, _request: Request<Timestamp>) -> Result<Response<Interval>, Status> {
        let current_time = now();

        let earliest = Some(Timestamp::from(current_time.earliest));
        let latest = Some(Timestamp::from(current_time.latest));

        let interval = Interval { earliest, latest };
        Ok(Response::new(interval))
    }
}
