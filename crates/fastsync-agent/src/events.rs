use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::broadcast;
use uuid::Uuid;

pub const EVENT_CHANNEL_CAPACITY: usize = 256;

#[derive(Clone, Debug, Serialize)]
pub struct AppEvent {
    #[serde(rename = "type")]
    pub kind: EventKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<Uuid>,
    pub timestamp: i64,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    DeviceUpdated,
    JobUpdated,
    JobProgress,
}

impl AppEvent {
    pub fn device_updated(device_id: Uuid) -> Self {
        Self::new(EventKind::DeviceUpdated, Some(device_id), None)
    }

    pub fn job_updated(job_id: Uuid) -> Self {
        Self::new(EventKind::JobUpdated, None, Some(job_id))
    }

    pub fn job_progress(job_id: Uuid) -> Self {
        Self::new(EventKind::JobProgress, None, Some(job_id))
    }

    fn new(kind: EventKind, device_id: Option<Uuid>, job_id: Option<Uuid>) -> Self {
        Self {
            kind,
            device_id,
            job_id,
            timestamp: unix_millis(),
        }
    }
}

#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<AppEvent>,
}

impl EventBus {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AppEvent> {
        self.sender.subscribe()
    }

    pub fn send(&self, event: AppEvent) {
        let _ = self.sender.send(event);
    }
}

pub fn unix_millis() -> i64 {
    let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    let millis = elapsed.as_millis().min(i64::MAX as u128);
    millis as i64
}
