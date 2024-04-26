use crate::schedule::Schedule;

use tokio::sync::{Mutex, OwnedMutexGuard};

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct ScheduleManager {
    schedules: Arc<RwLock<HashMap<String, Schedule>>>,
    write_lock: Arc<Mutex<()>>,
}

impl ScheduleManager {
    pub fn new() -> Self {
        Self {
            schedules: Arc::new(RwLock::new(HashMap::new())),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    // could make this per-namespace in future
    pub async fn take_write_lock(&self) -> OwnedMutexGuard<()> {
        self.write_lock.clone().lock_owned().await
    }

    pub fn get_schedules(&self) -> Arc<RwLock<HashMap<String, Schedule>>> {
        self.schedules.clone()
    }
}
