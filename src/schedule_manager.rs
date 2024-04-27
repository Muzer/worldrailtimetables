use crate::schedule::Schedule;

use tokio::sync::{Mutex, OwnedMutexGuard};

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct ImmediateWriter<'a> {
    schedules: RwLockWriteGuard<'a, HashMap<String, Schedule>>,
    _transaction_lock: OwnedMutexGuard<()>,
}

impl Deref for ImmediateWriter<'_> {
    type Target = HashMap<String, Schedule>;

    fn deref(&self) -> &Self::Target {
        &self.schedules
    }
}

impl DerefMut for ImmediateWriter<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.schedules
    }
}

pub struct TransactionalWriter {
    new_schedules: HashMap<String, Schedule>,
    schedules_ref: Arc<RwLock<HashMap<String, Schedule>>>,
    _transaction_lock: OwnedMutexGuard<()>,
}

impl Deref for TransactionalWriter {
    type Target = HashMap<String, Schedule>;

    fn deref(&self) -> &Self::Target {
        &self.new_schedules
    }
}

impl DerefMut for TransactionalWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.new_schedules
    }
}

impl TransactionalWriter {
    pub fn commit(self) {
        let mut schedules = self.schedules_ref.write().unwrap();
        *schedules = self.new_schedules
    }
}

pub struct ScheduleManager {
    schedules: Arc<RwLock<HashMap<String, Schedule>>>,
    transaction_lock: Arc<Mutex<()>>,
}

impl ScheduleManager {
    pub fn new() -> Self {
        Self {
            schedules: Arc::new(RwLock::new(HashMap::new())),
            transaction_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<HashMap<String, Schedule>> {
        self.schedules.read().unwrap()
    }

    pub async fn immediate_write<'a>(&'a self) -> ImmediateWriter<'a> {
        let trans_lock = self.transaction_lock.clone().lock_owned().await;

        ImmediateWriter {
            schedules: self.schedules.write().unwrap(),
            _transaction_lock: trans_lock,
        }
    }

    pub async fn transactional_write(&self) -> TransactionalWriter {
        let trans_lock = self.transaction_lock.clone().lock_owned().await;

        let schedules = self.schedules.read().unwrap();

        TransactionalWriter {
            new_schedules: schedules.clone(),
            schedules_ref: self.schedules.clone(),
            _transaction_lock: trans_lock,
        }
    }
}
