use crate::error::Error;
use crate::schedule::{Schedule, Train};

use serde::Deserialize;
use tokio::fs;
use tokio::sync::{Mutex, OwnedMutexGuard};

use std::collections::{HashMap, HashSet};
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

#[derive(Clone, Deserialize)]
pub struct AssociatedLocation {
    pub namespace: String,
    pub id: Option<String>,
    pub public_id: Option<String>,
}

#[derive(Clone, Deserialize)]
pub struct LocationAssociation {
    pub associated_locations: Vec<AssociatedLocation>,
}

#[derive(Default)]
pub struct ScheduleManager {
    schedules: Arc<RwLock<HashMap<String, Schedule>>>,
    transaction_lock: Arc<Mutex<()>>,
    pub location_associations_by_id: HashMap<String, LocationAssociation>,
    pub location_associations_by_public_id: HashMap<String, LocationAssociation>,
}

impl ScheduleManager {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub async fn load_location_associations(&mut self) -> Result<(), Error> {
        match fs::read_to_string("location_associations.json").await {
            Ok(contents) => {
                let location_associations
                    = serde_json::from_str::<Vec<LocationAssociation>>(&contents)?;
                for location_association in location_associations {
                    for location in &location_association.associated_locations {
                        match &location.public_id {
                            Some(public_id) => {
                                self.location_associations_by_public_id.insert(
                                    public_id.clone(),
                                    location_association.clone(),
                                );
                            },
                            None => (),
                        };
                        match &location.id {
                            Some(id) => {
                                self.location_associations_by_id.insert(
                                    id.clone(),
                                    location_association.clone(),
                                );
                            },
                            None => (),
                        };
                    }
                }
            }
            Err(x) => {
                println!("WARNING: Failed to load location associations: {}", x);
            }
        }
        Ok(())
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

    pub fn get_irish_duplicate_trains(
        &self, train: &Train, other_namespace: &str
    ) -> HashSet<(String, String)> {
        // Ireland is symmetrical in how to find the other half's trains, so we can have one
        // function that manages both
        let schedule_manager = self.read();
        let other_schedule = schedule_manager.get(other_namespace);
        let other_schedule = match other_schedule {
            Some(other_schedule) => other_schedule,
            None => return HashSet::new(),
        };
        let public_id = match &train.variable_train.public_id {
            Some(public_id) => public_id,
            None => return HashSet::new(),
        };
        other_schedule
            .trains_indexed_by_public_id
            .get(public_id)
            .unwrap_or(&HashSet::new())
            .iter()
            .map(|train_id| (train_id.clone(), other_namespace.to_string()))
            .collect()
    }

    pub fn get_network_rail_duplicate_trains(
        &self, train: &Train
    ) -> HashSet<(String, String)> {
        // For Network Rail we only currently worry about Eurostar (LU is currently out of scope).
        // To map Eurostar we convert the reporting number to a train number and find by public ID.
        let schedule_manager = self.read();
        let other_schedule = schedule_manager.get("zzes");
        let other_schedule = match other_schedule {
            Some(other_schedule) => other_schedule,
            None => return HashSet::new(),
        };
        let public_id = match &train.variable_train.public_id {
            Some(public_id) => public_id,
            None => return HashSet::new(),
        };
        if !public_id.starts_with("9O") && !public_id.starts_with("9I") {
            return HashSet::new();
        };
        other_schedule
            .trains_indexed_by_public_id
            .get(&public_id.replace("O", "0").replace("I", "1"))
            .unwrap_or(&HashSet::new())
            .iter()
            .map(|train_id| (train_id.clone(), "zzes".to_string()))
            .collect()
    }

    pub fn get_eurostar_duplicate_trains(
        &self, train: &Train
    ) -> HashSet<(String, String)> {
        // For Eurostar for now we are worried about Network Rail; in future we might care about
        // other European infra operators but those are currently unsupported.
        // We convert the train number to a Network Rail alphanumeric reporting number.
        let schedule_manager = self.read();
        let other_schedule = schedule_manager.get("gbnr");
        let other_schedule = match other_schedule {
            Some(other_schedule) => other_schedule,
            None => return HashSet::new(),
        };
        let public_id = match &train.variable_train.public_id {
            Some(public_id) => public_id,
            None => return HashSet::new(),
        };
        if !public_id.starts_with("9") {
            return HashSet::new();
        };
        let train_reporting_number = if public_id[1..2] == *"0" {
            let mut train_reporting_number = public_id.clone();
            train_reporting_number.replace_range(1..2, "O");
            train_reporting_number
        } else if public_id[1..2] == *"1" {
            let mut train_reporting_number = public_id.clone();
            train_reporting_number.replace_range(1..2, "I");
            train_reporting_number
        } else {
            return HashSet::new();
        };
        other_schedule
            .trains_indexed_by_public_id
            .get(&train_reporting_number)
            .unwrap_or(&HashSet::new())
            .iter()
            .map(|train_id| (train_id.clone(), "gbnr".to_string()))
            .collect()
    }

    pub fn get_sncf_voyageurs_duplicate_trains(
        &self, _train: &Train
    ) -> HashSet<(String, String)> {
        // There's currently nothing
        return HashSet::new();
    }

    pub fn get_duplicate_trains(
        &self, namespace: &str, train: &Train
    ) -> HashSet<(String, String)> {
        // TODO this whole function should go somewhere else probably
        match namespace {
            "ieir" => self.get_irish_duplicate_trains(train, "gbni"),
            "gbni" => self.get_irish_duplicate_trains(train, "ieir"),
            "gbnr" => self.get_network_rail_duplicate_trains(train),
            "zzes" => self.get_eurostar_duplicate_trains(train),
            "frsv" => self.get_sncf_voyageurs_duplicate_trains(train),
            _ => {
                println!("WARNING: Unmatched namespace when finding duplicate trains");
                HashSet::new()
            },
        }
    }
}
