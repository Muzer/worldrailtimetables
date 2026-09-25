use crate::error::Error;
use crate::schedule::{
    accommodation_types, association_cancellation, association_node, line, location, schedule,
    train, train_allocation, train_cancellation, train_location, train_operator,
    train_validity_period, train_variant, train_vehicle, variable_train
};

use chrono::NaiveDate;
use chrono::naive::Days;

use sea_orm::{
    DatabaseConnection, DatabaseTransaction, DbErr, EntityLoaderTrait, ExprTrait, IntoSimpleExpr,
    QueryFilter, QueryOrder, TransactionTrait
};
use sea_orm::entity::prelude::Expr;
use sea_orm::prelude::{BelongsTo, HasMany, HasOne};

use serde::Deserialize;

use tokio::fs;
use tokio::sync::{Mutex, OwnedMutexGuard};

use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

// We maintain our own transaction lock to prevent small writes getting lost when we have large
// changes, or those small writes interrupting the larger transaction. TODO figure out if this can
// be done better at the DB level by strategically locking the schedule row.
pub struct TransactionalWriter {
    transaction: DatabaseTransaction,
    _transaction_lock: OwnedMutexGuard<()>,
}

impl Deref for TransactionalWriter {
    type Target = DatabaseTransaction;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

impl DerefMut for TransactionalWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transaction
    }
}

impl TransactionalWriter {
    pub async fn commit(self) -> Result<(), Error> {
        Ok(self.transaction.commit().await?)
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
    db: DatabaseConnection,
    transaction_lock: Arc<Mutex<()>>,
    pub location_associations_by_id: HashMap<String, LocationAssociation>,
    pub location_associations_by_public_id: HashMap<String, LocationAssociation>,
}

impl ScheduleManager {
    pub fn new(db: DatabaseConnection) -> Self {
        Self {
            db,
            ..Default::default()
        }
    }

    pub async fn load_location_associations(&mut self, filename: String) -> Result<(), Error> {
        match fs::read_to_string(filename).await {
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

    pub async fn get_all_schedules(&self) -> Result<Vec<schedule::ModelEx>, Error> {
        Ok(
            schedule::Entity::load()
            .order_by_asc(schedule::COLUMN.namespace)
            .all(&self.db)
            .await?
        )
    }

    pub async fn get_schedule_by_id(
        &self, namespace: &str
    ) -> Result<Option<schedule::ModelEx>, Error> {
        Ok(
            schedule::Entity::load()
            .filter_by_id(namespace.to_owned())
            .one(&self.db)
            .await?
        )
    }

    pub async fn get_location_by_id(
        &self, id: &str, namespace: &str
    ) -> Result<Option<location::ModelEx>, Error> {
        Ok(
            location::Entity::load()
            .filter_by_id((id.to_owned(), namespace.to_owned()))
            .one(&self.db)
            .await?
        )
    }

    pub async fn populate_association_nodes(
        &self, association_nodes: &mut Vec<association_node::ModelEx>
    ) -> Result<(), Error> {
        let mut replacements = association_node::Entity::load()
            .filter(
                association_node::COLUMN.parent_association_node_id.is_in(
                    association_nodes.iter().map(|x| x.id)
                )
            )
            .with(train_validity_period::Entity)
            .with((association_cancellation::Entity, train_validity_period::Entity))
            .all(&self.db)
            .await?;
        if replacements.len() > 0 {
            Box::pin(self.populate_association_nodes(&mut replacements)).await?;
        }
        let mut replacements: HashMap<i64, Vec<association_node::ModelEx>> = replacements
                .into_iter()
                .fold(HashMap::new(), |mut map, item| {
                    map.entry(item.parent_association_node_id.unwrap())
                        .or_default()
                        .push(item);
                    map
                });
        for association_node in association_nodes {
            association_node.replacements = HasMany::Loaded(
                replacements.entry(association_node.id).or_insert(vec![]).clone()
            );
        }
        Ok(())
    }

    pub async fn populate_variable_train(
        &self, variable_train: &mut variable_train::ModelEx
    ) -> Result<(), Error> {
        match &variable_train.timing_allocation_id {
            Some(allocation_id) => {
                let timing_allocation = train_allocation::Entity::load()
                    .filter_by_id((allocation_id.to_owned(), variable_train.namespace.to_owned()))
                    .with(train_vehicle::Entity)
                    .one(&self.db)
                    .await?;
                let timing_allocation = match timing_allocation {
                    Some(x) => x,
                    None => return Err(
                        DbErr::RecordNotFound(
                            "variable_train.timing_allocation not found".to_owned()
                        ).into()
                    ),
                };
                variable_train.timing_allocation = BelongsTo::Loaded(
                    Some(Box::new(timing_allocation))
                );
            },
            None => variable_train.timing_allocation = BelongsTo::Loaded(None),
        };
        match &variable_train.actual_allocation_id {
            Some(allocation_id) => {
                let actual_allocation = train_allocation::Entity::load()
                    .filter_by_id((allocation_id.to_owned(), variable_train.namespace.to_owned()))
                    .with(train_vehicle::Entity)
                    .one(&self.db)
                    .await?;
                let actual_allocation = match actual_allocation {
                    Some(x) => x,
                    None => return Err(
                        DbErr::RecordNotFound(
                            "variable_train.actual_allocation not found".to_owned()
                        ).into()
                    ),
                };
                variable_train.actual_allocation = BelongsTo::Loaded(
                    Some(Box::new(actual_allocation))
                );
            },
            None => variable_train.actual_allocation = BelongsTo::Loaded(None),
        };
        let accommodation_types = accommodation_types::Entity::load()
            .filter(accommodation_types::COLUMN.variable_train_id.eq(variable_train.id))
            .all(&self.db)
            .await?;
        variable_train.accommodation = HasMany::Loaded(accommodation_types);
        match &variable_train.line_id {
            Some(line_id) => {
                let line = line::Entity::load()
                    .filter_by_id((line_id.to_owned(), variable_train.namespace.to_owned()))
                    .one(&self.db)
                    .await?;
                let line = match line {
                    Some(x) => x,
                    None => return Err(
                        DbErr::RecordNotFound("variable_train.line not found".to_owned()).into()
                    ),
                };
                variable_train.line = BelongsTo::Loaded(Some(Box::new(line)));
            },
            None => variable_train.line = BelongsTo::Loaded(None),
        };
        match &variable_train.operator_id {
            Some(operator_id) => {
                let operator = train_operator::Entity::load()
                    .filter_by_id((operator_id.to_owned(), variable_train.namespace.to_owned()))
                    .one(&self.db)
                    .await?;
                let operator = match operator {
                    Some(x) => x,
                    None => return Err(
                        DbErr::RecordNotFound(
                            "variable_train.operator not found".to_owned()
                        ).into()
                    ),
                };
                variable_train.operator = BelongsTo::Loaded(Some(Box::new(operator)));
            },
            None => variable_train.operator = BelongsTo::Loaded(None),
        };
        Ok(())
    }

    pub async fn populate_train_variant(
        &self, train_variant: &mut train_variant::ModelEx
    ) -> Result<(), Error> {
        let mut replacements = train_variant::Entity::load()
            .filter(train_variant::COLUMN.parent_train_variant_id.eq(train_variant.id))
            .with(train_cancellation::Entity)
            .with(train_location::Entity)
            .with(train_validity_period::Entity)
            .with(variable_train::Entity)
            .all(&self.db)
            .await?;
        for replacement in &mut replacements {
            Box::pin(self.populate_train_variant(replacement)).await?;
        }
        train_variant.replacements = HasMany::Loaded(replacements);
        let cancellations = match &mut train_variant.cancellations {
            HasMany::Loaded(x) => x,
            HasMany::Unloaded => return Err(
                DbErr::RecordNotFound("train_variant.cancellations not loaded".to_owned()).into()
            ),
        };
        for train_cancellation in cancellations {
            let train_validity_periods = train_validity_period::Entity::load()
                .filter(
                    train_validity_period::COLUMN.train_cancellation_id.eq(
                        Some(train_cancellation.id)
                    )
                )
                .all(&self.db)
                .await?;
            train_cancellation.validity = HasMany::Loaded(train_validity_periods);
        }
        let route = match &mut train_variant.route {
            HasMany::Loaded(x) => x,
            HasMany::Unloaded => return Err(
                DbErr::RecordNotFound("train_variant.route not loaded".to_owned()).into()
            ),
        };
        route.sort_by_key(|train_location| train_location.index);
        let mut association_nodes = association_node::Entity::load()
                .filter(association_node::COLUMN.main_train_location_id.is_in(
                    route.iter().map(|x| x.id)
                ))
                .with(train_validity_period::Entity)
                .with((association_cancellation::Entity, train_validity_period::Entity))
                .all(&self.db)
                .await?;
        self.populate_association_nodes(&mut association_nodes).await?;
        let mut association_nodes: HashMap<i64, Vec<association_node::ModelEx>> = association_nodes
                .into_iter()
                .fold(HashMap::new(), |mut map, item| {
                    map.entry(item.main_train_location_id.unwrap())
                        .or_default()
                        .push(item);
                    map
                });
        for train_location in route {
            train_location.association_nodes = HasMany::Loaded(
                association_nodes.entry(train_location.id).or_insert(vec![]).clone()
            );
            let variable_train = variable_train::Entity::load()
                .filter(variable_train::COLUMN.train_location_id.eq(train_location.id))
                .one(&self.db)
                .await?;
            let variable_train = match variable_train {
                Some(mut variable_train) => {
                    self.populate_variable_train(&mut variable_train).await?;
                    Some(Box::new(variable_train))
                },
                None => None,
            };
            train_location.change_en_route = HasOne::Loaded(variable_train);
        }
        let variable_train = match &mut train_variant.variable_train {
            HasOne::Loaded(Some(x)) => &mut *x,
            HasOne::Loaded(None) => return Err(
                DbErr::RecordNotFound("train_variant.variable_train not found".to_owned()).into()
            ),
            HasOne::Unloaded => return Err(
                DbErr::RecordNotFound("train_variant.variable_train not loaded".to_owned()).into()
            ),
        };
        self.populate_variable_train(variable_train).await?;
        Ok(())
    }

    pub async fn populate_train_variants_for_location_lineup(
        &self, train_variants: &mut Vec<train_variant::ModelEx>, excluded_variants: &HashSet<i64>,
    ) -> Result<(), Error> {
        let mut replacements = train_variant::Entity::load()
            .filter(train_variant::COLUMN.parent_train_variant_id.is_in(
                train_variants.iter().map(|x| Some(x.id))
            ))
            .filter(train_variant::COLUMN.id.is_not_in(excluded_variants.clone()))
            .with((train_cancellation::Entity, train_validity_period::Entity))
            .with(train_location::Entity)
            .with(train_validity_period::Entity)
            .with((variable_train::Entity, train_operator::Entity))
            .all(&self.db)
            .await?;
        if replacements.len() > 0 {
            Box::pin(self.populate_train_variants_for_location_lineup(
                &mut replacements, excluded_variants
            )).await?;
        }
        let mut replacements: HashMap<i64, Vec<train_variant::ModelEx>> = replacements
                .into_iter()
                .fold(HashMap::new(), |mut map, item| {
                    map.entry(item.parent_train_variant_id.unwrap())
                        .or_default()
                        .push(item);
                    map
                });
        let route_chunks: Vec<Vec<i64>>
            = train_variants
            .iter()
            .map(|x| &x.route)
            .flatten()
            .map(|x| x.id)
            .collect::<Vec<i64>>()
            .chunks(10000)
            .map(|x| x.to_vec())
            .collect();
        let mut association_nodes: Vec<association_node::ModelEx> = vec![];
        for chunk in &route_chunks {
            association_nodes.append(&mut association_node::Entity::load()
                .filter(association_node::COLUMN.main_train_location_id.is_in(chunk.clone()))
                .with(train_validity_period::Entity)
                .with((association_cancellation::Entity, train_validity_period::Entity))
                .all(&self.db)
                .await?);
        }
        self.populate_association_nodes(&mut association_nodes).await?;
        let mut association_nodes: HashMap<i64, Vec<association_node::ModelEx>> = association_nodes
                .into_iter()
                .fold(HashMap::new(), |mut map, item| {
                    map.entry(item.main_train_location_id.unwrap())
                        .or_default()
                        .push(item);
                    map
                });
        let mut change_variable_trains: HashMap<i64, Option<Box<variable_train::ModelEx>>>
            = HashMap::new();
        for chunk in &route_chunks {
            change_variable_trains.extend(variable_train::Entity::load()
                .filter(variable_train::COLUMN.train_location_id.is_in(chunk.clone()))
                // Take a shortcut and do this explicitly here
                .with(train_operator::Entity)
                .all(&self.db)
                .await?
                .into_iter()
                .map(|x| (x.train_location_id.unwrap(), Some(Box::new(x.clone())))))
        }
        for train_variant in train_variants {
            let route = match &mut train_variant.route {
                HasMany::Loaded(x) => x,
                HasMany::Unloaded => return Err(
                    DbErr::RecordNotFound("train_variant.route not loaded".to_owned()).into()
                ),
            };
            route.sort_by_key(|train_location| train_location.index);
            for train_location in route {
                train_location.association_nodes = HasMany::Loaded(
                    association_nodes.entry(train_location.id).or_insert(vec![]).clone()
                );
                train_location.change_en_route = HasOne::Loaded(
                    change_variable_trains.entry(train_location.id).or_insert(None).clone()
                );
            }
            train_variant.replacements = HasMany::Loaded(
                replacements.entry(train_variant.id).or_insert(vec![]).clone()
            );
        }
        Ok(())
    }

    pub async fn get_train_by_id(
        &self, id: &str, namespace: &str
    ) -> Result<Option<train::ModelEx>, Error> {
        let train = train::Entity::load()
            .filter_by_id((id.to_owned(), namespace.to_owned()))
            .with((train_variant::Entity, train_cancellation::Entity))
            .with((train_variant::Entity, train_location::Entity))
            .with((train_variant::Entity, train_validity_period::Entity))
            .with((train_variant::Entity, variable_train::Entity))
            .one(&self.db)
            .await?;

        let mut train = match train {
            Some(x) => x,
            None => return Ok(None),
        };

        let train_variants = match &mut train.train_variants {
            HasMany::Loaded(x) => x,
            HasMany::Unloaded => return Err(
                DbErr::RecordNotFound("train.train_variants not loaded".to_owned()).into()
            ),
        };

        for train_variant in train_variants {
            self.populate_train_variant(train_variant).await?;
        }

        Ok(Some(train))
    }

    pub async fn get_trains_for_location_lineup_by_ids(
        &self, ids: &HashSet<(String, String)>, excluded_variants: &HashSet<i64>
    ) -> Result<Vec<train::ModelEx>, Error> {
        let mut trains = train::Entity::load()
            .filter(Expr::tuple(
                    [train::COLUMN.id.into_simple_expr(),
                    train::COLUMN.namespace.into_simple_expr()]
                )
                .is_in(ids.iter().map(|(x, y)| Expr::tuple([x.into(), y.into()])))
            )
            .all(&self.db)
            .await?;

        let mut train_variants = train_variant::Entity::load()
            .filter(
                Expr::tuple(
                    [train_variant::COLUMN.train_id.into_simple_expr(),
                    train_variant::COLUMN.namespace.into_simple_expr()]
                )
                .is_in(ids.iter().map(|(x, y)| Expr::tuple([x.into(), y.into()])))
            )
            .filter(train_variant::COLUMN.id.is_not_in(excluded_variants.clone()))
            .with((train_cancellation::Entity, train_validity_period::Entity))
            .with(train_location::Entity)
            .with(train_validity_period::Entity)
            .with((variable_train::Entity, train_operator::Entity))
            .all(&self.db)
            .await?;

        self.populate_train_variants_for_location_lineup(
            &mut train_variants, &excluded_variants
        ).await?;

        let mut train_variants: HashMap<(String, String), Vec<train_variant::ModelEx>>
            = train_variants.into_iter()
                .fold(HashMap::new(), |mut map, item| {
                    map.entry((item.train_id.clone().unwrap(), item.namespace.clone()))
                        .or_default()
                        .push(item);
                    map
                });

        for train in trains.iter_mut() {
            match train_variants.remove(&(train.id.clone(), train.namespace.clone())) {
                Some(train_variants) => train.train_variants = HasMany::Loaded(train_variants),
                None => train.train_variants = HasMany::Loaded(vec![]),
            };
        }

        Ok(trains)
    }

    pub async fn get_train_id_by_train_variant_id(
        &self, train_variant_id: Option<i64>
    ) -> Result<String, Error> {
        let train_variant_id = match train_variant_id {
            Some(train_variant_id) => train_variant_id,
            None => return Err(
                DbErr::RecordNotFound(
                    "train_variant.parent_train_variant_id not set when it must be".to_owned()
                ).into()
            ),
        };
        let train_variant = train_variant::Entity::load()
            .filter_by_id(train_variant_id)
            .one(&self.db)
            .await?;

        let train_variant = match &train_variant {
            Some(train_variant) => train_variant,
            None => return Err(
                DbErr::RecordNotFound(
                    "train_variant.parent_train_variant not found".to_owned()
                ).into()
            ),
        };

        Ok(match &train_variant.train_id {
            Some(train_id) => train_id.clone(),
            None => Box::pin(self.get_train_id_by_train_variant_id(
                    train_variant.parent_train_variant_id
                )).await?,
        })
    }

    pub async fn get_train_ids_by_train_variant_ids(
        &self, train_variant_ids: Vec<i64>
    ) -> Result<Vec<(String, String)>, Error> {
        let train_variants = train_variant::Entity::load()
            .filter(train_variant::COLUMN.id.is_in(train_variant_ids))
            .all(&self.db)
            .await?;

        let mut train_ids: Vec<(String, String)>
            = train_variants
            .iter()
            .filter(|x| x.train_id.is_some())
            .map(|x| (x.train_id.clone().unwrap(), x.namespace.clone()))
            .collect();

        let parent_ids: Vec<i64>
            = train_variants.iter().map(|x| x.parent_train_variant_id).flatten().collect();
        if parent_ids.len() != 0 {
            train_ids.append(
                &mut Box::pin(self.get_train_ids_by_train_variant_ids(parent_ids)).await?
            );
        }

        Ok(train_ids)
    }

    pub async fn get_train_ids_by_public_id(
        &self, public_id: &str, namespace: &str
    ) -> Result<HashSet<(String, String)>, Error> {
        let variable_trains = variable_train::Entity::load()
            .filter(variable_train::COLUMN.public_id.eq(public_id))
            .filter(variable_train::COLUMN.namespace.eq(namespace))
            .with(train_variant::Entity)
            .with((train_location::Entity, train_variant::Entity))
            .all(&self.db)
            .await?;
        let mut output = HashSet::new();

        for variable_train in variable_trains {
            match variable_train.train_variant.into_option() {
                Some(x) => {
                    match x.train_id {
                        Some(train_id) =>
                            output.insert((train_id.clone(), variable_train.namespace.clone())),
                        // Replacement train, recurse into the parent train
                        None => output.insert((
                            self.get_train_id_by_train_variant_id(
                                x.parent_train_variant_id
                            ).await?,
                            variable_train.namespace.clone(),
                        )),
                    };
                },
                None => (),
            };

            match variable_train.train_location.into_option() {
                Some(x) => {
                    let train_variant = x.train_variant.unwrap();
                    match train_variant.train_id {
                        Some(train_id) =>
                            output.insert(
                                (train_id.clone(), variable_train.namespace.clone())
                            ),
                        // Replacement train, recurse into the parent train
                        None => output.insert((
                            self.get_train_id_by_train_variant_id(
                                train_variant.parent_train_variant_id
                            ).await?,
                            variable_train.namespace.clone(),
                        )),
                    };
                },
                None => (),
            }
        }

        Ok(output)
    }

    pub async fn get_train_ids_with_excluded_variants_by_location_id_for_dates(
        &self, location_id: &str, namespace: &str, start_date: NaiveDate, end_date: NaiveDate
    ) -> Result<(HashSet<(String, String)>, HashSet<i64>), Error> {
        let mut train_locations = train_location::Entity::load()
            .filter(train_location::COLUMN.location_id.eq(location_id))
            .filter(train_location::COLUMN.namespace.eq(namespace))
            /*.filter(
                Expr::cust_with_exprs(
                    "DATE(@?, \"-\" || @? || \" DAYS\") <= @? \
                    AND DATE(@?, \"+\" || @? || \" DAYS\") >= @?",
                    start_date,
                    train_location::COLUMN.working_dep_day.if_null(
                        train_location::COLUMN.public_dep_day.if_null(
                            train_location::COLUMN.working_pass_day.if_null(
                                train_location::COLUMN.working_arr_day.if_null(
                                    train_location::COLUMN.public_arr_day
                                )
                            )
                        )
                    ) + 1,
                    train_location::COLUMN.
                )
            )*/
            .with((train_variant::Entity, train_validity_period::Entity))
            .all(&self.db)
            .await?;

        // We can now build a list of excluded variants based on the date provided. This is to allow
        // the next, "real" loading stage to also exclude them.
        let excluded_variants: HashSet<i64> = train_locations
            .iter()
            .filter(|train_location| {
                let train_variant = train_location.train_variant.clone().into_option().unwrap();
                // Account for timezone differences etc.
                let start_date = start_date - Days::new(1);
                let end_date = end_date + Days::new(1);
                let best_offset = {
                    if train_location.working_dep.is_some() {
                        train_location.working_dep_day.unwrap()
                    } else if train_location.public_dep.is_some() {
                        train_location.public_dep_day.unwrap()
                    } else if train_location.working_pass.is_some() {
                        train_location.working_pass_day.unwrap()
                    } else if train_location.working_arr.is_some() {
                        train_location.working_arr_day.unwrap()
                    } else if train_location.public_arr.is_some() {
                        train_location.public_arr_day.unwrap()
                    } else {
                        panic!("Location has no times");
                    }
                };
                let mut valid = false;
                for validity in train_variant.validity {
                    if start_date - Days::new(best_offset.into()) <= validity.valid_end.date()
                        && end_date + Days::new(best_offset.into()) >= validity.valid_begin.date() {
                        valid = true;
                    }
                }
                !valid
            })
            .map(|x| x.train_variant_id)
            .collect();
        train_locations.retain(|x| !excluded_variants.contains(&x.train_variant_id));
        let mut output = HashSet::new();

        output.extend(
            self.get_train_ids_by_train_variant_ids(
                train_locations
                .iter()
                .map(|x| x.train_variant.clone().into_option().unwrap().parent_train_variant_id)
                .flatten()
                .collect()
            )
            .await?
            .into_iter()
        );

        for train_location in train_locations {
            match train_location.train_variant.into_option() {
                Some(x) => {
                    match x.train_id {
                        Some(train_id) => {
                            output.insert((train_id.clone(), train_location.namespace.clone()));
                        },
                        // Replacement trains are handled above
                        None => (),
                    };
                },
                None => (),
            };
        }

        Ok((output, excluded_variants))
    }

    pub async fn get_locations_by_public_id(
        &self, public_id: &str, namespace: &str
    ) -> Result<Vec<location::ModelEx>, Error> {
        Ok(
            location::Entity::load()
            .filter(location::COLUMN.public_id.eq(Some(public_id)))
            .filter(location::COLUMN.namespace.eq(namespace))
            .all(&self.db)
            .await?
        )
    }

    pub async fn transactional_write(&self) -> Result<TransactionalWriter, Error> {
        let trans_lock = self.transaction_lock.clone().lock_owned().await;

        let transaction = self.db.begin().await?;

        Ok(TransactionalWriter {
            transaction: transaction,
            _transaction_lock: trans_lock,
        })
    }

    pub async fn get_irish_duplicate_trains(
        &self, train_variant: &train_variant::ModelEx, other_namespace: &str
    ) -> Result<HashSet<(String, String)>, Error> {
        // Ireland is symmetrical in how to find the other half's trains, so we can have one
        // function that manages both
        let public_id = match &train_variant.variable_train.clone().unwrap().public_id {
            Some(public_id) => public_id.clone(),
            None => return Ok(HashSet::new()),
        };
        Ok(self.get_train_ids_by_public_id(&public_id, other_namespace).await?)
    }

    pub async fn get_network_rail_duplicate_trains(
        &self, train_variant: &train_variant::ModelEx
    ) -> Result<HashSet<(String, String)>, Error> {
        // For Network Rail we only currently worry about Eurostar (LU is currently out of scope).
        // To map Eurostar we convert the reporting number to a train number and find by public ID.
        let public_id = match &train_variant.variable_train.clone().unwrap().public_id {
            Some(public_id) => public_id.clone(),
            None => return Ok(HashSet::new()),
        };
        if !public_id.starts_with("9O") && !public_id.starts_with("9I") {
            return Ok(HashSet::new());
        };
        Ok(self.get_train_ids_by_public_id(
            &public_id.replace("O", "0").replace("I", "1"), "zzes"
        ).await?)
    }

    pub async fn get_eurostar_duplicate_trains(
        &self, train_variant: &train_variant::ModelEx
    ) -> Result<HashSet<(String, String)>, Error> {
        // For Eurostar for now we are worried about Network Rail; in future we might care about
        // other European infra operators but those are currently unsupported.
        // We convert the train number to a Network Rail alphanumeric reporting number.
        let public_id = match &train_variant.variable_train.clone().unwrap().public_id {
            Some(public_id) => public_id.clone(),
            None => return Ok(HashSet::new()),
        };
        if !public_id.starts_with("9") {
            return Ok(HashSet::new());
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
            return Ok(HashSet::new());
        };
        Ok(self.get_train_ids_by_public_id(&train_reporting_number, "gbnr").await?)
    }

    pub async fn get_sncf_voyageurs_duplicate_trains(
        &self, _train_variant: &train_variant::ModelEx
    ) -> Result<HashSet<(String, String)>, Error> {
        // There's currently nothing
        return Ok(HashSet::new());
    }

    pub async fn get_duplicate_trains(
        &self, namespace: &str, train_variant: &train_variant::ModelEx
    ) -> Result<HashSet<(String, String)>, Error> {
        match namespace {
            "ieir" => Ok(self.get_irish_duplicate_trains(train_variant, "gbni").await?),
            "gbni" => Ok(self.get_irish_duplicate_trains(train_variant, "ieir").await?),
            "gbnr" => Ok(self.get_network_rail_duplicate_trains(train_variant).await?),
            "zzes" => Ok(self.get_eurostar_duplicate_trains(train_variant).await?),
            "frsv" => Ok(self.get_sncf_voyageurs_duplicate_trains(train_variant).await?),
            _ => {
                println!("WARNING: Unmatched namespace when finding duplicate trains");
                Ok(HashSet::new())
            },
        }
    }
}
