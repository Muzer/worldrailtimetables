use sea_orm::entity::prelude::*;

use serde::Serialize;

// Note: throughout this file there is an apparent inconsistency with the way primary keys are used.
// This is, at least somewhat, intentional — some entities are expected to correspond 1:1 with an
// entity from the source schedule, in which case the primary key will be the source ID (as a
// string) combined with the namespace (which as discussed below identifies the schedule itself).
// Other entities, however, are expected to be constructed and are unlikely to correspond with a
// source schedule entity. In these cases the primary key will be an autoincrement integer, with no
// namespace.

pub mod schedule {
    use async_trait::async_trait;
    use chrono::NaiveDateTime;
    use serde::Serialize;

    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "schedules")]
    pub struct Model {
        // represents a complete set of schedule data published by an infrastructure or train
        // operator

        // this is defined by me to uniquely identify the schedule source
        #[sea_orm(primary_key, auto_increment = false)]
        pub namespace: String,
        // what this schedule actually is, again defined by me
        pub description: String,
        // if the schedule itself has an ID, this goes here
        pub their_id: Option<String>,
        // timezone for the below datetimes; TODO constraint to ensure must be supplied
        pub timezone: Option<String>,
        // first date of validity of this schedule
        pub valid_begin: Option<NaiveDateTime>,
        // last date of validity of this schedule
        pub valid_end: Option<NaiveDateTime>,
        // date at which the schedule was last updated; does not include VSTP changes.
        pub last_updated: Option<NaiveDateTime>,

        // Relationship to locations in this namespace
        #[sea_orm(has_many)]
        pub locations: HasMany<super::location::Entity>,
        // Relationship to trains in this namespace. Note that a train can consist of multiple
        // TrainVariants in some schedule systems, so there is an extra level of indirection here.
        #[sea_orm(has_many)]
        pub trains: HasMany<super::train::Entity>,
        // Relationship to lines in this namespace
        #[sea_orm(has_many)]
        pub lines: HasMany<super::line::Entity>,
        // Relationship to operators in this namespace
        #[sea_orm(has_many)]
        pub train_operators: HasMany<super::train_operator::Entity>,
        // Relationship to allocations in this namespace
        #[sea_orm(has_many)]
        pub train_allocations: HasMany<super::train_allocation::Entity>,
        // Relationship to vehicles in this namespace
        #[sea_orm(has_many)]
        pub train_vehicles: HasMany<super::train_vehicle::Entity>,
    }

    #[async_trait]
    impl ActiveModelBehavior for ActiveModel {}
}

pub mod location {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "locations")]
    pub struct Model {
        // represents a location; trains can call at or pass a location. May represent a single
        // operationally separate location belonging to a larger physical location; `public_id` is
        // used to group logical locations into physical ones within a schedule.

        // the canonical ID of the location in the source data
        #[sea_orm(primary_key)]
        pub id: String,
        // this is defined by me to uniquely identify the schedule source
        #[sea_orm(primary_key)]
        pub namespace: String,
        // human-readable name of the location
        pub name: String,
        // some countries have an internal ID for planning and a public ID for retail; we allow
        // storing the public one here. Note that these need NOT be unique; public IDs may be
        // browsed to which will allow multiple logical locations to be represented within a single
        // physical one with a single public ID.
        pub public_id: Option<String>,
        // the timezone of this physical location; if wrong in source data this should be corrected
        // manually (often the source data has incorrect timezones for international locations)
        pub timezone: String,

        // Relationship to the parent Schedule
        #[sea_orm(belongs_to, from = "namespace", to = "namespace", on_delete = "Cascade")]
        pub schedule: BelongsTo<super::schedule::Entity>,
        // Relationship to TrainLocations, which represent a train passing through this location
        // (and potentially stopping)
        #[sea_orm(has_many)]
        pub train_locations: HasMany<super::train_location::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod train_validity_period {
    use sea_orm::entity::ActiveValue;
    use sea_orm::entity::prelude::*;

    use chrono::{NaiveDateTime, Weekday};
    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "train_validity_periods")]
    pub struct Model {
        // represents a consistent period of validity for a train, cancellation, association, or
        // association cancellation, with the train departing its first location or the primary
        // train in the association departing its first location on each indicated day of the week
        // within the range. Note that these are generated in logic so are not expected to be
        // reused; as such there are one-or-none-to-many relationship to the various places that
        // might need a validity period

        // autoinc PK
        #[sea_orm(primary_key)]
        pub id: i64,
        // Parent train ID
        #[sea_orm(indexed)]
        pub train_variant_id: Option<i64>,
        // Parent train cancellation ID
        #[sea_orm(indexed)]
        pub train_cancellation_id: Option<i64>,
        // Parent association ID
        #[sea_orm(indexed)]
        pub association_node_id: Option<i64>,
        // Parent association cancellation ID
        #[sea_orm(indexed)]
        pub association_cancellation_id: Option<i64>,
        // Timezone for the below DateTimes
        pub timezone: String,
        // first date of validity of this validity period
        pub valid_begin: NaiveDateTime,
        // last date of validity of this validity period
        pub valid_end: NaiveDateTime,

        // DAYS OF WEEK
        // Fields indicating the days of the week within the validity period on which this train
        // runs
        // True if the train runs on Monday
        pub monday: bool,
        // True if the train runs on Tuesday
        pub tuesday: bool,
        // True if the train runs on Wednesday
        pub wednesday: bool,
        // True if the train runs on Thursday
        pub thursday: bool,
        // True if the train runs on Friday
        pub friday: bool,
        // True if the train runs on Saturday
        pub saturday: bool,
        // True if the train runs on Sunday
        pub sunday: bool,

        // Relationship to the parent TrainVariant
        #[sea_orm(belongs_to, from = "train_variant_id", to = "id", on_delete = "Cascade")]
        pub train_variant: BelongsTo<Option<super::train_variant::Entity>>,
        // Relationship to the parent Train Cancellation
        #[sea_orm(belongs_to, from = "train_cancellation_id", to = "id", on_delete = "Cascade")]
        pub train_cancellation: BelongsTo<Option<super::train_cancellation::Entity>>,
        // Relationship to the parent AssociationNode
        #[sea_orm(belongs_to, from = "association_node_id", to = "id", on_delete = "Cascade")]
        pub association_node: BelongsTo<Option<super::association_node::Entity>>,
        // Relationship to the parent Association Cancellation
        #[sea_orm(belongs_to, from = "association_cancellation_id", to = "id", on_delete = "Cascade")]
        pub association_cancellation: BelongsTo<Option<super::association_cancellation::Entity>>,
    }

    impl ActiveModelBehavior for ActiveModel {}

    #[derive(Clone, Debug, PartialEq)]
    pub struct DaysOfWeek {
        // True if the train runs on Monday
        pub monday: bool,
        // True if the train runs on Tuesday
        pub tuesday: bool,
        // True if the train runs on Wednesday
        pub wednesday: bool,
        // True if the train runs on Thursday
        pub thursday: bool,
        // True if the train runs on Friday
        pub friday: bool,
        // True if the train runs on Saturday
        pub saturday: bool,
        // True if the train runs on Sunday
        pub sunday: bool,
    }

    impl DaysOfWeek {
        pub fn get_from_model(m: &ModelEx) -> Self {
            Self {
                monday: m.monday,
                tuesday: m.tuesday,
                wednesday: m.wednesday,
                thursday: m.thursday,
                friday: m.friday,
                saturday: m.saturday,
                sunday: m.sunday,
            }
        }

        pub fn get_from_active_model(m: &ActiveModelEx) -> Self {
            Self {
                monday: m.monday.clone().unwrap(),
                tuesday: m.tuesday.clone().unwrap(),
                wednesday: m.wednesday.clone().unwrap(),
                thursday: m.thursday.clone().unwrap(),
                friday: m.friday.clone().unwrap(),
                saturday: m.saturday.clone().unwrap(),
                sunday: m.sunday.clone().unwrap(),
            }
        }
    }

    impl ActiveModelEx {
        pub fn populate_days_of_week(&mut self, d: &DaysOfWeek) -> () {
            self.monday = ActiveValue::Set(d.monday);
            self.tuesday = ActiveValue::Set(d.tuesday);
            self.wednesday = ActiveValue::Set(d.wednesday);
            self.thursday = ActiveValue::Set(d.thursday);
            self.friday = ActiveValue::Set(d.friday);
            self.saturday = ActiveValue::Set(d.saturday);
            self.sunday = ActiveValue::Set(d.sunday);
        }

        pub fn populate_single_weekday(&mut self, weekday: Weekday) -> () {
            self.monday = ActiveValue::Set(false);
            self.tuesday = ActiveValue::Set(false);
            self.wednesday = ActiveValue::Set(false);
            self.thursday = ActiveValue::Set(false);
            self.friday = ActiveValue::Set(false);
            self.saturday = ActiveValue::Set(false);
            self.sunday = ActiveValue::Set(false);

            match weekday {
                Weekday::Mon => self.monday = ActiveValue::Set(true),
                Weekday::Tue => self.tuesday = ActiveValue::Set(true),
                Weekday::Wed => self.wednesday = ActiveValue::Set(true),
                Weekday::Thu => self.thursday = ActiveValue::Set(true),
                Weekday::Fri => self.friday = ActiveValue::Set(true),
                Weekday::Sat => self.saturday = ActiveValue::Set(true),
                Weekday::Sun => self.sunday = ActiveValue::Set(true),
            }
        }
    }

    impl DaysOfWeek {
        pub fn get_by_weekday(&self, weekday: Weekday) -> bool {
            match weekday {
                Weekday::Mon => self.monday,
                Weekday::Tue => self.tuesday,
                Weekday::Wed => self.wednesday,
                Weekday::Thu => self.thursday,
                Weekday::Fri => self.friday,
                Weekday::Sat => self.saturday,
                Weekday::Sun => self.sunday,
            }
        }
    }

    impl IntoIterator for &DaysOfWeek {
        type Item = bool;
        type IntoIter = std::array::IntoIter<bool, 7>;

        fn into_iter(self) -> Self::IntoIter {
            IntoIterator::into_iter([
                self.monday,
                self.tuesday,
                self.wednesday,
                self.thursday,
                self.friday,
                self.saturday,
                self.sunday,
            ])
        }
    }
}

#[derive(Clone, Copy, Debug, DeriveActiveEnum, EnumIter, Eq, PartialEq, Serialize)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::None)", rename_all = "PascalCase")]
pub enum TrainType {
    // Represents the type of train service (or other transport specified in a train schedule)

    // A bus of any kind; may or may not include coach depending on source data
    Bus,
    // An ordinary service bus
    ServiceBus,
    // A bus replacing a train
    ReplacementBus,
    // A freight train
    Freight,
    // A departmental (ie for internal railway use) freight train
    FreightDepartmental,
    // A freight train for internal civil engineering use
    FreightCivilEngineer,
    // A freight train for the use of internal mechanical and electrical engineers
    FreightMechanicalElectricalEngineer,
    // A freight train for the use of railway stores
    FreightStores,
    // A freight train to test for track defects
    FreightTest,
    // A freight train for the use of internal signalling and telecoms engineers
    FreightSignalTelecoms,
    // A freight train conveying automotive components
    FreightAutomotiveComponents,
    // A freight train conveying automotive vehicles
    FreightAutomotiveVehicles,
    // A freight train conveying edible products
    FreightEdibleProducts,
    // A freight train conveying industrial minerals
    FreightIndustrialMinerals,
    // A freight train conveying chemicals
    FreightChemicals,
    // A freight train conveying building materials as wagonload freight (ie each wagon may have a
    // different destination)
    FreightWagonloadBuildingMaterials,
    // A freight train conveying merchandise
    FreightMerchandise,
    // A freight train travelling internationally
    FreightInternational,
    // A freight train travelling internationally conveying mixed goods
    FreightInternationalMixed,
    // A freight train travelling internationally conveying intermodal containers
    FreightInternationalIntermodal,
    // A freight train travelling internationally conveying automotive vehicles
    FreightInternationalAutomotive,
    // A freight train travelling internationally conveying contract loads (ie fixed price
    // consistent)
    FreightInternationalContract,
    // A freight train travelling internationally conveying former Haulmark containers door-to-door
    FreightInternationalHaulmark,
    // A freight train travelling internationally running a joint venture service
    FreightInternationalJointVenture,
    // A freight train conveying intermodal containers on contract
    FreightIntermodalContracts,
    // A freight train conveying intermodal containers otherwise
    FreightIntermodalOther,
    // A freight train conveying coal for industrial distribution
    FreightCoalDistributive,
    // A freight train conveying coal for electricity generation
    FreightCoalElectricity,
    // A freight train conveying nuclear fuel or waste
    FreightNuclear,
    // A freight train conveying metals
    FreightMetals,
    // A freight train conveying building aggregates
    FreightAggregates,
    // A freight train conveying waste
    FreightWaste,
    // A freight train conveying building materials as trainload freight (ie the whole train has a
    // single destination)
    FreightTrainloadBuildingMaterials,
    // A freight train conveying petroleum products
    FreightPetroleum,
    // A train consisting only of locomotive(s) and a brake/guard's van
    LocomotiveBrakeVan,
    // A train consisting only of locomotive(s)
    Locomotive,
    // A passenger train of any kind
    Passenger,
    // An ordinary passenger train
    OrdinaryPassenger,
    // A passenger train that runs express, usually with fewer stops or on a more prestigious
    // service
    ExpressPassenger,
    // A passenger train that runs between major citiies, usually with limited stops in between
    IntercityPassenger,
    // A passenger train that runs primarily within a single urban area
    UrbanPassenger,
    // A passenger train that runs internationally
    InternationalPassenger,
    // A passenger train that runs a local service, usually with more stops and often a fairly rural
    // route
    LocalPassenger,
    // A passenger train that runs at least some portion of its route at high speeds, as defined by
    // the EU being above 200km/h for upgraded lines and 250km/h on newly built lines
    HighSpeedPassenger,
    // A passenger train that runs to suburban destinations, eg commuter towns
    SuburbanPassenger,
    // A passenger train that runs on regional routes, usually within a single region and with many
    // stops
    RegionalPassenger,
    // A passenger train that runs on interregional routes; usually a more limited stop service
    // travelling between regions but without the population to warrant an Intercity route
    InterregionalPassenger,
    // A passenger train that runs long distances, regardless of the speed or stopping pattern
    LongDistancePassenger,
    // A passenger train that conveys sleeping accommodation, ie sleepers or couchettes
    SleeperPassenger,
    // A passenger train that runs overnight but does not convey sleeping accommodation
    NightPassenger,
    // A passenger train that runs internationally and conveys sleeping accommodation
    InternationalSleeperPassenger,
    // A passenger train that carries automotive cars
    CarCarryingPassenger,
    // A passenger train that carries lorries or other large automotive vehicles
    LorryCarryingPassenger,
    // A passenger train running for tourist use, usually on a very scenic line and/or with heritage
    // stock
    TouristPassenger,
    // A passenger train intended primarily to link an airport with a city
    AirportLinkPassenger,
    // A passenger train running a shuttle service, normally with just two stops
    ShuttlePassenger,
    // A passenger train running as a replacement to another passenger train (Ersatzzug), usually
    // when the main train is unavailable and the different characteristics of the new train
    // (whether that be the train itself or the calling pattern) warrant a new train in the schedule
    ReplacementPassenger,
    // A passenger train that runs for special purposes, for example a charter train for special
    // ticket holders only, or a train to commemorate a special event
    SpecialPassenger,
    // A passenger train that runs as an additional service to the original schedule, to provide
    // relief to trains that would otherwise be overcrowded
    ReliefPassenger,
    // A passenger train that runs cross-country, normally an Intercity route that avoids a
    // significant population centre to provide long-distance semi-orbital travel or similar.
    CrossCountryPassenger,
    // A passenger train on a rack-and-pinion railway, for instance up a mountain
    RackAndPinionPassenger,
    // A passenger train not advertised to the general public, for example conveying invited guests
    // or a chartered train
    UnadvertisedPassenger,
    // A passenger train that runs express and is not advertised to the general public
    UnadvertisedExpressPassenger,
    // A passenger train running empty, usually but not always to or from a depot
    EmptyPassenger,
    // A train conveying only railway staff
    Staff,
    // An empty passenger train which railway staff may also use
    EmptyPassengerAndStaff,
    // A train conveying both passengers and freight
    Mixed,
    // A passenger train running a metro service; these are high frequency and often have little to
    // no emphasis on public timetables, and usually run at least partially underground or elevated
    Metro,
    // An empty passenger train for a metro service
    EmptyMetro,
    // A train conveying post/mail
    Post,
    // A train conveying parcels
    Parcels,
    // A train of empty non-passenger-carrying coaching stock
    EmptyNonPassenger,
    // A train which conveys both passengers and parcels
    PassengerParcels,
    // A ship or ferry
    Ship,
    // A trip working; that is a non-timetabled short-distance move which (depending on country) may
    // be operated under different regulations
    Trip,
    // A tram, that is a train with a substantial portion of street running, which (depending on
    // country) may be operated under different regulations, and is sometimes driven on sight
    Tram,
    // A tram hauled by a cable under the road
    CableTram,
    // An elevated cable from which cars hang, for example up a mountain
    CableCar,
    // A funicular railway, usually conveying passengers up a very steep hill or cliff in a
    // counterbalanced system
    Funicular,
    // An electric bus powered by trolley wires in a similar manner to a tram
    Trolleybus,
    // A train with only one rail
    Monorail,
    // A long-distance bus, normally high floor, with amenities for long-distance travel such as
    // luggage storage and a toilet
    Coach,
    // A coach of unknown type
    UndefinedCoach,
    // A coach travelling internationally
    InternationalCoach,
    // A coach travelling nationally
    NationalCoach,
    // A coach running a shuttle service, with just two stops
    ShuttleCoach,
    // A coach running within a region
    RegionalCoach,
    // A coach running a special trip, for instance for special ticket holders only
    SpecialCoach,
    // A coach conveying children to or from school
    SchoolCoach,
    // A coach conveying passengers through scenic areas for sightseeing purposes
    SightseeingCoach,
    // A coach conveying tourists
    TouristCoach,
    // A coach conveying commuters
    CommuterCoach,
    // A private taxi
    Taxi,
    // An aircraft
    Air,
    // An unknown form of transport
    Unknown,
    // A watercraft other than a ship/ferry
    Water,
    // Skiing
    SnowAndIce,
    // A lift/elevator
    Lift,
    // A car driven by the passenger
    SelfDrive,
    // Another form of transport
    Other,
}

#[derive(Clone, Copy, Debug, DeriveActiveEnum, EnumIter, Eq, PartialEq, Serialize)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::None)", rename_all = "PascalCase")]
pub enum TrainSource {
    // The source of the entity, borrowing British terminology but generally applicable.

    // An entity scheduled as part of the original plan for the whole timetable period
    LongTerm,
    // An entity scheduled as part of a short term alteration to the original plan, but still days
    // in advance
    ShortTerm,
    // An entity scheduled last-minute as a short-notice alteration, with less than a few days'
    // notice.
    VeryShortTerm,
    // An entity scheduled provisionally, whereby the schedule may not yet be final.
    Provisional,
}

#[derive(Clone, Copy, Debug, DeriveActiveEnum, EnumIter, Eq, PartialEq, Serialize)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::None)", rename_all = "PascalCase")]
pub enum TrainPower {
    // The way the train is powered

    // A train hauled by a diesel locomotive
    DieselLocomotive,
    // A train formed of diesel electric (ie, diesel with electric transmission) multiple unit(s)
    // (self-propelled coaches)
    DieselElectricMultipleUnit,
    // A train formed of diesel mechanical (ie, diesel with mechanical transmission) multiple
    // unit(s) (self-propelled coaches)
    DieselMechanicalMultipleUnit,
    // A train formed of diesel hydraulic (ie, diesel with hydraulic transmission) multiple unit(s)
    // (self-propelled coaches)
    DieselHydraulicMultipleUnit,
    // A train hauled by an electric locomotive, drawing power from overhead or third rail
    // electrification
    ElectricLocomotive,
    // A train hauled by a locomotive which can operate on diesel or electric, the latter drawing
    // power from overhead or third rail electrification
    ElectricAndDieselLocomotive,
    // A train formed of electric multiple unit(s) being hauled by a locomotive, for example when
    // being hauled beyond the limit of electrification
    ElectricMultipleUnitWithLocomotive,
    // A train formed of electric multiple unit(s), drawing power from overhead or third rail
    // electrification
    ElectricMultipleUnit,
    // A train formed of multiple unit(s) which can operate on diesel or electric, the latter
    // drawing power from overhead or third rail electrification
    ElectricAndDieselMultipleUnit,
    // A train hauled by a battery locomotive
    BatteryLocomotive,
    // A train formed of battery multiple unit(s), drawing power from internal batteries
    BatteryMultipleUnit,
    // A train hauled by a steam locomotive
    SteamLocomotive,
    // A train formed of a steam railcar (self-propelled coaches with a steam engine inside)
    SteamRailcar,
}

pub mod train_vehicle {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "train_vehicles")]
    pub struct Model {
        // Represents an individual vehicle planned to be part of the formation of a given train

        // ID from the source schedule
        #[sea_orm(primary_key)]
        pub id: String,
        // namespace of the source schedule
        #[sea_orm(primary_key)]
        pub namespace: String,
        // Description of the type of train vehicle
        pub description: String,
        // TODO more here, types etc.?

        // Relationship to the TrainAllocations
        #[sea_orm(has_many, via = "train_allocation_vehicle")]
        pub train_allocations: HasMany<super::train_allocation::Entity>,
        // Relationship to the parent Schedule
        #[sea_orm(belongs_to, from = "namespace", to = "namespace", on_delete = "Cascade")]
        pub schedule: BelongsTo<super::schedule::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod train_allocation {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "train_allocations")]
    pub struct Model {
        // Represents the scheduled allocation of a given train at a given point; this may either
        // be the actual planned allocation or just the allocation for timing purposes

        // ID from the source schedule
        #[sea_orm(primary_key)]
        pub id: String,
        // namespace of the source schedule
        #[sea_orm(primary_key)]
        pub namespace: String,
        // Description of the allocation; may either supplement or replace the list of vehicles
        pub description: String,

        // Relationship to the TrainVehicles (if available)
        #[sea_orm(has_many, via = "train_allocation_vehicle")]
        pub vehicles: HasMany<super::train_vehicle::Entity>,
        // Relationship to VariableTrains for actual allocations
        #[sea_orm(has_many, relation_enum = "AllocationActual", via_rel = "Actual" )]
        pub actual_variable_trains: HasMany<super::variable_train::Entity>,
        // Relationship to VariableTrains for timing allocations
        #[sea_orm(has_many, relation_enum = "AllocationTiming", via_rel = "Timing" )]
        pub timing_variable_trains: HasMany<super::variable_train::Entity>,
        // Relationship to the parent Schedule
        #[sea_orm(belongs_to, from = "namespace", to = "namespace", on_delete = "Cascade")]
        pub schedule: BelongsTo<super::schedule::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod train_allocation_vehicle {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "train_allocation_vehicles")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub train_allocation_id: String,
        #[sea_orm(primary_key)]
        pub train_vehicle_id: String,
        #[sea_orm(primary_key)]
        pub namespace: String,
        #[sea_orm(belongs_to, from = "(train_allocation_id, namespace)", to = "(id, namespace)", on_delete = "Cascade")]
        pub train_allocation: BelongsTo<super::train_allocation::Entity>,
        #[sea_orm(belongs_to, from = "(train_vehicle_id, namespace)", to = "(id, namespace)", on_delete = "Cascade")]
        pub train_vehicle: BelongsTo<super::train_vehicle::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod train_operator {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "train_operators")]
    pub struct Model {
        // Represents the operator of a given train at a given point; this may refer to a legal
        // entity or to a sub-brand thereof depending on the source data.

        // ID from the source schedule
        #[sea_orm(primary_key)]
        pub id: String,
        // namespace of the source schedule
        #[sea_orm(primary_key)]
        pub namespace: String,
        // Public-facing ID of the operator, if there is one. Generally if left empty the internal
        // ID will be used instead.
        pub public_id: Option<String>,
        // Textual description (eg human-readable name) of the operator.
        pub description: Option<String>,

        // Relationship to VariableTrains that run with this operator
        #[sea_orm(has_many)]
        pub variable_trains: HasMany<super::variable_train::Entity>,
        // Relationship to the parent Schedule
        #[sea_orm(belongs_to, from = "namespace", to = "namespace", on_delete = "Cascade")]
        pub schedule: BelongsTo<super::schedule::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

#[derive(Clone, Copy, Debug, DeriveActiveEnum, EnumIter, Eq, PartialEq, Serialize)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::None)", rename_all = "PascalCase")]
pub enum ReservationField {
    // Indicates the nature of seat (or berth) reservation for a particular type of travel (class or
    // accommodation type)

    // Reservations are possible, but not required; passengers may either reserve a seat or (with a
    // valid ticket) sit in an available seat
    Possible,
    // Reservations are required to travel on this train; passengers must hold a booked seat along
    // with their travel ticket
    Mandatory,
    // Reservations are required only if travelling on this train from its origin station
    MandatoryFromOrigin,
    // Reservations are recommended, but not required, for instance on services that regularly fill
    // up
    Recommended,
    // Reservations are not possible; passengers cannot reserve a seat in advance
    Impossible,
    // Reservations are not mandatory for this train; whether or not they are even possible is left
    // ambiguous, as sometimes source data does not provide this information
    NotMandatory,
    // This particular type of travel is not conveyed on this train, so reservation information is
    // not applicable here
    NotApplicable,
    // Reservations have restrictions around their purchase
    Restricted,
    // The type of travel is not allowed on this train at all, for example group bookings forbidden
    NotAllowed,
    // The nature of reservations is unknown for this type of travel
    Unknown,
}

#[derive(Clone, Copy, Debug, DeriveActiveEnum, EnumIter, Eq, PartialEq, Serialize)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::None)", rename_all = "PascalCase")]
pub enum AssociationType {
    // Represents the type of association between two trains

    // Main train divides to form the other train. In some countries divisions are handled
    // "symmetrically", with both trains duplicating the schedule in the shared section. In these
    // cases it does not matter right now which way around the division is represented for now,
    // though in future in cases where one train ID is to be "suppressed" the main train for this
    // relationship should contain the train ID which should remain. In other countries, however,
    // the subsidiary train "starts at" the division point, in which case it is important that in
    // this relationship direction, the main train is the one through from the origin and the other
    // train is the one that starts at the division point.
    MainDividesToFormOther,
    // Main train divides from the other train. This is the inverse relationship of the above.
    MainDividesFromOther,
    // Main train joins the other train. In some countries divisions are handled "symmetrically",
    // with both trains duplicating the schedule in the shared section. In these cases it does not
    // matter right now which way around the division is represented for now, though in future in
    // cases where one train ID is to be "suppressed" the main train for this relationship should
    // contain the train ID which should remain. In other countries, however, the subsidiary train
    // "ends at" the join point, in which case it is important that in this relationship direction,
    // the main train is the one through to the final destination, and the other train is the one
    // that terminates at the join point.
    MainJoinsToOther,
    // Main train is joined to by the other train. This is the inverse relationship of the above.
    MainIsJoinedToByOther,
    // Main train becomes the other train after terminating. This is normally an operational
    // association, though may also be used for a train changing identity en route in some datasets.
    // In theory there should only be one of these per TrainLocation; trains becoming multiple
    // trains can be indicated with non-passenger divisions.
    MainBecomesOther,
    // Main train is formed from the other train after the latter terminates. This is the inverse
    // relationship of the above.
    MainFormsFromOther,
}

pub mod association_node {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "association_nodes")]
    pub struct Model {
        // Represents a unidirectional relationship between two trains at a given location; for
        // instance, joining, dividing, or forming the next/previous service.

        // autoinc PK
        #[sea_orm(primary_key)]
        pub id: i64,
        // Not set if replacement
        #[sea_orm(indexed)]
        pub main_train_location_id: Option<i64>,
        // ID of the other train. Note that we identify the other train with this train
        // ID/namespace/location suffix trio deliberately, as the same association may apply to
        // multiple TrainVariants of the other train.
        #[sea_orm(indexed)]
        pub other_train_id: String,
        // Namespace (with which to identify the other train)
        #[sea_orm(indexed)]
        pub namespace: String,
        // Suffix to allow the location of the other train to be uniquely identified, if the train
        // runs through the same place twice for instance. TODO this should be made more flexible,
        // for example to allow NeTEx where the key is either the arrival or departure time
        // depending on the type of association.
        pub other_train_location_id_suffix: Option<String>,
        // Indicates the parent association, if this association replaces another.
        #[sea_orm(indexed)]
        pub parent_association_node_id: Option<i64>,
        // The type of association. TODO partial unique index on this and parent_association_node_id
        // if this is MainBecomesOther or MainFormsFromOther?
        pub association_type: super::AssociationType,
        // Offset of the start day of the other train compared to the main train. For example, if
        // the main train starts on Fridays and then divides early on Saturday morning to form a
        // second train which has the division point as its first location, this will be 1; for the
        // inverse relationship it will be -1.
        pub day_diff: i8,
        // Indicates whether or not this association is to be advertised to passengers.
        pub for_passengers: bool,
        // The schedule source of the association; see the documentation there for the full meaning.
        pub source: Option<super::TrainSource>,

        // Relationship to the validity periods of this association
        #[sea_orm(has_many)]
        pub validity: HasMany<super::train_validity_period::Entity>,
        // Relationship to the cancellations of this association; these are periods during which
        // this association does not apply, even during its validity period.
        #[sea_orm(has_many)]
        pub cancellations: HasMany<super::association_cancellation::Entity>,
        // Relationship to the replacements for this association; these are associations that
        // override this one during their validity periods, even though they are a part of this
        // association's validity period.
        #[sea_orm(has_many, relation_enum = "AssociationReplacement", via_rel = "Replacement" )]
        pub replacements: HasMany<super::association_node::Entity>,
        // Relationship to the parent main train's TrainLocation
        #[sea_orm(belongs_to, from = "main_train_location_id", to = "id", on_delete = "Cascade")]
        pub main_train_location: BelongsTo<Option<super::train_location::Entity>>,
        // Relationship to the parent AssociationNode if this is a replacement
        #[sea_orm(
            belongs_to,
            relation_enum = "Replacement",
            from = "parent_association_node_id",
            to = "id",
            on_delete = "Cascade",
        )]
        pub parent: BelongsTo<Option<super::association_node::Entity>>,
        // TODO: other_train is not currently a FK to allow simple single-pass filling of the
        // database with trains. Should this change?
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod association_cancellation {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "association_cancellations")]
    pub struct Model {
        // Represents a period of cancellation of an association, in which the association is not
        // valid despite being inside its validity period.

        // autoinc PK
        #[sea_orm(primary_key)]
        pub id: i64,
        // Indicates the association node being cancelled
        #[sea_orm(indexed)]
        pub association_node_id: i64,
        // Indicates the source of the cancellation; see the documentation there for the full
        // meaning.
        pub source: Option<super::TrainSource>,

        // Relationship to the validity periods of this cancellation
        #[sea_orm(has_many)]
        pub validity: HasMany<super::train_validity_period::Entity>,
        // Relationship to the parent AssociationNode
        #[sea_orm(belongs_to, from = "association_node_id", to = "id", on_delete = "Cascade")]
        pub association_node: BelongsTo<super::association_node::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod train_location {
    use sea_orm::entity::ActiveValue;
    use sea_orm::entity::prelude::*;

    use chrono::NaiveTime;
    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "train_locations")]
    pub struct Model {
        // Represents a train being at a location in its journey

        // autoinc PK
        #[sea_orm(primary_key)]
        pub id: i64,
        // Index of the location in the train's journey
        pub index: i64,
        pub train_variant_id: i64,
        // Timezone for timings, if different from the location TZ (true for GTFS)
        pub timing_tz: Option<String>,
        // ID of the location the train is passing through
        pub location_id: String,
        // Namespace (with which to identify the location)
        pub namespace: String,
        // Additional key to uniquely identify the location in case a train runs through the same
        // location more than once, for matching associations. TODO this should be elaborated upon
        // as some formats (eg NeTEx) key this based on either arrival or departure time depending
        // on the type of association
        pub id_suffix: Option<String>,
        // Arrival time in operational/internal publications
        pub working_arr: Option<NaiveTime>,
        // Number of days after the train's departure that the above time occurs
        pub working_arr_day: Option<u8>,
        // Departure time in operational/internal publications
        pub working_dep: Option<NaiveTime>,
        // Number of days after the train's departure that the above time occurs
        pub working_dep_day: Option<u8>,
        // Passing time in operational/internal publications
        pub working_pass: Option<NaiveTime>,
        // Number of days after the train's departure that the above time occurs
        pub working_pass_day: Option<u8>,
        // Arrival time in public publications
        pub public_arr: Option<NaiveTime>,
        // Number of days after the train's departure that the above time occurs
        pub public_arr_day: Option<u8>,
        // Departure time in public publications
        pub public_dep: Option<NaiveTime>,
        // Number of days after the train's departure that the above time occurs
        pub public_dep_day: Option<u8>,
        // Platform number
        pub platform: Option<String>,
        // Platform zone — this indicates which fraction of a longer platform a train will stop at
        pub platform_zone: Option<String>,
        // Code of the line the train will use after departure from this location
        pub line: Option<String>,
        // Code of the line the train will have used to enter this location
        pub path: Option<String>,
        // Number of seconds of additional allowance added after this location for engineering work
        pub engineering_allowance_s: Option<u32>,
        // Number of seconds of additional allowance added after this location for pathing reasons
        // (ie being delayed by another timetabled train)
        pub pathing_allowance_s: Option<u32>,
        // Number of seconds of additional allowance added after this location for train performance
        // reasons
        pub performance_allowance_s: Option<u32>,

        // ACTIVITIES (mandatory)
        // Indicates the activities performed by a train at a location. Availability depends on
        // source data.
        // Detaches part of the train
        pub detach: bool,
        // Attaches part of the train
        pub attach: bool,
        // Allows other trains to pass (overtake) here
        pub other_trains_pass: bool,
        // Attaches or detaches an assisting locomotive
        pub attach_or_detach_assisting_loco: bool,
        // Shows an x in the arrival time in the printed timetable (Britain only)
        pub x_on_arrival: bool,
        // Is assisted by a banking locomotive
        pub banking_loco: bool,
        // Changes train crew
        pub crew_change: bool,
        // Sets down passengers only
        pub set_down_only: bool,
        // Train is examined
        pub examination: bool,
        // "GBPRTT data to add" (Britain only, I have no idea what this means)
        pub gbprtt: bool,
        // Used to prevent two columns being merged in a printed timetable
        pub prevent_column_merge: bool,
        // Used to prevent a third column being merged in a printed timetable
        pub prevent_third_column_merge: bool,
        // Passenger numbers are counted
        pub passenger_count: bool,
        // Tickets are collected
        pub ticket_collection: bool,
        // Tickets are examined
        pub ticket_examination: bool,
        // First class tickets are examined
        pub first_class_ticket_examination: bool,
        // Some tickets are examined
        pub selective_ticket_examination: bool,
        // A locomotive is changed
        pub change_loco: bool,
        // Stop not advertised for passenger use
        pub unadvertised_stop: bool,
        // Stop for operational reasons
        pub operational_stop: bool,
        // Main locomotive is on the rear of the train
        pub train_locomotive_on_rear: bool,
        // Start or end of a propelling move (driven from the rear of the train)
        pub propelling: bool,
        // Picks up passengers only on request
        pub request_pick_up: bool,
        // Sets down passengers only on request
        pub request_set_down: bool,
        // Train reverses direction
        pub reversing_move: bool,
        // Locomotive runs round
        pub run_round: bool,
        // Stops for staff to board or alight
        pub staff_stop: bool,
        // Stops to pick up and set down passengers (a normal passenger stop)
        pub normal_passenger_stop: bool,
        // Train begins its journey
        pub train_begins: bool,
        // Train finishes its journey
        pub train_finishes: bool,
        // Activity for TOPS reporting (Britain only)
        pub tops_reporting: bool,
        // Exchanges token (or other means of guaranteeing entry onto a single line)
        pub token_etc: bool,
        // Stops to pick up passengers only
        pub pick_up_only: bool,
        // Coaching stock is watered
        pub watering_stock: bool,
        // Crosses another train travelling in the opposite direction at a passing point on a single
        // line (or conflicting junction)
        pub cross_at_passing_point: bool,
        // Picks up passengers only on request by telephone
        pub request_pick_up_by_telephone: bool,
        // Sets down passengers only on request by telephone
        pub request_set_down_by_telephone: bool,
        // Timings are approximate at this location
        pub times_approximate: bool,

        // Relationship to the new VariableTrain that takes force upon departure from this location,
        // if any (this can be used to indicate the majority of changes to a train en route)
        #[sea_orm(has_one)]
        pub change_en_route: HasOne<super::variable_train::Entity>,
        // Relationship to the AssociationNodes that apply to this location.
        #[sea_orm(has_many)]
        pub association_nodes: HasMany<super::association_node::Entity>,
        // Relationship to the TrainVariant to which this TrainLocation belongs
        #[sea_orm(belongs_to, from = "train_variant_id", to = "id", on_delete = "Cascade")]
        pub train_variant: BelongsTo<super::train_variant::Entity>,
        // Relationship to the Location to which this TrainLocation belongs
        #[sea_orm(belongs_to, from = "(location_id, namespace)", to = "(id, namespace)", on_delete = "Cascade")]
        pub location: BelongsTo<super::location::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}

    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct Activities {
        // Detaches part of the train
        pub detach: bool,
        // Attaches part of the train
        pub attach: bool,
        // Allows other trains to pass (overtake) here
        pub other_trains_pass: bool,
        // Attaches or detaches an assisting locomotive
        pub attach_or_detach_assisting_loco: bool,
        // Shows an x in the arrival time in the printed timetable (Britain only)
        pub x_on_arrival: bool,
        // Is assisted by a banking locomotive
        pub banking_loco: bool,
        // Changes train crew
        pub crew_change: bool,
        // Sets down passengers only
        pub set_down_only: bool,
        // Train is examined
        pub examination: bool,
        // "GBPRTT data to add" (Britain only, I have no idea what this means)
        pub gbprtt: bool,
        // Used to prevent two columns being merged in a printed timetable
        pub prevent_column_merge: bool,
        // Used to prevent a third column being merged in a printed timetable
        pub prevent_third_column_merge: bool,
        // Passenger numbers are counted
        pub passenger_count: bool,
        // Tickets are collected
        pub ticket_collection: bool,
        // Tickets are examined
        pub ticket_examination: bool,
        // First class tickets are examined
        pub first_class_ticket_examination: bool,
        // Some tickets are examined
        pub selective_ticket_examination: bool,
        // A locomotive is changed
        pub change_loco: bool,
        // Stop not advertised for passenger use
        pub unadvertised_stop: bool,
        // Stop for operational reasons
        pub operational_stop: bool,
        // Main locomotive is on the rear of the train
        pub train_locomotive_on_rear: bool,
        // Start or end of a propelling move (driven from the rear of the train)
        pub propelling: bool,
        // Picks up passengers only on request
        pub request_pick_up: bool,
        // Sets down passengers only on request
        pub request_set_down: bool,
        // Train reverses direction
        pub reversing_move: bool,
        // Locomotive runs round
        pub run_round: bool,
        // Stops for staff to board or alight
        pub staff_stop: bool,
        // Stops to pick up and set down passengers (a normal passenger stop)
        pub normal_passenger_stop: bool,
        // Train begins its journey
        pub train_begins: bool,
        // Train finishes its journey
        pub train_finishes: bool,
        // Activity for TOPS reporting (Britain only)
        pub tops_reporting: bool,
        // Exchanges token (or other means of guaranteeing entry onto a single line)
        pub token_etc: bool,
        // Stops to pick up passengers only
        pub pick_up_only: bool,
        // Coaching stock is watered
        pub watering_stock: bool,
        // Crosses another train travelling in the opposite direction at a passing point on a single
        // line (or conflicting junction)
        pub cross_at_passing_point: bool,
        // Picks up passengers only on request by telephone
        pub request_pick_up_by_telephone: bool,
        // Sets down passengers only on request by telephone
        pub request_set_down_by_telephone: bool,
        // Timings are approximate at this location
        pub times_approximate: bool,
    }

    impl Activities {
        pub fn get_from_model(m: &ModelEx) -> Self {
            Self {
                detach: m.detach,
                attach: m.attach,
                other_trains_pass: m.other_trains_pass,
                attach_or_detach_assisting_loco: m.attach_or_detach_assisting_loco,
                x_on_arrival: m.x_on_arrival,
                banking_loco: m.banking_loco,
                crew_change: m.crew_change,
                set_down_only: m.set_down_only,
                examination: m.examination,
                gbprtt: m.gbprtt,
                prevent_column_merge: m.prevent_column_merge,
                prevent_third_column_merge: m.prevent_third_column_merge,
                passenger_count: m.passenger_count,
                ticket_collection: m.ticket_collection,
                ticket_examination: m.ticket_examination,
                first_class_ticket_examination: m.first_class_ticket_examination,
                selective_ticket_examination: m.selective_ticket_examination,
                change_loco: m.change_loco,
                unadvertised_stop: m.unadvertised_stop,
                operational_stop: m.operational_stop,
                train_locomotive_on_rear: m.train_locomotive_on_rear,
                propelling: m.propelling,
                request_pick_up: m.request_pick_up,
                request_set_down: m.request_set_down,
                reversing_move: m.reversing_move,
                run_round: m.run_round,
                staff_stop: m.staff_stop,
                normal_passenger_stop: m.normal_passenger_stop,
                train_begins: m.train_begins,
                train_finishes: m.train_finishes,
                tops_reporting: m.tops_reporting,
                token_etc: m.token_etc,
                pick_up_only: m.pick_up_only,
                watering_stock: m.watering_stock,
                cross_at_passing_point: m.cross_at_passing_point,
                request_pick_up_by_telephone: m.request_pick_up_by_telephone,
                request_set_down_by_telephone: m.request_set_down_by_telephone,
                times_approximate: m.times_approximate,
            }
        }
    }

    impl ActiveModelEx {
        pub fn populate_activities(&mut self, a: &Activities) -> () {
            self.detach = ActiveValue::Set(a.detach);
            self.attach = ActiveValue::Set(a.attach);
            self.other_trains_pass = ActiveValue::Set(a.other_trains_pass);
            self.attach_or_detach_assisting_loco
                = ActiveValue::Set(a.attach_or_detach_assisting_loco);
            self.x_on_arrival = ActiveValue::Set(a.x_on_arrival);
            self.banking_loco = ActiveValue::Set(a.banking_loco);
            self.crew_change = ActiveValue::Set(a.crew_change);
            self.set_down_only = ActiveValue::Set(a.set_down_only);
            self.examination = ActiveValue::Set(a.examination);
            self.gbprtt = ActiveValue::Set(a.gbprtt);
            self.prevent_column_merge = ActiveValue::Set(a.prevent_column_merge);
            self.prevent_third_column_merge = ActiveValue::Set(a.prevent_third_column_merge);
            self.passenger_count = ActiveValue::Set(a.passenger_count);
            self.ticket_collection = ActiveValue::Set(a.ticket_collection);
            self.ticket_examination = ActiveValue::Set(a.ticket_examination);
            self.first_class_ticket_examination
                = ActiveValue::Set(a.first_class_ticket_examination);
            self.selective_ticket_examination = ActiveValue::Set(a.selective_ticket_examination);
            self.change_loco = ActiveValue::Set(a.change_loco);
            self.unadvertised_stop = ActiveValue::Set(a.unadvertised_stop);
            self.operational_stop = ActiveValue::Set(a.operational_stop);
            self.train_locomotive_on_rear = ActiveValue::Set(a.train_locomotive_on_rear);
            self.propelling = ActiveValue::Set(a.propelling);
            self.request_pick_up = ActiveValue::Set(a.request_pick_up);
            self.request_set_down = ActiveValue::Set(a.request_set_down);
            self.reversing_move = ActiveValue::Set(a.reversing_move);
            self.run_round = ActiveValue::Set(a.run_round);
            self.staff_stop = ActiveValue::Set(a.staff_stop);
            self.normal_passenger_stop = ActiveValue::Set(a.normal_passenger_stop);
            self.train_begins = ActiveValue::Set(a.train_begins);
            self.train_finishes = ActiveValue::Set(a.train_finishes);
            self.tops_reporting = ActiveValue::Set(a.tops_reporting);
            self.token_etc = ActiveValue::Set(a.token_etc);
            self.pick_up_only = ActiveValue::Set(a.pick_up_only);
            self.watering_stock = ActiveValue::Set(a.watering_stock);
            self.cross_at_passing_point = ActiveValue::Set(a.cross_at_passing_point);
            self.request_pick_up_by_telephone = ActiveValue::Set(a.request_pick_up_by_telephone);
            self.request_set_down_by_telephone = ActiveValue::Set(a.request_set_down_by_telephone);
            self.times_approximate = ActiveValue::Set(a.times_approximate);
        }
    }
}

#[derive(Clone, Copy, Debug, DeriveActiveEnum, EnumIter, Eq, PartialEq, Serialize)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::None)", rename_all = "PascalCase")]
pub enum AccommodationClass {
    // Represents a class of accommodation, fairly heavily normalised

    // For where the class of accommodation is not known
    Unknown,
    // Any premium accommodation class sold with an additional charge on top of a first class fare.
    // Examples include ÖBB Railjet Business Class and Trenitalia Executive
    FirstPremium,
    // Ordinary first class accommodation, accessible with a first class ticket (and any mandatory
    // reservation fee if the train requires it)
    First,
    // Any premium accommodation class sold with an additional charge on top of a second class fare.
    // Examples include Avanti West Coast Standard Premium, Eurostar Plus, and many more.
    SecondPremium,
    // Ordinary second class accommodation, accessible with a second class ticket (and any mandatory
    // reservation fee if the train requires it)
    Second,
    // Third class accommodation, for trains with a budget option below the usual second class
    Third,
    // Unclassified accommodation, for instance many restaurant cars; this is traditionally not
    // considered to be either first or second class.
    Unclassified,
}

pub mod accommodation_types {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "accommodation_types")]
    pub struct Model {
        // Represents the types of accommodation available for the specified class of accommodation
        // on a given train. All are optional; None means "unknown", false means "not present", true
        // means "present".

        // autoinc PK
        #[sea_orm(primary_key)]
        pub id: i64,
        #[sea_orm(indexed)]
        pub variable_train_id: i64,
        // The class of accommodation to which these accommodation types apply
        pub class: super::AccommodationClass,
        // Standing is permitted
        pub standing: Option<bool>,
        // Seats available
        pub seating: Option<bool>,
        // Seats which recline are available
        pub reclining_seating: Option<bool>,
        // Special seating is available
        pub special_seating: Option<bool>,
        // Sleepers (private compartments with 1-3 beds with linen) available
        pub sleeper: Option<bool>,
        // Single sleepers (private compartments with 1 bed with linen) available
        pub single_sleeper: Option<bool>,
        // Double sleepers (private compartments with 2 beds with linen) available
        pub double_sleeper: Option<bool>,
        // Special sleepers available
        pub special_sleeper: Option<bool>,
        // Couchettes (shared compartments with 4-6 spots to lie down in with no linen) available
        pub couchette: Option<bool>,
        // Single couchettes (not sure on meaning of this) available
        pub single_couchette: Option<bool>,
        // Double couchettes (not sure on meaning of this) available
        pub double_couchette: Option<bool>,
        // Accommodation specifically for babies available
        pub baby: Option<bool>,
        // Accommodation specifically for families available
        pub family: Option<bool>,
        // Area for recreation (eg cinema coach) available
        pub recreation: Option<bool>,
        // Panoramic coach (with large windows stretching to the roof) available
        pub panoramic: Option<bool>,
        // Pullman accommodation available — the meaning of this likely varies by operator, but
        // traditionally Pullman was associated with luxury at-seat dining in air conditioned
        // coaches; nowadays "Pullman-style" sometimes also refers to open coaches as opposed to
        // compartment stock
        pub pullman: Option<bool>,
        // Spaces for pushchairs available
        pub pushchair: Option<bool>,
        // Spaces for wheelchair users available
        pub wheelchair: Option<bool>,
        // Contains male-only accommodation
        pub has_male_only: Option<bool>,
        // Contains female-only accommodation
        pub has_female_only: Option<bool>,
        // Contains same-sex-only accommodation (traditionally applies to sleeping compartments, but
        // not couchettes, when booked by strangers)
        pub has_same_sex_only: Option<bool>,

        // Relationship to the VariableTrain to which this applies
        #[sea_orm(belongs_to, from = "variable_train_id", to = "id", on_delete = "Cascade")]
        pub variable_train: BelongsTo<super::variable_train::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod line {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "lines")]
    pub struct Model {
        // Represents a "line". What this means varies from place to place, but generally this could
        // be considered a general flow of services; that is trains which depart in roughly one
        // place and go to roughly some other place, sometimes stopping short or starting late, but
        // with a common core section of route. These are often but not always public-facing
        // entities. This may also be used to represent a "service group" on railways without the
        // concept of a line, which is a fairly similar concept but applicable more operationally
        // than to the public.

        // ID of the line in the source data
        #[sea_orm(primary_key)]
        pub id: String,
        // Namespace of the line
        #[sea_orm(primary_key)]
        pub namespace: String,
        // Public-facing ID of the line, if one exists. This may be the line number.
        pub public_id: Option<String>,
        // Human-readable name of the line
        pub name: Option<String>,
        // Human-readable description of the line
        pub description: Option<String>,
        // URL to more information about the line
        pub url: Option<String>,
        // The colour of the line
        pub background_colour: Option<u32>,
        // Contrasting colour of text to be used on top of the line colour
        pub foreground_colour: Option<u32>,

        // Variable trains associated with this line — opposite to usual relationship direction as a
        // Line is a source entity and not one generated per-VariableTrain.
        #[sea_orm(has_many)]
        pub variable_trains: HasMany<super::variable_train::Entity>,
        // Relationship to the parent Schedule
        #[sea_orm(belongs_to, from = "namespace", to = "namespace", on_delete = "Cascade")]
        pub schedule: BelongsTo<super::schedule::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod variable_train {
    use sea_orm::entity::ActiveValue;
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, PartialEq, Serialize)]
    #[sea_orm(table_name = "variable_trains")]
    pub struct Model {
        // Represents the parts of a train's basic metadata which may change en route. This is
        // basically most of the information about a train besides that which affects its running.
        // The parent of this may either be a TrainLocation or a TrainVariant itself.


        // autoinc PK
        #[sea_orm(primary_key)]
        pub id: i64,
        // Namespace of the containing schedule
        pub namespace: String,
        // ID of the parent TrainVariant, if this applies from the start of a journey
        #[sea_orm(indexed)]
        pub train_variant_id: Option<i64>,
        // ID of the parent TrainLocation, if this is a change en route
        #[sea_orm(indexed)]
        pub train_location_id: Option<i64>,
        // ID of the actual allocation of this train, if available
        pub actual_allocation_id: Option<String>,
        // ID of the timing allocation of this train, if available
        pub timing_allocation_id: Option<String>,
        // ID of the line of this train, if available
        pub line_id: Option<String>,
        // ID of the operator of this train, if available
        pub operator_id: Option<String>,
        // The type of service (train or otherwise)
        pub train_type: super::TrainType,
        // The public-facing ID of the train. "Public" may be a stretch in some systems as sometimes
        // train IDs are kept reasonably hidden, but the intent is that this refers to the number
        // the train runs as, rather than some internal schedule ID.
        pub public_id: Option<String>,
        // The headcode of the train. This is specifically something displayed on the front of a
        // train to indicate the destination or route; on many systems this may map to the
        // destination display text; on others, some actual code (whether it is actually displayed
        // or not in practice). Note that this number should identify the _route_, not the
        // individual _train_; use the `public_id` field for that.
        pub headcode: Option<String>,
        // Indicates the broad type of rolling stock and the way said rolling stock running this
        // train is powered.
        pub power_type: Option<super::TrainPower>,
        // Indicates the maximum speed in m/s the train is timed to run at. Again may or may not
        // correspond to the train's actual maximum speed on this route.
        pub timing_speed_m_per_s: Option<f64>,
        // Indicates the brand the train runs as; this may differ from the operator. Brands are
        // often used for subgroups of service.
        pub brand: Option<String>,
        // Indicates the name of the train; this should be used for individually-named trains on
        // networks that still do this.
        pub name: Option<String>,
        // The international (UIC) running number for this train, if it differs from the public ID.
        pub uic_code: Option<String>,
        // Whether or not the train is accessible to wheelchair users, regardless of actual
        // facilities available.
        pub wheelchair_accessible: Option<bool>,

        // OPERATING CHARACTERISTICS (optional)
        // Represents a set of characteristics affecting the operation of the train (as opposed to
        // ones directly affecting passengers). Mostly Britain-centric simply due to the
        // availability of data, and also fairly outdated.
        // Whether or not operating characteristics are available
        pub has_operating_characteristics: bool,
        // Whether or not the train uses vacuum brakes, as opposed to air brakes.
        pub vacuum_braked: Option<bool>,
        // Whether or not the train runs at at least 100mph
        pub one_hundred_mph: Option<bool>,
        // Whether or not the train runs in passenger service without a safety-critical guard
        pub driver_only_passenger: Option<bool>,
        // Whether or not the train runs with British Rail Mk 4 coaching stock
        pub br_mark_four_coaches: Option<bool>,
        // Whether or not the train requires a guard (used eg for freight and ECS moves)
        pub guard_required: Option<bool>,
        // Whether or not the train runs at at least 110mph
        pub one_hundred_and_ten_mph: Option<bool>,
        // Whether or not the train is push-pull (that is, with a locomotive that stays fixed at one
        // end and is controlled by either end)
        pub push_pull: Option<bool>,
        // Whether or not the train runs with air conditioned coaching stock with a public address
        // system
        pub air_conditioned_with_pa: Option<bool>,
        // Whether or not the train uses steam heating, as opposed to electric heating
        pub steam_heat: Option<bool>,
        // Whether or not the train can deviate from its schedule to serve locations as
        // operationally required (mostly applicable to freight)
        pub runs_to_locations_as_required: Option<bool>,
        // Whether or not the train can convey larger containers to SB1C gauge
        pub sb1c_gauge: Option<bool>,

        // RESERVATIONS (optional)
        // Represents information about reservations for each type of travel; depending on the
        // source data different combinations of these might be populated
        // Whether or not reservations data is available
        pub has_reservations: bool,
        // The nature of reservations for all seated accommodation on this train
        pub seats: Option<super::ReservationField>,
        // The nature of reservations for all group bookings on this train
        pub groups: Option<super::ReservationField>,
        // The nature of reservations for all first class accommodation on this train
        pub first_class: Option<super::ReservationField>,
        // The nature of reservations for all second class accommodation on this train
        pub second_class: Option<super::ReservationField>,
        // The nature of reservations for some but not all classes on this train
        pub not_every_class: Option<super::ReservationField>,
        // The nature of reservations for carriage of bicycles on this train
        pub reservations_bicycles: Option<super::ReservationField>,
        // The nature of reservations for all sleeping (including couchette) accommodation on this
        // train
        pub sleepers: Option<super::ReservationField>,
        // The nature of reservations for carriage of vehicles (eg cars) on this train
        pub reservations_vehicles: Option<super::ReservationField>,
        // The nature of reservations for wheelchair users on this train
        pub wheelchairs: Option<super::ReservationField>,
        // Whether or not a supplement is charged for reservations
        pub supplement_charged: Option<bool>,

        // CATERING (optional)
        // Represents the catering available
        // Whether or not catering data is available
        pub has_catering: bool,
        // An at-seat meal
        pub at_seat_meal: Option<bool>,
        // A bar
        pub bar: Option<bool>,
        // A bistro
        pub bistro: Option<bool>,
        // Breakfast served to your car (most often applicable for sleeper trains)
        pub breakfast_in_car: Option<bool>,
        // A buffet
        pub buffet: Option<bool>,
        // A coffee shop
        pub coffee_shop: Option<bool>,
        // Self-service catering of some description
        pub self_service: Option<bool>,
        // A trolley
        pub trolley: Option<bool>,
        // A vending machine selling food
        pub vending_machine_food: Option<bool>,
        // A vending machine selling drinks
        pub vending_machine_drink: Option<bool>,
        // A minibar
        pub mini_bar: Option<bool>,
        // A restaurant, that is a sit-down meal served in a specific part of the train
        pub restaurant: Option<bool>,
        // A restaurant for first class passengers only
        pub first_class_restaurant: Option<bool>,
        // An at-seat meal for first-class passengers only
        pub first_class_meal: Option<bool>,
        // Another form of catering
        pub catering_other: Option<bool>,
        // Indicates whether or not food is available with the above catering options
        pub food_available: Option<bool>,
        // Indicates whether or not hot food is available with the above catering options
        pub hot_food_available: Option<bool>,
        // Indicates whether or not drink is available with the above catering options
        pub drink_available: Option<bool>,
        // Indicates whether or not snacks are available with the above catering options
        pub snacks_available: Option<bool>,

        // TOILETS (optional)
        // Represents information about the toilet facilities on a train. All are optional; None
        // means "unknown", false means "not present", true means "present".
        // Whether or not toilets data is available
        pub has_toilets: bool,
        // Toilet available
        pub toilet: Option<bool>,
        // Sink (washbasin) available, separate to toilet
        pub sink: Option<bool>,
        // Disabled toilet available
        pub disabled_toilet: Option<bool>,
        // Shower available
        pub shower: Option<bool>,
        // Changing room available
        pub changing: Option<bool>,
        // Baby changing facilities available
        pub baby_changing: Option<bool>,
        // Disabled baby changing facilities available
        pub disabled_baby_changing: Option<bool>,
        // Shoe shiner available
        pub shoe_shiner: Option<bool>,
        // Other toilet facilities available
        pub toilets_other: Option<bool>,

        // LUGGAGE (optional)
        // Represents information about the luggage passengers may take on a train and the
        // facilities available for storing it. All are optional; None means "unknown", false means
        // "not present", true means "present".
        // Whether or not luggage data is available
        pub has_luggage: bool,
        // Storage for bags available
        pub bag_storage: Option<bool>,
        // Luggage racks available
        pub racks: Option<bool>,
        // Conveyance of skis possible
        pub skis: Option<bool>,
        // Conveyance of skis on the rear of the train possible
        pub skis_on_rear: Option<bool>,
        // Extra large luggage racks available
        pub extra_large_racks: Option<bool>,
        // Luggage van (separate coach or area of a coach closed off to passengers for conveying
        // luggage) available
        pub van: Option<bool>,
        // Conveyance of bicycles possible
        pub luggage_bicycles: Option<bool>,
        // Conveyance of bicycles possible in the luggage van
        pub bicycles_in_van: Option<bool>,
        // Conveyance of bicycles possible in the carriage
        pub bicycles_in_carriage: Option<bool>,
        // Conveyance of pushchairs possible
        pub pushchairs: Option<bool>,
        // Conveyance of automotive vehicles possible
        pub luggage_vehicles: Option<bool>,

        // FAMILIES (optional)
        // Represents information about the facilities available on the train for families. All are
        // optional; None means "unknown", false means "not present", true means "present".
        // Whether or not families data is available
        pub has_families: bool,
        // Facilities specifically for children are available
        pub children_facilities: Option<bool>,
        // Facilities for military families available(!)
        pub military_family_facilities: Option<bool>,
        // Nursery available
        pub nursery: Option<bool>,

        // PASSENGER COMMUNICATIONS (optional)
        // Represents information about the facilities available on the train for passengers to
        // communicate. All are optional; None means "unknown", false means "not present", true
        // means "present".
        // Whether or not passenger communications data is available
        pub has_passenger_communications: bool,
        // Internet access via WiFi is available for free
        pub free_wifi: Option<bool>,
        // Internet access via WiFi is available
        pub wifi: Option<bool>,
        // Mains sockets are available
        pub mains_sockets: Option<bool>,
        // A telephone is available
        pub telephone: Option<bool>,
        // Audio-based entertainment is available
        pub radio: Option<bool>,
        // Audiovisual entertainment is available
        pub video: Option<bool>,
        // Business facilities are available (what these might be I can't imagine)
        pub business: Option<bool>,
        // Internet access is available (I guess not via WiFi?)
        pub internet: Option<bool>,
        // A travelling post office is available
        pub post_office: Option<bool>,
        // A postbox is available
        pub postbox: Option<bool>,
        // USB-A sockets are available for delivering power
        pub usb_a: Option<bool>,
        // USB-C sockets are available for delivering power
        pub usb_c: Option<bool>,
        // Another form of communication is available
        pub passenger_communications_other: Option<bool>,

        // ASSISTANCE (optional)
        // Represents information about the assistance available to passengers. All are optional;
        // None means "unknown", false means "not present", true means "present".
        // Whether or not assistance data is available
        pub has_assistance: bool,
        // Personal assistance is available
        pub personal: Option<bool>,
        // Assistance to board the train is available
        pub boarding: Option<bool>,
        // Assistance for passengers using wheelchairs is available
        pub wheelchair: Option<bool>,
        // Assistance for unaccompanied minors is available
        pub unaccompanied_minor: Option<bool>,
        // The use of a wheelchair is available
        pub use_of_wheelchair: Option<bool>,
        // A guard/conductor/etc. is available
        pub guard: Option<bool>,
        // Passenger information is available
        pub information: Option<bool>,
        // Another form of assistance is available
        pub assistance_other: Option<bool>,

        // PASSENGER INFORMATION (optional)
        // Represents information about the information available to passengers, including
        // accessible information. All are optional; None means "unknown", false means "not
        // present", true means "present".
        // Whether or not passenger information data is available
        pub has_passenger_information: bool,
        // Indicators to show the next stop are available
        pub next_stop_indication: Option<bool>,
        // Announcements of each stop are available
        pub stop_announcements: Option<bool>,
        // Displays showing information are available
        pub information_display: Option<bool>,
        // Displays showing realtime connection information are available
        pub realtime_connections: Option<bool>,
        // Audible information is available
        pub audible_information: Option<bool>,
        // Audible information is available with accessibility features for the hearing impaired
        pub hearing_impaired_audible_information: Option<bool>,
        // Visible information is available
        pub visible_information: Option<bool>,
        // Visible information is available with accessibility features for the visually impaired
        pub visually_impaired_visible_information: Option<bool>,
        // Large print timetables are available
        pub large_print_timetable: Option<bool>,
        // Another form of information is available
        pub passenger_information_other: Option<bool>,

        // Relationship to VariableTrains for actual allocations. Indicates the rolling stock
        // allocated to run this service in the schedule.
        #[sea_orm(
            belongs_to,
            relation_enum = "Actual",
            from = "(actual_allocation_id, namespace)",
            to = "(id, namespace)",
            on_delete = "Cascade",
        )]
        pub actual_allocation: BelongsTo<Option<super::train_allocation::Entity>>,
        // Relationship to VariableTrains for timing allocations. Indicates the nominal rolling
        // stock used for timing purposes when planning this train. May or may not match what is
        // intended to actually run the train.
        #[sea_orm(
            belongs_to,
            relation_enum = "Timing",
            from = "(timing_allocation_id, namespace)",
            to = "(id, namespace)",
            on_delete = "Cascade",
        )]
        pub timing_allocation: BelongsTo<Option<super::train_allocation::Entity>>,
        // Relationship to AccommodationTypes of this train
        #[sea_orm(has_many)]
        pub accommodation: HasMany<super::accommodation_types::Entity>,
        // Relationship to the Line of this train
        #[sea_orm(belongs_to, from = "(line_id, namespace)", to = "(id, namespace)", on_delete = "Cascade")]
        pub line: BelongsTo<Option<super::line::Entity>>,
        // Relationship to the Operator of this train
        #[sea_orm(belongs_to, from = "(operator_id, namespace)", to = "(id, namespace)", on_delete = "Cascade")]
        pub operator: BelongsTo<Option<super::train_operator::Entity>>,
        // Relationship to parent TrainVariant
        #[sea_orm(belongs_to, from = "train_variant_id", to = "id", on_delete = "Cascade")]
        pub train_variant: BelongsTo<Option<super::train_variant::Entity>>,
        // Relationship to parent TrainLocation
        #[sea_orm(belongs_to, from = "train_location_id", to = "id", on_delete = "Cascade")]
        pub train_location: BelongsTo<Option<super::train_location::Entity>>,
    }

    impl ActiveModelBehavior for ActiveModel {}

    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct OperatingCharacteristics {
        // Whether or not the train uses vacuum brakes, as opposed to air brakes.
        pub vacuum_braked: bool,
        // Whether or not the train runs at at least 100mph
        pub one_hundred_mph: bool,
        // Whether or not the train runs in passenger service without a safety-critical guard
        pub driver_only_passenger: bool,
        // Whether or not the train runs with British Rail Mk 4 coaching stock
        pub br_mark_four_coaches: bool,
        // Whether or not the train requires a guard (used eg for freight and ECS moves)
        pub guard_required: bool,
        // Whether or not the train runs at at least 110mph
        pub one_hundred_and_ten_mph: bool,
        // Whether or not the train is push-pull (that is, with a locomotive that stays fixed at one
        // end and is controlled by either end)
        pub push_pull: bool,
        // Whether or not the train runs with air conditioned coaching stock with a public address
        // system
        pub air_conditioned_with_pa: bool,
        // Whether or not the train uses steam heating, as opposed to electric heating
        pub steam_heat: bool,
        // Whether or not the train can deviate from its schedule to serve locations as
        // operationally required (mostly applicable to freight)
        pub runs_to_locations_as_required: bool,
        // Whether or not the train can convey larger containers to SB1C gauge
        pub sb1c_gauge: bool,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct Reservations {
        // The nature of reservations for all seated accommodation on this train
        pub seats: super::ReservationField,
        // The nature of reservations for all group bookings on this train
        pub groups: super::ReservationField,
        // The nature of reservations for all first class accommodation on this train
        pub first_class: super::ReservationField,
        // The nature of reservations for all second class accommodation on this train
        pub second_class: super::ReservationField,
        // The nature of reservations for some but not all classes on this train
        pub not_every_class: super::ReservationField,
        // The nature of reservations for carriage of bicycles on this train
        pub bicycles: super::ReservationField,
        // The nature of reservations for all sleeping (including couchette) accommodation on this
        // train
        pub sleepers: super::ReservationField,
        // The nature of reservations for carriage of vehicles (eg cars) on this train
        pub vehicles: super::ReservationField,
        // The nature of reservations for wheelchair users on this train
        pub wheelchairs: super::ReservationField,
        // Whether or not a supplement is charged for reservations
        pub supplement_charged: Option<bool>,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct Catering {
        // An at-seat meal
        pub at_seat_meal: bool,
        // A bar
        pub bar: bool,
        // A bistro
        pub bistro: bool,
        // Breakfast served to your car (most often applicable for sleeper trains)
        pub breakfast_in_car: bool,
        // A buffet
        pub buffet: bool,
        // A coffee shop
        pub coffee_shop: bool,
        // Self-service catering of some description
        pub self_service: bool,
        // A trolley
        pub trolley: bool,
        // A vending machine selling food
        pub vending_machine_food: bool,
        // A vending machine selling drinks
        pub vending_machine_drink: bool,
        // A minibar
        pub mini_bar: bool,
        // A restaurant, that is a sit-down meal served in a specific part of the train
        pub restaurant: bool,
        // A restaurant for first class passengers only
        pub first_class_restaurant: bool,
        // An at-seat meal for first-class passengers only
        pub first_class_meal: bool,
        // Another form of catering
        pub other: bool,
        // Indicates whether or not food is available with the above catering options
        pub food_available: Option<bool>,
        // Indicates whether or not hot food is available with the above catering options
        pub hot_food_available: Option<bool>,
        // Indicates whether or not drink is available with the above catering options
        pub drink_available: Option<bool>,
        // Indicates whether or not snacks are available with the above catering options
        pub snacks_available: Option<bool>,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct Toilets {
        // Toilet available
        pub toilet: Option<bool>,
        // Sink (washbasin) available, separate to toilet
        pub sink: Option<bool>,
        // Disabled toilet available
        pub disabled_toilet: Option<bool>,
        // Shower available
        pub shower: Option<bool>,
        // Changing room available
        pub changing: Option<bool>,
        // Baby changing facilities available
        pub baby_changing: Option<bool>,
        // Disabled baby changing facilities available
        pub disabled_baby_changing: Option<bool>,
        // Shoe shiner available
        pub shoe_shiner: Option<bool>,
        // Other toilet facilities available
        pub other: Option<bool>,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct Luggage {
        // Storage for bags available
        pub bag_storage: Option<bool>,
        // Luggage racks available
        pub racks: Option<bool>,
        // Conveyance of skis possible
        pub skis: Option<bool>,
        // Conveyance of skis on the rear of the train possible
        pub skis_on_rear: Option<bool>,
        // Extra large luggage racks available
        pub extra_large_racks: Option<bool>,
        // Luggage van (separate coach or area of a coach closed off to passengers for conveying
        // luggage) available
        pub van: Option<bool>,
        // Conveyance of bicycles possible
        pub bicycles: Option<bool>,
        // Conveyance of bicycles possible in the luggage van
        pub bicycles_in_van: Option<bool>,
        // Conveyance of bicycles possible in the carriage
        pub bicycles_in_carriage: Option<bool>,
        // Conveyance of pushchairs possible
        pub pushchairs: Option<bool>,
        // Conveyance of automotive vehicles possible
        pub vehicles: Option<bool>,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct Families {
        // Facilities specifically for children are available
        pub children_facilities: Option<bool>,
        // Facilities for military families available(!)
        pub military_family_facilities: Option<bool>,
        // Nursery available
        pub nursery: Option<bool>,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct PassengerCommunications {
        // Internet access via WiFi is available for free
        pub free_wifi: Option<bool>,
        // Internet access via WiFi is available
        pub wifi: Option<bool>,
        // Mains sockets are available
        pub mains_sockets: Option<bool>,
        // A telephone is available
        pub telephone: Option<bool>,
        // Audio-based entertainment is available
        pub radio: Option<bool>,
        // Audiovisual entertainment is available
        pub video: Option<bool>,
        // Business facilities are available (what these might be I can't imagine)
        pub business: Option<bool>,
        // Internet access is available (I guess not via WiFi?)
        pub internet: Option<bool>,
        // A travelling post office is available
        pub post_office: Option<bool>,
        // A postbox is available
        pub postbox: Option<bool>,
        // USB-A sockets are available for delivering power
        pub usb_a: Option<bool>,
        // USB-C sockets are available for delivering power
        pub usb_c: Option<bool>,
        // Another form of communication is available
        pub other: Option<bool>,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct Assistance {
        // Personal assistance is available
        pub personal: Option<bool>,
        // Assistance to board the train is available
        pub boarding: Option<bool>,
        // Assistance for passengers using wheelchairs is available
        pub wheelchair: Option<bool>,
        // Assistance for unaccompanied minors is available
        pub unaccompanied_minor: Option<bool>,
        // The use of a wheelchair is available
        pub use_of_wheelchair: Option<bool>,
        // A guard/conductor/etc. is available
        pub guard: Option<bool>,
        // Passenger information is available
        pub information: Option<bool>,
        // Another form of assistance is available
        pub other: Option<bool>,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct PassengerInformation {
        // Indicators to show the next stop are available
        pub next_stop_indication: Option<bool>,
        // Announcements of each stop are available
        pub stop_announcements: Option<bool>,
        // Displays showing information are available
        pub information_display: Option<bool>,
        // Displays showing realtime connection information are available
        pub realtime_connections: Option<bool>,
        // Audible information is available
        pub audible_information: Option<bool>,
        // Audible information is available with accessibility features for the hearing impaired
        pub hearing_impaired_audible_information: Option<bool>,
        // Visible information is available
        pub visible_information: Option<bool>,
        // Visible information is available with accessibility features for the visually impaired
        pub visually_impaired_visible_information: Option<bool>,
        // Large print timetables are available
        pub large_print_timetable: Option<bool>,
        // Another form of information is available
        pub other: Option<bool>,
    }

    impl OperatingCharacteristics {
        pub fn get_from_model(m: &ModelEx) -> Option<Self> {
            if m.has_operating_characteristics {
                Some(Self {
                    vacuum_braked: m.vacuum_braked.unwrap(),
                    one_hundred_mph: m.one_hundred_mph.unwrap(),
                    driver_only_passenger: m.driver_only_passenger.unwrap(),
                    br_mark_four_coaches: m.br_mark_four_coaches.unwrap(),
                    guard_required: m.guard_required.unwrap(),
                    one_hundred_and_ten_mph: m.one_hundred_and_ten_mph.unwrap(),
                    push_pull: m.push_pull.unwrap(),
                    air_conditioned_with_pa: m.air_conditioned_with_pa.unwrap(),
                    steam_heat: m.steam_heat.unwrap(),
                    runs_to_locations_as_required: m.runs_to_locations_as_required.unwrap(),
                    sb1c_gauge: m.sb1c_gauge.unwrap(),
                })
            } else {
                None
            }
        }
    }

    impl Reservations {
        pub fn get_from_model(m: &ModelEx) -> Option<Self> {
            if m.has_reservations {
                Some(Self {
                    seats: m.seats.unwrap(),
                    groups: m.groups.unwrap(),
                    first_class: m.first_class.unwrap(),
                    second_class: m.second_class.unwrap(),
                    not_every_class: m.not_every_class.unwrap(),
                    bicycles: m.reservations_bicycles.unwrap(),
                    sleepers: m.sleepers.unwrap(),
                    vehicles: m.reservations_vehicles.unwrap(),
                    wheelchairs: m.wheelchairs.unwrap(),
                    supplement_charged: m.supplement_charged,
                })
            } else {
                None
            }
        }
    }

    impl Catering {
        pub fn get_from_model(m: &ModelEx) -> Option<Self> {
            if m.has_catering {
                Some(Self {
                    at_seat_meal: m.at_seat_meal.unwrap(),
                    bar: m.bar.unwrap(),
                    bistro: m.bistro.unwrap(),
                    breakfast_in_car: m.breakfast_in_car.unwrap(),
                    buffet: m.buffet.unwrap(),
                    coffee_shop: m.coffee_shop.unwrap(),
                    self_service: m.self_service.unwrap(),
                    trolley: m.trolley.unwrap(),
                    vending_machine_food: m.vending_machine_food.unwrap(),
                    vending_machine_drink: m.vending_machine_drink.unwrap(),
                    mini_bar: m.mini_bar.unwrap(),
                    restaurant: m.restaurant.unwrap(),
                    first_class_restaurant: m.first_class_restaurant.unwrap(),
                    first_class_meal: m.first_class_meal.unwrap(),
                    other: m.catering_other.unwrap(),
                    food_available: m.food_available,
                    hot_food_available: m.hot_food_available,
                    drink_available: m.drink_available,
                    snacks_available: m.snacks_available,
                })
            } else {
                None
            }
        }
    }

    impl Toilets {
        pub fn get_from_model(m: &ModelEx) -> Option<Self> {
            if m.has_toilets {
                Some(Self {
                    toilet: m.toilet,
                    sink: m.sink,
                    disabled_toilet: m.disabled_toilet,
                    shower: m.shower,
                    changing: m.changing,
                    baby_changing: m.baby_changing,
                    disabled_baby_changing: m.disabled_baby_changing,
                    shoe_shiner: m.shoe_shiner,
                    other: m.toilets_other,
                })
            } else {
                None
            }
        }
    }

    impl Luggage {
        pub fn get_from_model(m: &ModelEx) -> Option<Self> {
            if m.has_luggage {
                Some(Self {
                    bag_storage: m.bag_storage,
                    racks: m.racks,
                    skis: m.skis,
                    skis_on_rear: m.skis_on_rear,
                    extra_large_racks: m.extra_large_racks,
                    van: m.van,
                    bicycles: m.luggage_bicycles,
                    bicycles_in_van: m.bicycles_in_van,
                    bicycles_in_carriage: m.bicycles_in_carriage,
                    pushchairs: m.pushchairs,
                    vehicles: m.luggage_vehicles,
                })
            } else {
                None
            }
        }
    }

    impl Families {
        pub fn get_from_model(m: &ModelEx) -> Option<Self> {
            if m.has_families {
                Some(Self {
                    children_facilities: m.children_facilities,
                    military_family_facilities: m.military_family_facilities,
                    nursery: m.nursery,
                })
            } else {
                None
            }
        }
    }

    impl PassengerCommunications {
        pub fn get_from_model(m: &ModelEx) -> Option<Self> {
            if m.has_passenger_communications {
                Some(Self {
                    free_wifi: m.free_wifi,
                    wifi: m.wifi,
                    mains_sockets: m.mains_sockets,
                    telephone: m.telephone,
                    radio: m.radio,
                    video: m.video,
                    business: m.business,
                    internet: m.internet,
                    post_office: m.post_office,
                    postbox: m.postbox,
                    usb_a: m.usb_a,
                    usb_c: m.usb_c,
                    other: m.passenger_communications_other,
                })
            } else {
                None
            }
        }
    }

    impl Assistance {
        pub fn get_from_model(m: &ModelEx) -> Option<Self> {
            if m.has_assistance {
                Some(Self {
                    personal: m.personal,
                    boarding: m.boarding,
                    wheelchair: m.wheelchair,
                    unaccompanied_minor: m.unaccompanied_minor,
                    use_of_wheelchair: m.use_of_wheelchair,
                    guard: m.guard,
                    information: m.information,
                    other: m.assistance_other,
                })
            } else {
                None
            }
        }
    }

    impl PassengerInformation {
        pub fn get_from_model(m: &ModelEx) -> Option<Self> {
            if m.has_passenger_information {
                Some(Self {
                    next_stop_indication: m.next_stop_indication,
                    stop_announcements: m.stop_announcements,
                    information_display: m.information_display,
                    realtime_connections: m.realtime_connections,
                    audible_information: m.audible_information,
                    hearing_impaired_audible_information: m.hearing_impaired_audible_information,
                    visible_information: m.visible_information,
                    visually_impaired_visible_information: m.visually_impaired_visible_information,
                    large_print_timetable: m.large_print_timetable,
                    other: m.passenger_information_other,
                })
            } else {
                None
            }
        }
    }

    impl ActiveModelEx {
        pub fn populate_operating_characteristics(&mut self, o: &OperatingCharacteristics) -> () {
            self.has_operating_characteristics = ActiveValue::Set(true);
            self.vacuum_braked = ActiveValue::Set(Some(o.vacuum_braked));
            self.one_hundred_mph = ActiveValue::Set(Some(o.one_hundred_mph));
            self.driver_only_passenger = ActiveValue::Set(Some(o.driver_only_passenger));
            self.br_mark_four_coaches = ActiveValue::Set(Some(o.br_mark_four_coaches));
            self.guard_required = ActiveValue::Set(Some(o.guard_required));
            self.one_hundred_and_ten_mph = ActiveValue::Set(Some(o.one_hundred_and_ten_mph));
            self.push_pull = ActiveValue::Set(Some(o.push_pull));
            self.air_conditioned_with_pa = ActiveValue::Set(Some(o.air_conditioned_with_pa));
            self.steam_heat = ActiveValue::Set(Some(o.steam_heat));
            self.runs_to_locations_as_required
                = ActiveValue::Set(Some(o.runs_to_locations_as_required));
            self.sb1c_gauge = ActiveValue::Set(Some(o.sb1c_gauge));
        }

        pub fn populate_reservations(&mut self, r: &Reservations) -> () {
            self.has_reservations = ActiveValue::Set(true);
            self.seats = ActiveValue::Set(Some(r.seats));
            self.groups = ActiveValue::Set(Some(r.groups));
            self.first_class = ActiveValue::Set(Some(r.first_class));
            self.second_class = ActiveValue::Set(Some(r.second_class));
            self.not_every_class = ActiveValue::Set(Some(r.not_every_class));
            self.reservations_bicycles = ActiveValue::Set(Some(r.bicycles));
            self.sleepers = ActiveValue::Set(Some(r.sleepers));
            self.reservations_vehicles = ActiveValue::Set(Some(r.vehicles));
            self.wheelchairs = ActiveValue::Set(Some(r.wheelchairs));
            self.supplement_charged = ActiveValue::Set(r.supplement_charged);
        }

        pub fn populate_catering(&mut self, c: &Catering) -> () {
            self.has_catering = ActiveValue::Set(true);
            self.at_seat_meal = ActiveValue::Set(Some(c.at_seat_meal));
            self.bar = ActiveValue::Set(Some(c.bar));
            self.bistro = ActiveValue::Set(Some(c.bistro));
            self.breakfast_in_car = ActiveValue::Set(Some(c.breakfast_in_car));
            self.buffet = ActiveValue::Set(Some(c.buffet));
            self.coffee_shop = ActiveValue::Set(Some(c.coffee_shop));
            self.self_service = ActiveValue::Set(Some(c.self_service));
            self.trolley = ActiveValue::Set(Some(c.trolley));
            self.vending_machine_food = ActiveValue::Set(Some(c.vending_machine_food));
            self.vending_machine_drink = ActiveValue::Set(Some(c.vending_machine_drink));
            self.mini_bar = ActiveValue::Set(Some(c.mini_bar));
            self.restaurant = ActiveValue::Set(Some(c.restaurant));
            self.first_class_restaurant = ActiveValue::Set(Some(c.first_class_restaurant));
            self.first_class_meal = ActiveValue::Set(Some(c.first_class_meal));
            self.catering_other = ActiveValue::Set(Some(c.other));
            self.food_available = ActiveValue::Set(c.food_available);
            self.hot_food_available = ActiveValue::Set(c.hot_food_available);
            self.drink_available = ActiveValue::Set(c.drink_available);
            self.snacks_available = ActiveValue::Set(c.snacks_available);
        }

        pub fn populate_toilets(&mut self, t: &Toilets) -> () {
            self.has_toilets = ActiveValue::Set(true);
            self.toilet = ActiveValue::Set(t.toilet);
            self.sink = ActiveValue::Set(t.sink);
            self.disabled_toilet = ActiveValue::Set(t.disabled_toilet);
            self.shower = ActiveValue::Set(t.shower);
            self.changing = ActiveValue::Set(t.changing);
            self.baby_changing = ActiveValue::Set(t.baby_changing);
            self.disabled_baby_changing = ActiveValue::Set(t.disabled_baby_changing);
            self.shoe_shiner = ActiveValue::Set(t.shoe_shiner);
            self.toilets_other = ActiveValue::Set(t.other);
        }

        pub fn populate_luggage(&mut self, l: &Luggage) -> () {
            self.has_luggage = ActiveValue::Set(true);
            self.bag_storage = ActiveValue::Set(l.bag_storage);
            self.racks = ActiveValue::Set(l.racks);
            self.skis = ActiveValue::Set(l.skis);
            self.skis_on_rear = ActiveValue::Set(l.skis_on_rear);
            self.extra_large_racks = ActiveValue::Set(l.extra_large_racks);
            self.van = ActiveValue::Set(l.van);
            self.luggage_bicycles = ActiveValue::Set(l.bicycles);
            self.bicycles_in_van = ActiveValue::Set(l.bicycles_in_van);
            self.bicycles_in_carriage = ActiveValue::Set(l.bicycles_in_carriage);
            self.pushchairs = ActiveValue::Set(l.pushchairs);
            self.luggage_vehicles = ActiveValue::Set(l.vehicles);
        }

        pub fn populate_families(&mut self, f: &Families) -> () {
            self.has_families = ActiveValue::Set(true);
            self.children_facilities = ActiveValue::Set(f.children_facilities);
            self.military_family_facilities = ActiveValue::Set(f.military_family_facilities);
            self.nursery = ActiveValue::Set(f.nursery);
        }

        pub fn populate_passenger_communications(&mut self, p: &PassengerCommunications) -> () {
            self.has_passenger_communications = ActiveValue::Set(true);
            self.free_wifi = ActiveValue::Set(p.free_wifi);
            self.wifi = ActiveValue::Set(p.wifi);
            self.mains_sockets = ActiveValue::Set(p.mains_sockets);
            self.telephone = ActiveValue::Set(p.telephone);
            self.radio = ActiveValue::Set(p.radio);
            self.video = ActiveValue::Set(p.video);
            self.business = ActiveValue::Set(p.business);
            self.internet = ActiveValue::Set(p.internet);
            self.post_office = ActiveValue::Set(p.post_office);
            self.postbox = ActiveValue::Set(p.postbox);
            self.usb_a = ActiveValue::Set(p.usb_a);
            self.usb_c = ActiveValue::Set(p.usb_c);
            self.passenger_communications_other = ActiveValue::Set(p.other);
        }

        pub fn populate_assistance(&mut self, a: &Assistance) -> () {
            self.has_assistance = ActiveValue::Set(true);
            self.personal = ActiveValue::Set(a.personal);
            self.boarding = ActiveValue::Set(a.boarding);
            self.wheelchair = ActiveValue::Set(a.wheelchair);
            self.unaccompanied_minor = ActiveValue::Set(a.unaccompanied_minor);
            self.use_of_wheelchair = ActiveValue::Set(a.use_of_wheelchair);
            self.guard = ActiveValue::Set(a.guard);
            self.information = ActiveValue::Set(a.information);
            self.assistance_other = ActiveValue::Set(a.other);
        }

        pub fn populate_passenger_information(&mut self, p: &PassengerInformation) -> () {
            self.has_passenger_information = ActiveValue::Set(true);
            self.next_stop_indication = ActiveValue::Set(p.next_stop_indication);
            self.stop_announcements = ActiveValue::Set(p.stop_announcements);
            self.information_display = ActiveValue::Set(p.information_display);
            self.realtime_connections = ActiveValue::Set(p.realtime_connections);
            self.audible_information = ActiveValue::Set(p.audible_information);
            self.hearing_impaired_audible_information
                = ActiveValue::Set(p.hearing_impaired_audible_information);
            self.visible_information = ActiveValue::Set(p.visible_information);
            self.visually_impaired_visible_information
                = ActiveValue::Set(p.visually_impaired_visible_information);
            self.large_print_timetable = ActiveValue::Set(p.large_print_timetable);
            self.passenger_information_other = ActiveValue::Set(p.other);
        }
    }
}

pub mod train_variant {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "train_variants")]
    pub struct Model {
        // Represents a variant of a train with a given ID. These are normally used for minor
        // variations of the same train which might run in a different validity period. It is not
        // valid to have two TrainVariants belonging to the same Train which are set to run on the
        // same day. Not all schedule sources will do this; others might have them run as different
        // trains, in which case there will simply be one of these per train.

        // autoinc PK
        #[sea_orm(primary_key)]
        pub id: i64,
        // ID of the parent train — not set if this is a replacement
        pub train_id: Option<String>,
        // namespace of the parent train (always set for simplicity)
        pub namespace: String,
        // Indicates the parent train, if this train replaces another.
        #[sea_orm(indexed)]
        pub parent_train_variant_id: Option<i64>,
        // Indicates the source of this TrainVariant; see the documentation there for the full
        // meaning.
        pub source: Option<super::TrainSource>,
        // If true, the train will run only when required (most commonly used for freight and other
        // non-passenger moves)
        pub runs_as_required: bool,
        // If true, the train's performance will be monitored to account for delays
        pub performance_monitoring: Option<bool>,

        // Relationship to the validity periods of this TrainVariant
        #[sea_orm(has_many)]
        pub validity: HasMany<super::train_validity_period::Entity>,
        // Relationship to the cancellations of this train; these are periods during which this
        // TrainVariant does not run, even during its validity period.
        #[sea_orm(has_many)]
        pub cancellations: HasMany<super::train_cancellation::Entity>,
        // Relationship to the replacements for this train; these are trains that override this one
        // during their validity periods, even though they are a part of this train's validity
        // period.
        #[sea_orm(has_many, relation_enum = "TrainReplacement", via_rel = "Replacement")]
        pub replacements: HasMany<super::train_variant::Entity>,
        // Relationship to the parent TrainVariant if this is a replacement
        #[sea_orm(
            belongs_to, relation_enum = "Replacement", from = "parent_train_variant_id", to = "id",
            on_delete = "Cascade"
        )]
        pub parent: BelongsTo<Option<super::train_variant::Entity>>,
        // Relationship to the VariableTrain that is in force at the start of the journey;
        // mandatory.
        #[sea_orm(has_one)]
        pub variable_train: HasOne<super::variable_train::Entity>,
        // Relationship to the locations representing the route of the train. You MUST order by
        // `index` to get the correct order.
        #[sea_orm(has_many)]
        pub route: HasMany<super::train_location::Entity>,
        // Relationship to the parent Train
        #[sea_orm(belongs_to, from = "(train_id, namespace)", to = "(id, namespace)", on_delete = "Cascade")]
        pub train: BelongsTo<Option<super::train::Entity>>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod train_cancellation {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "train_cancellations")]
    pub struct Model {
        // Represents a period of cancellation of a train variant, in which the train variant is not
        // valid despite being inside its validity period.

        // autoinc PK
        #[sea_orm(primary_key)]
        pub id: i64,
        // Indicates the train variant being cancelled
        #[sea_orm(indexed)]
        pub train_variant_id: i64,
        // Indicates the source of the cancellation; see the documentation there for the full
        // meaning.
        pub source: Option<super::TrainSource>,

        // Relationship to the validity periods of this cancellation
        #[sea_orm(has_many)]
        pub validity: HasMany<super::train_validity_period::Entity>,
        // Relationship to the parent TrainVariant
        #[sea_orm(belongs_to, from = "train_variant_id", to = "id", on_delete = "Cascade")]
        pub train_variant: BelongsTo<super::train_variant::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod train {
    use sea_orm::entity::prelude::*;

    use serde::Serialize;

    #[sea_orm::model]
    #[derive(Clone, Debug, DeriveEntityModel, Eq, PartialEq, Serialize)]
    #[sea_orm(table_name = "trains")]
    pub struct Model {
        // Represents a train (or other non-train but railway-relevant service) from the source
        // schedule. A train with a single ID might run differently on different days in some source
        // schedules; for this reason, there is no data in here except a relationship with
        // `train_variant` which contains the actual runs of the train. TrainVariants must be
        // non-overlapping; that is there must be exactly 0 or 1 TrainVariants that run for a given
        // Train on any given day.

        // The ID of the train from the source schedule
        #[sea_orm(primary_key)]
        pub id: String,
        // The namespace of the train's schedule
        #[sea_orm(primary_key)]
        pub namespace: String,

        // Relationship to the parent Schedule
        #[sea_orm(belongs_to, from = "namespace", to = "namespace", on_delete = "Cascade")]
        pub schedule: BelongsTo<super::schedule::Entity>,
        // Relationship to the variants of this train
        #[sea_orm(has_many)]
        pub train_variants: HasMany<super::train_variant::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}
