use chrono::{DateTime, NaiveTime, Weekday};
use chrono_tz::Tz;

use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::collections::HashSet;

#[derive(Clone, Debug, Serialize)]
pub struct Schedule {
    pub locations: HashMap<String, Location>,
    pub trains: HashMap<String, Vec<Train>>, // one ID could have multiple permanent schedules on
    // different dates
    pub namespace: String,   // this is defined by me
    pub description: String, // what this schedule actually is, again defined by me
    pub their_id: Option<String>,
    pub valid_begin: Option<DateTime<Tz>>,
    pub valid_end: Option<DateTime<Tz>>,
    pub last_updated: Option<DateTime<Tz>>,
    pub trains_indexed_by_location: HashMap<String, HashSet<String>>,
    pub trains_indexed_by_public_id: HashMap<String, HashSet<String>>,
    pub locations_indexed_by_public_id: HashMap<String, HashSet<String>>,
}

impl Schedule {
    pub fn new(namespace: String, description: String) -> Self {
        Self {
            locations: HashMap::new(),
            trains: HashMap::new(),
            namespace,
            description,
            their_id: None,
            valid_begin: None,
            valid_end: None,
            last_updated: None,
            trains_indexed_by_location: HashMap::new(),
            trains_indexed_by_public_id: HashMap::new(),
            locations_indexed_by_public_id: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Location {
    pub id: String,
    pub name: String,
    pub public_id: Option<String>, // some countries have an internal ID for planning and a public
    // ID for retail; we should expose the public one.
    pub timezone: Tz,
}

#[derive(Clone, Debug, Serialize)]
pub struct TrainValidityPeriod {
    pub valid_begin: DateTime<Tz>,
    pub valid_end: DateTime<Tz>,
    pub days_of_week: DaysOfWeek,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct DaysOfWeek {
    pub monday: bool,
    pub tuesday: bool,
    pub wednesday: bool,
    pub thursday: bool,
    pub friday: bool,
    pub saturday: bool,
    pub sunday: bool,
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

    pub fn from_single_weekday(weekday: Weekday) -> DaysOfWeek {
        let mut days = DaysOfWeek {
            monday: false,
            tuesday: false,
            wednesday: false,
            thursday: false,
            friday: false,
            saturday: false,
            sunday: false,
        };
        match weekday {
            Weekday::Mon => days.monday = true,
            Weekday::Tue => days.tuesday = true,
            Weekday::Wed => days.wednesday = true,
            Weekday::Thu => days.thursday = true,
            Weekday::Fri => days.friday = true,
            Weekday::Sat => days.saturday = true,
            Weekday::Sun => days.sunday = true,
        }

        days
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

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub enum TrainType {
    Bus,
    ServiceBus,
    ReplacementBus,
    Freight,
    FreightDepartmental,
    FreightCivilEngineer,
    FreightMechanicalElectricalEngineer,
    FreightStores,
    FreightTest,
    FreightSignalTelecoms,
    FreightAutomotiveComponents,
    FreightAutomotiveVehicles,
    FreightEdibleProducts,
    FreightIndustrialMinerals,
    FreightChemicals,
    FreightWagonloadBuildingMaterials,
    FreightMerchandise,
    FreightInternational,
    FreightInternationalMixed,
    FreightInternationalIntermodal,
    FreightInternationalAutomotive,
    FreightInternationalContract,
    FreightInternationalHaulmark,
    FreightInternationalJointVenture,
    FreightIntermodalContracts,
    FreightIntermodalOther,
    FreightCoalDistributive,
    FreightCoalElectricity,
    FreightNuclear,
    FreightMetals,
    FreightAggregates,
    FreightWaste,
    FreightTrainloadBuildingMaterials,
    FreightPetroleum,
    LocomotiveBrakeVan,
    Locomotive,
    OrdinaryPassenger,
    ExpressPassenger,
    InternationalPassenger,
    SleeperPassenger,
    InternationalSleeperPassenger,
    CarCarryingPassenger,
    UnadvertisedPassenger,
    UnadvertisedExpressPassenger,
    EmptyPassenger,
    Staff,
    EmptyPassengerAndStaff,
    Mixed,
    Metro,
    EmptyMetro,
    Post,
    Parcels,
    EmptyNonPassenger,
    PassengerParcels,
    Ship,
    Trip,
    Tram,
    CableTram,
    CableCar,
    Funicular,
    Trolleybus,
    Monorail,
    Coach,
    Taxi,
    Air,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub enum TrainSource {
    LongTerm,
    ShortTerm,
    VeryShortTerm,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub enum TrainPower {
    DieselLocomotive,
    DieselElectricMultipleUnit,
    DieselMechanicalMultipleUnit,
    DieselHydraulicMultipleUnit,
    ElectricLocomotive,
    ElectricAndDieselLocomotive,
    ElectricMultipleUnitWithLocomotive,
    ElectricMultipleUnit,
    ElectricAndDieselMultipleUnit,
    BatteryLocomotive,
    BatteryMultipleUnit,
    SteamLocomotive,
    SteamRailcar,
}

#[derive(Clone, Debug, Serialize)]
pub struct TrainVehicle {
    pub id: String,
    pub description: String,
    // TODO more here, types etc.?
}

#[derive(Clone, Debug, Serialize)]
pub struct TrainAllocation {
    pub id: String,
    pub description: String,
    pub vehicles: Option<Vec<TrainVehicle>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TrainOperator {
    pub id: String,
    pub description: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct OperatingCharacteristics {
    pub vacuum_braked: bool,
    pub one_hundred_mph: bool,
    pub driver_only_passenger: bool,
    pub br_mark_four_coaches: bool,
    pub guard_required: bool,
    pub one_hundred_and_ten_mph: bool,
    pub push_pull: bool,
    pub air_conditioned_with_pa: bool,
    pub steam_heat: bool,
    pub runs_to_locations_as_required: bool,
    pub sb1c_gauge: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub enum ReservationField {
    Possible,
    Mandatory,
    Recommended,
    Impossible,
    NotMandatory, // some railways might not have possible/impossible distinction
    NotApplicable,
    Unknown,
}

#[derive(Clone, Debug, Serialize)]
pub struct Reservations {
    pub seats: ReservationField,
    pub bicycles: ReservationField,
    pub sleepers: ReservationField,
    pub vehicles: ReservationField,
    pub wheelchairs: ReservationField,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Catering {
    pub buffet: bool,
    pub first_class_restaurant: bool,
    pub hot_food: bool,
    pub first_class_meal: bool,
    pub restaurant: bool,
    pub trolley: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Activities {
    pub detach: bool,
    pub attach: bool,
    pub other_trains_pass: bool,
    pub attach_or_detach_assisting_loco: bool,
    pub x_on_arrival: bool,
    pub banking_loco: bool,
    pub crew_change: bool,
    pub set_down_only: bool,
    pub examination: bool,
    pub gbprtt: bool,
    pub prevent_column_merge: bool,
    pub prevent_third_column_merge: bool,
    pub passenger_count: bool,
    pub ticket_collection: bool,
    pub ticket_examination: bool,
    pub first_class_ticket_examination: bool,
    pub selective_ticket_examination: bool,
    pub change_loco: bool,
    pub unadvertised_stop: bool,
    pub operational_stop: bool,
    pub train_locomotive_on_rear: bool,
    pub propelling: bool,
    pub request_pick_up: bool,
    pub request_set_down: bool,
    pub reversing_move: bool,
    pub run_round: bool,
    pub staff_stop: bool,
    pub normal_passenger_stop: bool,
    pub train_begins: bool,
    pub train_finishes: bool,
    pub tops_reporting: bool,
    pub token_etc: bool,
    pub pick_up_only: bool,
    pub watering_stock: bool,
    pub cross_at_passing_point: bool,
    pub request_pick_up_by_telephone: bool,
    pub request_set_down_by_telephone: bool,
    pub times_approximate: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct AssociationNode {
    pub other_train_id: String,
    pub other_train_location_id_suffix: Option<String>,
    pub validity: Vec<TrainValidityPeriod>,
    pub cancellations: Vec<(TrainValidityPeriod, TrainSource)>,
    pub replacements: Vec<AssociationNode>,
    pub day_diff: i8,
    pub for_passengers: bool,
    pub source: Option<TrainSource>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TrainLocation {
    pub timing_tz: Option<Tz>, // TZ for timings, if different from the location TZ (GTFS)
    pub id: String,
    pub id_suffix: Option<String>, // to allow associations to be matched when the same location
    // occurs multiple times in a given train
    pub working_arr: Option<NaiveTime>,
    pub working_arr_day: Option<u8>,
    pub working_dep: Option<NaiveTime>,
    pub working_dep_day: Option<u8>,
    pub working_pass: Option<NaiveTime>,
    pub working_pass_day: Option<u8>,
    pub public_arr: Option<NaiveTime>,
    pub public_arr_day: Option<u8>,
    pub public_dep: Option<NaiveTime>,
    pub public_dep_day: Option<u8>,
    pub platform: Option<String>,
    pub platform_zone: Option<String>,
    pub line: Option<String>,
    pub path: Option<String>,
    pub engineering_allowance_s: Option<u32>,
    pub pathing_allowance_s: Option<u32>,
    pub performance_allowance_s: Option<u32>,
    pub activities: Activities,
    pub change_en_route: Option<VariableTrain>,
    pub divides_to_form: Vec<AssociationNode>,
    pub joins_to: Vec<AssociationNode>,
    pub becomes: Option<AssociationNode>,
    pub divides_from: Vec<AssociationNode>,
    pub is_joined_to_by: Vec<AssociationNode>,
    pub forms_from: Option<AssociationNode>,
}

#[derive(Clone, Debug, Serialize)]
pub struct VariableTrain {
    pub train_type: TrainType,
    pub public_id: Option<String>,
    pub headcode: Option<String>,
    pub service_group: Option<String>,
    pub power_type: Option<TrainPower>,
    pub timing_allocation: Option<TrainAllocation>,
    pub actual_allocation: Option<TrainAllocation>,
    pub timing_speed_m_per_s: Option<f64>,
    pub operating_characteristics: Option<OperatingCharacteristics>,
    pub has_first_class_seats: Option<bool>,
    pub has_second_class_seats: Option<bool>,
    pub has_first_class_sleepers: Option<bool>,
    pub has_second_class_sleepers: Option<bool>,
    pub carries_vehicles: Option<bool>,
    pub reservations: Reservations,
    pub catering: Option<Catering>,
    pub brand: Option<String>,
    pub name: Option<String>,
    pub uic_code: Option<String>,
    pub operator: Option<TrainOperator>,
    pub wheelchair_accessible: Option<bool>,
    pub bicycles_allowed: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Train {
    pub id: String,
    pub validity: Vec<TrainValidityPeriod>,
    pub cancellations: Vec<(TrainValidityPeriod, TrainSource)>,
    pub replacements: Vec<Train>,
    pub variable_train: VariableTrain,
    pub source: Option<TrainSource>,
    pub runs_as_required: bool,
    pub performance_monitoring: Option<bool>,
    pub route: Vec<TrainLocation>,
}
