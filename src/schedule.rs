use chrono::{DateTime, NaiveTime, Weekday};
use chrono_tz::Tz;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Schedule {
    pub locations: HashMap<String, Location>,
    pub trains: HashMap<String, Vec<Train>>, // one ID could have multiple permanent schedules on
                                             // different dates
    pub namespace: String, // this is defined by me
    pub their_id: Option<String>,
    pub valid_begin: Option<DateTime::<Tz>>,
    pub valid_end: Option<DateTime::<Tz>>,
    pub last_updated: Option<DateTime::<Tz>>,
}

impl Schedule {
    pub fn new(namespace: String) -> Self {
        Self {
            locations: HashMap::new(),
            trains: HashMap::new(),
            namespace: namespace,
            their_id: None,
            valid_begin: None,
            valid_end: None,
            last_updated: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Location {
    pub id: String,
    pub name: String,
    pub public_id: Option<String>, // some countries have an internal ID for planning and a public
                                   // ID for retail; we should expose the public one.
}

#[derive(Clone, Debug)]
pub struct TrainValidityPeriod {
    pub valid_begin: DateTime::<Tz>,
    pub valid_end: DateTime::<Tz>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
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
}

impl IntoIterator for &DaysOfWeek {
    type Item = bool;
    type IntoIter = std::array::IntoIter<bool, 7>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIterator::into_iter([self.monday, self.tuesday, self.wednesday, self.thursday, self.friday, self.saturday, self.sunday])
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
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
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TrainSource {
    LongTerm,
    ShortTerm,
    VeryShortTerm,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TrainPower {
    DieselLocomotive,
    DieselElectricMultipleUnit,
    DieselMechanicalMultipleUnit,
    DieselHydraulicMultipleUnit,
    ElectricLocomotive,
    ElectricAndDieselLocomotive,
    ElectricMultipleUnitWithLocomotive,
    ElectricMultipleUnit,
    BatteryLocomotive,
    BatteryMultipleUnit,
    SteamLocomotive,
    SteamRailcar,
}

#[derive(Clone, Debug)]
pub struct TrainVehicle {
    pub id: String,
    pub description: String,
    // TODO more here, types etc.?
}

#[derive(Clone, Debug)]
pub struct TrainAllocation {
    pub id: String,
    pub description: String,
    pub vehicles: Option<Vec<TrainVehicle>>
}

#[derive(Clone, Debug)]
pub struct TrainOperator {
    pub id: String,
    pub description: Option<String>,
}

#[derive(Clone, Debug, Default)]
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReservationField {
    Possible,
    Mandatory,
    Recommended,
    Impossible,
    NotMandatory, // some railways might not have possible/impossible distinction
    NotApplicable,
}

#[derive(Clone, Debug)]
pub struct Reservations {
    pub seats: ReservationField,
    pub bicycles: ReservationField,
    pub sleepers: ReservationField,
    pub vehicles: ReservationField,
    pub wheelchairs: ReservationField,
}

#[derive(Clone, Debug, Default)]
pub struct Catering {
    pub buffet: bool,
    pub first_class_restaurant: bool,
    pub hot_food: bool,
    pub first_class_meal: bool,
    pub restaurant: bool,
    pub trolley: bool,
}

#[derive(Clone, Debug)]
pub struct Activities {
    pub detach: bool,
    pub attach: bool,
    pub stops_to_pass: bool,
    pub attach_or_detach_assisting_loco: bool,
    pub x_on_arrival: bool,
    pub stops_for_banking_loco: bool,
    pub stops_for_crew_change: bool,
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
    pub stops_to_change_loco: bool,
    pub unadvertised_stop: bool,
    pub operational_stop: bool,
    pub train_locomotive_on_rear: bool,
    pub propelling: bool,
    pub request_stop: bool,
    pub reversing_move: bool,
    pub run_round: bool,
    pub staff_stop: bool,
    pub normal_passenger_stop: bool,
    pub train_begins: bool,
    pub train_finishes: bool,
    pub tops_reporting: bool,
    pub stops_for_token_etc: bool,
    pub pick_up_only: bool,
    pub watering_stock: bool,
    pub stops_to_cross: bool,
}

#[derive(Clone, Debug)]
pub struct AssociationNode {
    pub other_train_id: String,
    pub other_train_location_id_suffix: Option<String>,
    pub validity: Vec<TrainValidityPeriod>,
    pub cancellations: Vec<(TrainValidityPeriod, DaysOfWeek)>,
    pub replacements: Vec<AssociationNode>,
    pub days: DaysOfWeek,
    pub day_diff: i8,
    pub for_passengers: bool,
    pub source: Option<TrainSource>,
}

#[derive(Clone, Debug)]
pub struct TrainLocation {
    pub timezone: Tz,
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
    pub line: Option<String>,
    pub path: Option<String>,
    pub engineering_allowance_s: Option<f64>,
    pub pathing_allowance_s: Option<f64>,
    pub performance_allowance_s: Option<f64>,
    pub activities: Activities,
    pub change_en_route: Option<VariableTrain>,
    pub divides_to_form: Vec<AssociationNode>,
    pub joins_to: Vec<AssociationNode>,
    pub becomes: Option<AssociationNode>,
    pub divides_from: Vec<AssociationNode>,
    pub is_joined_to_by: Vec<AssociationNode>,
    pub forms_from: Option<AssociationNode>,
}

#[derive(Clone, Debug)]
pub struct VariableTrain {
    pub train_type: TrainType,
    pub public_id: Option<String>,
    pub headcode: Option<String>,
    pub service_group: Option<String>,
    pub power_type: Option<TrainPower>,
    pub timing_allocation: Option<TrainAllocation>,
    pub actual_allocation: Option<TrainAllocation>,
    pub timing_speed_m_per_s: Option<f64>,
    pub operating_characteristics: OperatingCharacteristics,
    pub has_first_class_seats: bool,
    pub has_second_class_seats: bool,
    pub has_first_class_sleepers: bool,
    pub has_second_class_sleepers: bool,
    pub carries_vehicles: bool,
    pub reservations: Reservations,
    pub catering: Catering,
    pub brand: Option<String>,
    pub name: Option<String>,
    pub uic_code: Option<String>,
    pub operator: Option<TrainOperator>,
}

#[derive(Clone, Debug)]
pub struct Train {
    pub id: String,
    pub validity: Vec<TrainValidityPeriod>,
    pub cancellations: Vec<(TrainValidityPeriod, DaysOfWeek)>,
    pub replacements: Vec<Train>,
    pub days_of_week: DaysOfWeek,
    pub variable_train: VariableTrain,
    pub source: Option<TrainSource>,
    pub runs_as_required: bool,
    pub performance_monitoring: Option<bool>,
    pub route: Vec<TrainLocation>,
}
