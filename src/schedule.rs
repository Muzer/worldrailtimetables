use chrono::{DateTime, NaiveTime, Weekday};
use chrono_tz::Tz;

use rgb::RGB8;
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
    Passenger,
    OrdinaryPassenger,
    ExpressPassenger,
    IntercityPassenger,
    UrbanPassenger,
    InternationalPassenger,
    LocalPassenger,
    HighSpeedPassenger,
    SuburbanPassenger,
    RegionalPassenger,
    InterregionalPassenger,
    LongDistancePassenger,
    SleeperPassenger,
    NightPassenger,
    InternationalSleeperPassenger,
    CarCarryingPassenger,
    LorryCarryingPassenger,
    TouristPassenger,
    AirportLinkPassenger,
    ShuttlePassenger,
    ReplacementPassenger,
    SpecialPassenger,
    ReliefPassenger,
    CrossCountryPassenger,
    RackAndPinionPassenger,
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
    UndefinedCoach,
    InternationalCoach,
    NationalCoach,
    ShuttleCoach,
    RegionalCoach,
    SpecialCoach,
    SchoolCoach,
    SightseeingCoach,
    TouristCoach,
    CommuterCoach,
    Taxi,
    Air,
    Unknown,
    Water,
    SnowAndIce,
    Lift,
    SelfDrive,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub enum TrainSource {
    LongTerm,
    ShortTerm,
    VeryShortTerm,
    Provisional,
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
    pub public_id: Option<String>,
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
    MandatoryFromOrigin,
    Recommended,
    Impossible,
    NotMandatory, // some railways might not have possible/impossible distinction
    NotApplicable,
    Restricted,
    NotAllowed, // for when a specific type of booking is not allowed at all, eg groups forbidden
    Unknown,
}

#[derive(Clone, Debug, Serialize)]
pub struct Reservations {
    pub seats: ReservationField,
    pub groups: ReservationField,
    pub first_class: ReservationField,
    pub second_class: ReservationField,
    pub not_every_class: ReservationField,
    pub bicycles: ReservationField,
    pub sleepers: ReservationField,
    pub vehicles: ReservationField,
    pub wheelchairs: ReservationField,
    pub supplement_charged: Option<bool>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Catering {
    pub at_seat_meal: bool,
    pub bar: bool,
    pub bistro: bool,
    pub breakfast_in_car: bool,
    pub buffet: bool,
    pub coffee_shop: bool,
    pub self_service: bool,
    pub trolley: bool,
    pub vending_machine_food: bool,
    pub vending_machine_drink: bool,
    pub mini_bar: bool,
    pub restaurant: bool,
    pub first_class_restaurant: bool,
    pub first_class_meal: bool,
    pub other: bool,
    pub food_available: Option<bool>,
    pub hot_food_available: Option<bool>,
    pub drink_available: Option<bool>,
    pub snacks_available: Option<bool>,
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
pub struct AccommodationTypes {
    pub standing: Option<bool>,
    pub seating: Option<bool>,
    pub reclining_seating: Option<bool>,
    pub special_seating: Option<bool>,
    pub sleeper: Option<bool>,
    pub single_sleeper: Option<bool>,
    pub double_sleeper: Option<bool>,
    pub special_sleeper: Option<bool>,
    pub couchette: Option<bool>,
    pub single_couchette: Option<bool>,
    pub double_couchette: Option<bool>,
    pub baby: Option<bool>,
    pub family: Option<bool>,
    pub recreation: Option<bool>,
    pub panoramic: Option<bool>,
    pub pullman: Option<bool>,
    pub pushchair: Option<bool>,
    pub wheelchair: Option<bool>,
    pub has_male_only: Option<bool>,
    pub has_female_only: Option<bool>,
    pub has_same_sex_only: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AccommodationTypesByClass {
    pub unknown: Option<AccommodationTypes>,
    pub first_premium: Option<AccommodationTypes>,
    pub first: Option<AccommodationTypes>,
    pub second_premium: Option<AccommodationTypes>,
    pub second: Option<AccommodationTypes>,
    pub third: Option<AccommodationTypes>,
    pub unclassified: Option<AccommodationTypes>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Line {
    pub id: String,
    pub public_id: Option<String>,
    pub name: Option<String>,
    pub number: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub background_colour: Option<RGB8>,
    pub foreground_colour: Option<RGB8>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Toilets {
    pub toilet: Option<bool>,
    pub sink: Option<bool>,
    pub disabled_toilet: Option<bool>,
    pub shower: Option<bool>,
    pub changing: Option<bool>,
    pub baby_changing: Option<bool>,
    pub disabled_baby_changing: Option<bool>,
    pub shoe_shiner: Option<bool>,
    pub other: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Luggage {
    pub bag_storage: Option<bool>,
    pub racks: Option<bool>,
    pub skis: Option<bool>,
    pub skis_on_rear: Option<bool>,
    pub extra_large_racks: Option<bool>,
    pub van: Option<bool>,
    pub bicycles: Option<bool>,
    pub bicycles_in_van: Option<bool>,
    pub bicycles_in_carriage: Option<bool>,
    pub pushchairs: Option<bool>,
    pub vehicles: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Families {
    pub children_facilities: Option<bool>,
    pub military_family_facilities: Option<bool>,
    pub nursery: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PassengerCommunications {
    pub free_wifi: Option<bool>,
    pub wifi: Option<bool>,
    pub mains_sockets: Option<bool>,
    pub telephone: Option<bool>,
    pub radio: Option<bool>,
    pub video: Option<bool>,
    pub business: Option<bool>,
    pub internet: Option<bool>,
    pub post_office: Option<bool>,
    pub postbox: Option<bool>,
    pub usb_a: Option<bool>,
    pub usb_c: Option<bool>,
    pub other: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Assistance {
    pub personal: Option<bool>,
    pub boarding: Option<bool>,
    pub wheelchair: Option<bool>,
    pub unaccompanied_minor: Option<bool>,
    pub use_of_wheelchair: Option<bool>,
    pub guard: Option<bool>,
    pub information: Option<bool>,
    pub other: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PassengerInformation {
    pub next_stop_indication: Option<bool>,
    pub stop_announcements: Option<bool>,
    pub information_display: Option<bool>,
    pub realtime_connections: Option<bool>,
    pub audible_information: Option<bool>,
    pub hearing_impaired_audible_information: Option<bool>,
    pub visible_information: Option<bool>,
    pub visually_impaired_visible_information: Option<bool>,
    pub large_print_timetable: Option<bool>,
    pub other: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct VariableTrain {
    pub train_type: TrainType,
    pub public_id: Option<String>,
    pub headcode: Option<String>,
    pub power_type: Option<TrainPower>,
    pub timing_allocation: Option<TrainAllocation>,
    pub actual_allocation: Option<TrainAllocation>,
    pub timing_speed_m_per_s: Option<f64>,
    pub operating_characteristics: Option<OperatingCharacteristics>,
    pub accommodation: Option<AccommodationTypesByClass>,
    pub reservations: Reservations,
    pub catering: Option<Catering>,
    pub brand: Option<String>,
    pub name: Option<String>,
    pub line: Option<Line>,
    pub uic_code: Option<String>,
    pub operator: Option<TrainOperator>,
    pub wheelchair_accessible: Option<bool>,
    pub toilets: Option<Toilets>,
    pub luggage: Option<Luggage>,
    pub families: Option<Families>,
    pub passenger_communications: Option<PassengerCommunications>,
    pub assistance: Option<Assistance>,
    pub passenger_information: Option<PassengerInformation>,
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
