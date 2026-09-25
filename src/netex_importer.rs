use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::ParseIntError;

use async_trait::async_trait;

use chrono::{Datelike, NaiveDateTime, NaiveTime};
use chrono::naive::Days;
use chrono_tz::CET;

use quick_xml::de;

use sea_orm::DatabaseTransaction;
use sea_orm::entity::{ActiveHasMany, ActiveHasOne, ActiveValue};

use serde::{Deserialize, Serialize};

use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use crate::error::Error;
use crate::importer::SlowStreamingImporter;
use crate::schedule::{
    AccommodationClass, accommodation_types, association_node, AssociationType, line, location,
    schedule, ReservationField, train, train_cancellation, train_location, train_operator,
    TrainSource, TrainType, train_validity_period, train_variant, variable_train
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationDelivery {
    #[serde(rename = "@xmlns:gml")]
    pub xmlns_gml: String,
    #[serde(rename = "@xmlns:xsi")]
    pub xmlns_xsi: String,
    #[serde(rename = "@xmlns:siri")]
    pub xmlns_siri: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "@xmlns")]
    pub xmlns: String,
    #[serde(rename = "@schemaLocation")]
    pub xsi_schema_location: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "PublicationTimestamp")]
    pub publication_timestamp: NaiveDateTime,
    #[serde(rename = "ParticipantRef")]
    pub participant_ref: String,
    #[serde(rename = "Description")]
    pub description: PublicationDeliveryDescription,
    #[serde(rename = "dataObjects")]
    pub data_objects: DataObjects,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationDeliveryDescription {
    #[serde(rename = "@lang")]
    pub lang: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataObjects {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "CompositeFrame")]
    pub composite_frame: CompositeFrame,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositeFrame {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ValidBetween")]
    pub valid_between: CompositeFrameValidBetween,
    #[serde(rename = "FrameDefaults")]
    pub frame_defaults: FrameDefaults,
    pub frames: Frames,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositeFrameValidBetween {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "FromDate")]
    pub from_date: NaiveDateTime,
    #[serde(rename = "ToDate")]
    pub to_date: NaiveDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameDefaults {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "DefaultLocale")]
    pub default_locale: Locale,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Frames {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "GeneralFrame")]
    pub general_frame: GeneralFrame,
    #[serde(rename = "ResourceFrame")]
    pub resource_frame: ResourceFrame,
    #[serde(rename = "SiteFrame")]
    pub site_frame: SiteFrame,
    #[serde(rename = "ServiceFrame")]
    pub service_frame: ServiceFrame,
    #[serde(rename = "ServiceCalendarFrame")]
    pub service_calendar_frame: ServiceCalendarFrame,
    #[serde(rename = "TimetableFrame")]
    pub timetable_frame: TimetableFrame,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneralFrame {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Name")]
    pub name: String,
    pub members: GeneralFrameMembers,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneralFrameMembers {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "JourneyPart")]
    pub journey_part: Vec<JourneyPart>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPart {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ParentJourneyRef")]
    pub parent_journey_ref: ParentJourneyRef,
    #[serde(rename = "MainPartRef")]
    pub main_part_ref: JourneyPartMainPartRef,
    #[serde(rename = "JourneyPartCoupleRef")]
    pub journey_part_couple_ref: Option<JourneyPartCoupleRef>,
    #[serde(rename = "TrainNumberRef")]
    pub train_number_ref: JourneyPartTrainNumberRef,
    #[serde(rename = "FromStopPointRef")]
    pub from_stop_point_ref: JourneyPartFromStopPointRef,
    #[serde(rename = "ToStopPointRef")]
    pub to_stop_point_ref: JourneyPartToStopPointRef,
    #[serde(rename = "StartTime")]
    pub start_time: NaiveTime,
    #[serde(rename = "EndTime")]
    pub end_time: NaiveTime,
    #[serde(rename = "EndTimeDayOffset")]
    pub end_time_day_offset: Option<u8>,
    #[serde(rename = "StartTimeDayOffset")]
    pub start_time_day_offset: Option<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentJourneyRef {
    #[serde(rename = "@ref")]
    pub parent_journey_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartMainPartRef {
    #[serde(rename = "@ref")]
    pub main_part_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartCoupleRef {
    #[serde(rename = "@ref")]
    pub journey_part_couple_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartTrainNumberRef {
    #[serde(rename = "@ref")]
    pub train_number_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartFromStopPointRef {
    #[serde(rename = "@ref")]
    pub from_stop_point_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartToStopPointRef {
    #[serde(rename = "@ref")]
    pub to_stop_point_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceFrame {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "typesOfValue")]
    pub types_of_value: TypesOfValue,
    pub organisations: Organisations,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypesOfValue {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ValueSet")]
    pub value_set: Vec<ValueSet>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueSet {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "@nameOfClass")]
    pub name_of_class: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Name")]
    pub name: String,
    pub values: Values,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Values {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "TypeOfProductCategory")]
    pub type_of_product_category: Option<Vec<TypeOfProductCategory>>,
    #[serde(rename = "TypeOfPlace")]
    pub type_of_place: Option<Vec<TypeOfPlace>>,
    #[serde(rename = "TypeOfLine")]
    pub type_of_line: Option<Vec<TypeOfLine>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeOfProductCategory {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Name")]
    pub name: TypeOfProductCategoryName,
    #[serde(rename = "ShortName")]
    pub short_name: TypeOfProductCategoryShortName,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeOfProductCategoryName {
    #[serde(rename = "@lang")]
    pub lang: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeOfProductCategoryShortName {
    #[serde(rename = "@lang")]
    pub lang: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeOfPlace {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Name")]
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeOfLine {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Name")]
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Organisations {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Operator")]
    pub operator: Vec<Operator>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operator {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "PublicCode")]
    pub public_code: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "OrganisationType")]
    pub organisation_type: OrganisationType,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OrganisationType {
    #[serde(rename = "authority")]
    Authority,
    #[serde(rename = "operator")]
    Operator,
    #[serde(rename = "railOperator")]
    RailOperator,
    #[serde(rename = "railFreightOperator")]
    RailFreightOperator,
    #[serde(rename = "statutoryBody")]
    StatutoryBody,
    #[serde(rename = "facilityOperator")]
    FacilityOperator,
    #[serde(rename = "travelAgent")]
    TravelAgent,
    #[serde(rename = "servicedOrganisation")]
    ServicedOrganisation,
    #[serde(rename = "retailConsortium")]
    RetailConsortium,
    #[serde(rename = "alternativeModeOperator")]
    AlternativeModeOperator,
    #[serde(rename = "onlineProvider")]
    OnlineProvider,
    #[serde(rename = "other")]
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SiteFrame {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "topographicPlaces")]
    pub topographic_places: TopographicPlaces,
    #[serde(rename = "stopPlaces")]
    pub stop_places: StopPlaces,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopographicPlaces {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "TopographicPlace")]
    pub topographic_place: Vec<TopographicPlace>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopographicPlace {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "PrivateCode")]
    pub private_code: String,
    #[serde(rename = "Descriptor")]
    pub descriptor: Descriptor,
    #[serde(rename = "TopographicPlaceType")]
    pub topographic_place_type: TopographicPlaceType,
    #[serde(rename = "CountryRef")]
    pub country_ref: String,
    #[serde(rename = "ParentTopographicPlaceRef")]
    pub parent_topographic_place_ref: Option<ParentTopographicPlaceRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TopographicPlaceType {
    #[serde(rename = "continent")]
    Continent,
    #[serde(rename = "interregion")]
    Interregion,
    #[serde(rename = "country")]
    Country,
    #[serde(rename = "principality")]
    Principality,
    #[serde(rename = "state")]
    State,
    #[serde(rename = "province")]
    Province,
    #[serde(rename = "region")]
    Region,
    #[serde(rename = "county")]
    County,
    #[serde(rename = "area")]
    Area,
    #[serde(rename = "conurbation")]
    Conurbation,
    #[serde(rename = "city")]
    City,
    #[serde(rename = "municipality")]
    Municipality,
    #[serde(rename = "quarter")]
    Quarter,
    #[serde(rename = "suburb")]
    Suburb,
    #[serde(rename = "town")]
    Town,
    #[serde(rename = "urbanCentre")]
    UrbanCentre,
    #[serde(rename = "district")]
    District,
    #[serde(rename = "parish")]
    Parish,
    #[serde(rename = "village")]
    Village,
    #[serde(rename = "hamlet")]
    Hamlet,
    #[serde(rename = "placeOfInterest")]
    PlaceOfInterest,
    #[serde(rename = "other")]
    Other,
    #[serde(rename = "unrecorded")]
    Unrecorded,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Name")]
    pub name: DescriptorName,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DescriptorName {
    #[serde(rename = "@lang")]
    pub lang: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentTopographicPlaceRef {
    #[serde(rename = "@ref")]
    pub parent_topographic_place_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopPlaces {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "StopPlace")]
    pub stop_place: Vec<StopPlace>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopPlace {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@created")]
    pub created: String,
    #[serde(rename = "@changed")]
    pub changed: String,
    #[serde(rename = "@modification")]
    pub modification: Modification,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ValidBetween")]
    pub valid_between: StopPlaceValidBetween,
    #[serde(rename = "Name")]
    pub name: StopPlaceName,
    #[serde(rename = "ShortName")]
    pub short_name: StopPlaceShortName,
    #[serde(rename = "PrivateCode")]
    pub private_code: StopPlacePrivateCode,
    #[serde(rename = "Centroid")]
    pub centroid: Centroid,
    #[serde(rename = "placeTypes")]
    pub place_types: PlaceTypes,
    #[serde(rename = "PostalAddress")]
    pub postal_address: Option<PostalAddress>,
    #[serde(rename = "Locale")]
    pub locale: Locale,
    #[serde(rename = "TransportMode")]
    pub transport_mode: AllPublicTransportModes,
    #[serde(rename = "StopPlaceType")]
    pub stop_place_type: StopType,
    #[serde(rename = "OtherTransportModes")]
    pub other_transport_modes: Option<AllPublicTransportModes>,
    #[serde(rename = "TopographicPlaceRef")]
    pub topographic_place_ref: Option<TopographicPlaceRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Modification {
    #[serde(rename = "new")]
    New,
    #[serde(rename = "revise")]
    Revise,
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "unchanged")]
    Unchanged,
    #[serde(rename = "delta")]
    Delta,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AllPublicTransportModes {
    #[serde(rename = "all")]
    All,
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "bus")]
    Bus,
    #[serde(rename = "trolleyBus")]
    TrolleyBus,
    #[serde(rename = "tram")]
    Tram,
    #[serde(rename = "coach")]
    Coach,
    #[serde(rename = "rail")]
    Rail,
    #[serde(rename = "intercityRail")]
    IntercityRail,
    #[serde(rename = "urbanRail")]
    UrbanRail,
    #[serde(rename = "metro")]
    Metro,
    #[serde(rename = "air")]
    Air,
    #[serde(rename = "water")]
    Water,
    #[serde(rename = "cableway")]
    Cableway,
    #[serde(rename = "funicular")]
    Funicular,
    #[serde(rename = "snowAndIce")]
    SnowAndIce,
    #[serde(rename = "taxi")]
    Taxi,
    #[serde(rename = "ferry")]
    Ferry,
    #[serde(rename = "lift")]
    Lift,
    #[serde(rename = "selfDrive")]
    SelfDrive,
    #[serde(rename = "anyMode")]
    AnyMode,
    #[serde(rename = "other")]
    Other,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StopType {
    #[serde(rename = "onstreetBus")]
    OnstreetBus,
    #[serde(rename = "onstreetTram")]
    OnstreetTram,
    #[serde(rename = "airport")]
    Airport,
    #[serde(rename = "railStation")]
    RailStation,
    #[serde(rename = "metroStation")]
    MetroStation,
    #[serde(rename = "busStation")]
    BusStation,
    #[serde(rename = "coachStation")]
    CoachStation,
    #[serde(rename = "tramStation")]
    TramStation,
    #[serde(rename = "harbourPort")]
    HarbourPort,
    #[serde(rename = "ferryPort")]
    FerryPort,
    #[serde(rename = "ferryStop")]
    FerryStop,
    #[serde(rename = "liftStation")]
    LiftStation,
    #[serde(rename = "vehicleRailInterchange")]
    VehicleRailInterchange,
    #[serde(rename = "taxiRank")]
    TaxiRank,
    #[serde(rename = "other")]
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopPlaceValidBetween {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "FromDate")]
    pub from_date: NaiveDateTime,
    #[serde(rename = "ToDate")]
    pub to_date: NaiveDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopPlaceName {
    #[serde(rename = "@lang")]
    pub lang: String,
    #[serde(rename = "$text")]
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopPlaceShortName {
    #[serde(rename = "@lang")]
    pub lang: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopPlacePrivateCode {
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Centroid {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Location")]
    pub location: CentroidLocation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CentroidLocation {
    #[serde(rename = "@srsName")]
    pub srs_name: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Longitude")]
    pub longitude: f64,
    #[serde(rename = "Latitude")]
    pub latitude: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaceTypes {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "TypeOfPlaceRef")]
    pub type_of_place_ref: TypeOfPlaceRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeOfPlaceRef {
    #[serde(rename = "@ref")]
    pub type_of_place_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostalAddress {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "CountryRef")]
    pub country_ref: String,
    #[serde(rename = "HouseNumber")]
    pub house_number: String,
    #[serde(rename = "Street")]
    pub street: String,
    #[serde(rename = "Town")]
    pub town: String,
    #[serde(rename = "PostCode")]
    pub post_code: String,
    #[serde(rename = "PostalRegion")]
    pub postal_region: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Locale {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "TimeZoneOffset")]
    pub time_zone_offset: String,
    #[serde(rename = "TimeZone")]
    pub time_zone: Option<String>,
    #[serde(rename = "SummerTimeZoneOffset")]
    pub summer_time_zone_offset: String,
    #[serde(rename = "DefaultLanguage")]
    pub default_language: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopographicPlaceRef {
    #[serde(rename = "@ref")]
    pub topographic_place_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceFrame {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "additionalNetworks")]
    pub additional_networks: AdditionalNetworks,
    #[serde(rename = "routePoints")]
    pub route_points: RoutePoints,
    #[serde(rename = "routeLinks")]
    pub route_links: RouteLinks,
    pub routes: ServiceFrameRoutes,
    pub lines: Lines,
    #[serde(rename = "destinationDisplays")]
    pub destination_displays: DestinationDisplays,
    #[serde(rename = "scheduledStopPoints")]
    pub scheduled_stop_points: ScheduledStopPoints,
    pub connections: Connections,
    #[serde(rename = "stopAssignments")]
    pub stop_assignments: StopAssignments,
    #[serde(rename = "journeyPatterns")]
    pub journey_patterns: JourneyPatterns,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdditionalNetworks {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Network")]
    pub network: Vec<Network>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Network {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "groupsOfLines")]
    pub groups_of_lines: GroupsOfLines,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupsOfLines {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "GroupOfLines")]
    pub group_of_lines: Vec<GroupOfLines>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupOfLines {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Name")]
    pub name: String,
    pub members: GroupOfLinesMembers,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupOfLinesMembers {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "LineRef")]
    pub line_ref: Vec<MembersLineRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembersLineRef {
    #[serde(rename = "@ref")]
    pub line_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutePoints {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "RoutePoint")]
    pub route_point: Vec<RoutePoint>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutePoint {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Location")]
    pub location: RoutePointLocation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutePointLocation {
    #[serde(rename = "@srsName")]
    pub srs_name: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Longitude")]
    pub longitude: f64,
    #[serde(rename = "Latitude")]
    pub latitude: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteLinks {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "RouteLink")]
    pub route_link: Vec<RouteLink>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteLink {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Distance")]
    pub distance: u32,
    #[serde(rename = "FromPointRef")]
    pub from_point_ref: FromPointRef,
    #[serde(rename = "ToPointRef")]
    pub to_point_ref: ToPointRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FromPointRef {
    #[serde(rename = "@ref")]
    pub from_point_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToPointRef {
    #[serde(rename = "@ref")]
    pub to_point_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceFrameRoutes {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Route")]
    pub route: Vec<Route>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Distance")]
    pub distance: u32,
    #[serde(rename = "LineRef")]
    pub line_ref: RouteLineRef,
    #[serde(rename = "DirectionType")]
    pub direction_type: Option<DirectionType>,
    #[serde(rename = "pointsInSequence")]
    pub points_in_sequence: Option<RoutePointsInSequence>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DirectionType {
    #[serde(rename = "inbound")]
    Inbound,
    #[serde(rename = "outbound")]
    Outbound,
    #[serde(rename = "clockwise")]
    Clockwise,
    #[serde(rename = "anticlockwise")]
    Anticlockwise,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteLineRef {
    #[serde(rename = "@ref")]
    pub line_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutePointsInSequence {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "PointOnRoute")]
    pub point_on_route: Vec<PointOnRoute>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointOnRoute {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "@order")]
    pub order: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "RoutePointRef")]
    pub route_point_ref: RoutePointRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutePointRef {
    #[serde(rename = "@ref")]
    pub route_point_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lines {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Line")]
    pub line: Vec<Line>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Line {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "@responsibilitySetRef")]
    pub responsibility_set_ref: Option<String>,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "BrandingRef")]
    pub branding_ref: Option<String>,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Description")]
    pub description: String,
    #[serde(rename = "TransportMode")]
    pub transport_mode: AllPublicTransportModes,
    #[serde(rename = "TransportSubmode")]
    pub transport_submode: Option<LineTransportSubmode>,
    #[serde(rename = "PublicCode")]
    pub public_code: String,
    #[serde(rename = "OperatorRef")]
    pub operator_ref: Option<LineOperatorRef>,
    pub routes: LineRoutes,
    #[serde(rename = "Presentation")]
    pub presentation: Option<Presentation>,
    #[serde(rename = "TypeOfLineRef")]
    pub type_of_line_ref: Option<TypeOfLineRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineTransportSubmode {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "CoachSubmode")]
    pub coach_submode: Option<CoachSubmode>,
    #[serde(rename = "RailSubmode")]
    pub rail_submode: Option<RailSubmode>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CoachSubmode {
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "undefined")]
    Undefined,
    #[serde(rename = "internationalCoach")]
    InternationalCoach,
    #[serde(rename = "nationalCoach")]
    NationalCoach,
    #[serde(rename = "shuttleCoach")]
    ShuttleCoach,
    #[serde(rename = "regionalCoach")]
    RegionalCoach,
    #[serde(rename = "specialCoach")]
    SpecialCoach,
    #[serde(rename = "schoolCoach")]
    SchoolCoach,
    #[serde(rename = "sightseeingCoach")]
    SightseeingCoach,
    #[serde(rename = "touristCoach")]
    TouristCoach,
    #[serde(rename = "commuterCoach")]
    CommuterCoach,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RailSubmode {
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "highSpeedRail")]
    HighSpeedRail,
    #[serde(rename = "suburbanRailway")]
    SuburbanRailway,
    #[serde(rename = "regionalRail")]
    RegionalRail,
    #[serde(rename = "interregionalRail")]
    InterregionalRail,
    #[serde(rename = "longDistance")]
    LongDistance,
    #[serde(rename = "international")]
    International,
    #[serde(rename = "sleeperRailService")]
    SleeperRailService,
    #[serde(rename = "nightRail")]
    NightRail,
    #[serde(rename = "carTransportRailService")]
    CarTransportRailService,
    #[serde(rename = "largeVehicleTransportRailService")]
    LargeVehicleTransportRailService,
    #[serde(rename = "touristRailway")]
    TouristRailway,
    #[serde(rename = "airportRailLink")]
    AirportLinkRail,
    #[serde(rename = "railShuttle")]
    RailShuttle,
    #[serde(rename = "replacementRailService")]
    ReplacementRailService,
    #[serde(rename = "specialTrain")]
    SpecialTrain,
    #[serde(rename = "crossCountryRail")]
    CrossCountryRail,
    #[serde(rename = "rackAndPinionRailway")]
    RackAndPinionRailway,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineOperatorRef {
    #[serde(rename = "@ref")]
    pub operator_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineRoutes {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "RouteRef")]
    pub route_ref: Vec<RoutesRouteRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutesRouteRef {
    #[serde(rename = "@ref")]
    pub route_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Presentation {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Colour")]
    pub colour: Option<String>,
    #[serde(rename = "TextColour")]
    pub text_colour: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeOfLineRef {
    #[serde(rename = "@ref")]
    pub type_of_line_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationDisplays {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "DestinationDisplay")]
    pub destination_display: Vec<DestinationDisplay>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationDisplay {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "SideText")]
    pub side_text: String,
    #[serde(rename = "FrontText")]
    pub front_text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledStopPoints {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ScheduledStopPoint")]
    pub scheduled_stop_point: Vec<ScheduledStopPoint>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledStopPoint {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ValidBetween")]
    pub valid_between: ScheduledStopPointValidBetween,
    #[serde(rename = "Name")]
    pub name: ScheduledStopPointName,
    #[serde(rename = "Location")]
    pub location: ScheduledStopPointLocation,
    #[serde(rename = "PublicCode")]
    pub public_code: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledStopPointValidBetween {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "FromDate")]
    pub from_date: NaiveDateTime,
    #[serde(rename = "ToDate")]
    pub to_date: NaiveDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledStopPointName {
    #[serde(rename = "@lang")]
    pub lang: String,
    #[serde(rename = "$text")]
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledStopPointLocation {
    #[serde(rename = "@srsName")]
    pub srs_name: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Longitude")]
    pub longitude: f64,
    #[serde(rename = "Latitude")]
    pub latitude: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connections {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Connection")]
    pub connection: Vec<Connection>,
    #[serde(rename = "DefaultConnection")]
    pub default_connection: Vec<DefaultConnection>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connection {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "WalkTransferDuration")]
    pub walk_transfer_duration: ConnectionWalkTransferDuration,
    #[serde(rename = "BothWays")]
    pub both_ways: bool,
    #[serde(rename = "From")]
    pub from: ConnectionFrom,
    #[serde(rename = "To")]
    pub to: ConnectionTo,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionWalkTransferDuration {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "DefaultDuration")]
    pub default_duration: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionFrom {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ScheduledStopPointRef")]
    pub scheduled_stop_point_ref: FromScheduledStopPointRef,
    #[serde(rename = "TransportMode")]
    pub transport_mode: Option<AllPublicTransportModes>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FromScheduledStopPointRef {
    #[serde(rename = "@ref")]
    pub scheduled_stop_point_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionTo {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ScheduledStopPointRef")]
    pub scheduled_stop_point_ref: ToScheduledStopPointRef,
    #[serde(rename = "TransportMode")]
    pub transport_mode: Option<AllPublicTransportModes>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToScheduledStopPointRef {
    #[serde(rename = "@ref")]
    pub scheduled_stop_point_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultConnection {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "WalkTransferDuration")]
    pub walk_transfer_duration: DefaultConnectionWalkTransferDuration,
    #[serde(rename = "BothWays")]
    pub both_ways: bool,
    #[serde(rename = "From")]
    pub from: DefaultConnectionFrom,
    #[serde(rename = "To")]
    pub to: DefaultConnectionTo,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultConnectionWalkTransferDuration {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "DefaultDuration")]
    pub default_duration: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultConnectionFrom {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "TransportMode")]
    pub transport_mode: AllPublicTransportModes,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultConnectionTo {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "TransportMode")]
    pub transport_mode: AllPublicTransportModes,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopAssignments {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "PassengerStopAssignment")]
    pub passenger_stop_assignment: Vec<PassengerStopAssignment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PassengerStopAssignment {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "@order")]
    pub order: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ValidBetween")]
    pub valid_between: PassengerStopAssignmentValidBetween,
    #[serde(rename = "ScheduledStopPointRef")]
    pub scheduled_stop_point_ref: PassengerStopAssignmentScheduledStopPointRef,
    #[serde(rename = "StopPlaceRef")]
    pub stop_place_ref: StopPlaceRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PassengerStopAssignmentValidBetween {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "FromDate")]
    pub from_date: NaiveDateTime,
    #[serde(rename = "ToDate")]
    pub to_date: NaiveDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PassengerStopAssignmentScheduledStopPointRef {
    #[serde(rename = "@ref")]
    pub scheduled_stop_point_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopPlaceRef {
    #[serde(rename = "@ref")]
    pub stop_place_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPatterns {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ServiceJourneyPattern")]
    pub service_journey_pattern: Vec<ServiceJourneyPattern>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceJourneyPattern {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "@responsibilitySetRef")]
    pub responsibility_set_ref: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Distance")]
    pub distance: u32,
    #[serde(rename = "RouteRef")]
    pub route_ref: ServiceJourneyPatternRouteRef,
    #[serde(rename = "DestinationDisplayRef")]
    pub destination_display_ref: DestinationDisplayRef,
    #[serde(rename = "pointsInSequence")]
    pub points_in_sequence: ServiceJourneyPatternPointsInSequence,
    #[serde(rename = "ServiceJourneyPatternType")]
    pub service_journey_pattern_type: ServiceJourneyPatternType,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ServiceJourneyPatternType {
    #[serde(rename = "passenger")]
    Passenger,
    #[serde(rename = "garageRunOut")]
    GarageRunOut,
    #[serde(rename = "garageRunIn")]
    GarageRunIn,
    #[serde(rename = "turningManoeuvre")]
    TurningManoeuvre,
    #[serde(rename = "other")]
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceJourneyPatternRouteRef {
    #[serde(rename = "@ref")]
    pub route_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationDisplayRef {
    #[serde(rename = "@ref")]
    pub destination_display_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceJourneyPatternPointsInSequence {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "StopPointInJourneyPattern")]
    pub stop_point_in_journey_pattern: Vec<StopPointInJourneyPattern>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopPointInJourneyPattern {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "@order")]
    pub order: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ScheduledStopPointRef")]
    pub scheduled_stop_point_ref: StopPointInJourneyPatternScheduledStopPointRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopPointInJourneyPatternScheduledStopPointRef {
    #[serde(rename = "@ref")]
    pub scheduled_stop_point_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceCalendarFrame {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "dayTypes")]
    pub day_types: ServiceCalendarFrameDayTypes,
    #[serde(rename = "operatingPeriods")]
    pub operating_periods: OperatingPeriods,
    #[serde(rename = "dayTypeAssignments")]
    pub day_type_assignments: DayTypeAssignments,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceCalendarFrameDayTypes {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "DayType")]
    pub day_type: Vec<DayType>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DayType {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatingPeriods {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "UicOperatingPeriod")]
    pub uic_operating_period: Vec<UicOperatingPeriod>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UicOperatingPeriod {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "FromDate")]
    pub from_date: NaiveDateTime,
    #[serde(rename = "ToDate")]
    pub to_date: NaiveDateTime,
    #[serde(rename = "ValidDayBits")]
    pub valid_day_bits: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DayTypeAssignments {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "DayTypeAssignment")]
    pub day_type_assignment: Vec<DayTypeAssignment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DayTypeAssignment {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "@order")]
    pub order: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "OperatingPeriodRef")]
    pub operating_period_ref: OperatingPeriodRef,
    #[serde(rename = "DayTypeRef")]
    pub day_type_ref: DayTypeAssignmentDayTypeRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatingPeriodRef {
    #[serde(rename = "@ref")]
    pub operating_period_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DayTypeAssignmentDayTypeRef {
    #[serde(rename = "@ref")]
    pub day_type_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimetableFrame {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "vehicleJourneys")]
    pub vehicle_journeys: VehicleJourneys,
    #[serde(rename = "trainNumbers")]
    pub train_numbers: TimetableFrameTrainNumbers,
    #[serde(rename = "journeyPartCouples")]
    pub journey_part_couples: JourneyPartCouples,
    #[serde(rename = "coupledJourneys")]
    pub coupled_journeys: CoupledJourneys,
    #[serde(rename = "typesOfService")]
    pub types_of_service: TypesOfService,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VehicleJourneys {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ServiceJourney")]
    pub service_journey: Vec<ServiceJourney>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceJourney {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@responsibilitySetRef")]
    pub responsibility_set_ref: String,
    #[serde(rename = "@dataSourceRef")]
    pub data_source_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "@status")]
    pub status: String,
    #[serde(rename = "@derivedFromObjectRef")]
    pub derived_from_object_ref: Option<String>,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ValidBetween")]
    pub valid_between: ServiceJourneyValidBetween,
    #[serde(rename = "BrandingRef")]
    pub branding_ref: ServiceJourneyBrandingRef,
    #[serde(rename = "Distance")]
    pub distance: u32,
    #[serde(rename = "PrivateCode")]
    pub private_code: Option<String>,
    #[serde(rename = "TransportMode")]
    pub transport_mode: AllPublicTransportModes,
    #[serde(rename = "TransportSubmode")]
    pub transport_submode: ServiceJourneyTransportSubmode,
    #[serde(rename = "ServiceAlteration")]
    pub service_alteration: ServiceAlteration,
    #[serde(rename = "DepartureTime")]
    pub departure_time: NaiveTime,
    #[serde(rename = "dayTypes")]
    pub day_types: ServiceJourneyDayTypes,
    #[serde(rename = "JourneyPatternRef")]
    pub journey_pattern_ref: JourneyPatternRef,
    #[serde(rename = "OperatorRef")]
    pub operator_ref: ServiceJourneyOperatorRef,
    #[serde(rename = "LineRef")]
    pub line_ref: ServiceJourneyLineRef,
    #[serde(rename = "trainNumbers")]
    pub train_numbers: ServiceJourneyTrainNumbers,
    #[serde(rename = "passingTimes")]
    pub passing_times: PassingTimes,
    pub facilities: Option<Facilities>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ServiceAlteration {
    #[serde(rename = "extraJourney")]
    ExtraJourney,
    #[serde(rename = "cancellation")]
    Cancellation,
    #[serde(rename = "provisional")]
    Provisional,
    #[serde(rename = "planned")]
    Planned,
    #[serde(rename = "replaced")]
    Replaced,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceJourneyValidBetween {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "FromDate")]
    pub from_date: NaiveDateTime,
    #[serde(rename = "ToDate")]
    pub to_date: NaiveDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceJourneyBrandingRef {
    #[serde(rename = "@ref")]
    pub branding_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceJourneyTransportSubmode {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "RailSubmode")]
    pub rail_submode: Option<RailSubmode>,
    #[serde(rename = "CoachSubmode")]
    pub coach_submode: Option<CoachSubmode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceJourneyDayTypes {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "DayTypeRef")]
    pub day_type_ref: DayTypesDayTypeRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DayTypesDayTypeRef {
    #[serde(rename = "@ref")]
    pub day_type_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPatternRef {
    #[serde(rename = "@ref")]
    pub journey_pattern_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceJourneyOperatorRef {
    #[serde(rename = "@ref")]
    pub operator_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceJourneyLineRef {
    #[serde(rename = "@ref")]
    pub line_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceJourneyTrainNumbers {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "TrainNumberRef")]
    pub train_number_ref: TrainNumbersTrainNumberRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrainNumbersTrainNumberRef {
    #[serde(rename = "@ref")]
    pub train_number_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PassingTimes {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "TimetabledPassingTime")]
    pub timetabled_passing_time: Vec<TimetabledPassingTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimetabledPassingTime {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "PointInJourneyPatternRef")]
    pub point_in_journey_pattern_ref: PointInJourneyPatternRef,
    #[serde(rename = "DepartureTime")]
    pub departure_time: Option<NaiveTime>,
    #[serde(rename = "ArrivalTime")]
    pub arrival_time: Option<NaiveTime>,
    #[serde(rename = "ArrivalDayOffset")]
    pub arrival_day_offset: Option<u8>,
    #[serde(rename = "DepartureDayOffset")]
    pub departure_day_offset: Option<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointInJourneyPatternRef {
    #[serde(rename = "@ref")]
    pub point_in_journey_pattern_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Facilities {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ServiceFacilitySet")]
    pub service_facility_set: ServiceFacilitySet,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceFacilitySet {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "FareClasses")]
    pub fare_classes: Option<FareClasses>,
    #[serde(rename = "SanitaryFacilityList")]
    pub sanitary_facility_list: Option<SanitaryFacilities>,
    #[serde(rename = "AccommodationFacilityList")]
    pub accommodation_facility_list: Option<AccommodationFacilities>,
    #[serde(rename = "ServiceReservationFacilityList")]
    pub service_reservation_facility_list: Option<ReservationList>,
    #[serde(rename = "GroupBookingFacility")]
    pub group_booking_facility: Option<GroupBooking>,
    #[serde(rename = "LuggageCarriageFacilityList")]
    pub luggage_carriage_facility_list: Option<LuggageCarriageList>,
    #[serde(rename = "CateringFacilityList")]
    pub catering_facility_list: Option<CateringFacilities>,
    #[serde(rename = "FamilyFacilityList")]
    pub family_facility_list: Option<FamilyFacility>,
    #[serde(rename = "PassengerCommsFacilityList")]
    pub passenger_comms_facility_list: Option<PassengerCommsFacilities>,
    #[serde(rename = "AssistanceFacilityList")]
    pub assistance_facility_list: Option<AssistanceFacilities>,
    #[serde(rename = "AccessibilityInfoFacilityEnumeration")]
    pub accessibility_info_facility_enumeration: Option<AccessibilityInfoFacility>,
    #[serde(rename = "AssistanceFacilityEnumeration")]
    pub assistance_facility_enumeration: Option<AssistanceFacility>,
    #[serde(rename = "GenderLimitation")]
    pub gender_limitation: Option<GenderLimitation>,
    #[serde(rename = "PassengerInformationFacilityEnumeration")]
    pub passenger_information_facility_enumeration: Option<PassengerInformationFacility>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FareClasses {
    #[serde(rename = "$text")]
    pub text: Vec<FareClass>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FareClass {
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "firstClass")]
    FirstClass,
    #[serde(rename = "secondClass")]
    SecondClass,
    #[serde(rename = "thirdClass")]
    ThirdClass,
    #[serde(rename = "preferente")]
    Preferente,
    #[serde(rename = "premiumClass")]
    PremiumClass,
    #[serde(rename = "businessClass")]
    BusinessClass,
    #[serde(rename = "standardClass")]
    StandardClass,
    #[serde(rename = "turista")]
    Turista,
    #[serde(rename = "economyClass")]
    EconomyClass,
    #[serde(rename = "any")]
    Any,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SanitaryFacilities {
    #[serde(rename = "$text")]
    pub text: Vec<SanitaryFacility>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SanitaryFacility {
    #[serde(rename = "none")]
    SanitaryFacilityNone,
    #[serde(rename = "toilet")]
    Toilet,
    #[serde(rename = "washbasin")]
    Washbasin,
    #[serde(rename = "wheelChairAccessToilet")]
    WheelChairAccessToilet,
    #[serde(rename = "shower")]
    Shower,
    #[serde(rename = "washingAndChangeFacilities")]
    WashingAndChangeFacilities,
    #[serde(rename = "babyChange")]
    BabyChange,
    #[serde(rename = "wheelchairBabyChange")]
    WheelchairBabyChange,
    #[serde(rename = "shoeShiner")]
    ShoeShiner,
    #[serde(rename = "other")]
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccommodationFacilities {
    #[serde(rename = "$text")]
    pub text: Vec<AccommodationFacility>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AccommodationFacility {
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "standing")]
    Standing,
    #[serde(rename = "seating")]
    Seating,
    #[serde(rename = "sleeper")]
    Sleeper,
    #[serde(rename = "singleSleeper")]
    SingleSleeper,
    #[serde(rename = "doubleSleeper")]
    DoubleSleeper,
    #[serde(rename = "specialSleeper")]
    SpecialSleeper,
    #[serde(rename = "couchette")]
    Couchette,
    #[serde(rename = "singleCouchette")]
    SingleCouchette,
    #[serde(rename = "doubleCouchette")]
    DoubleCouchette,
    #[serde(rename = "specialSeating")]
    SpecialSeating,
    #[serde(rename = "recliningSeats")]
    RecliningSeats,
    #[serde(rename = "babyCompartment")]
    BabyCompartment,
    #[serde(rename = "familyCarriage")]
    FamilyCarriage,
    #[serde(rename = "recreationArea")]
    RecreationArea,
    #[serde(rename = "panoramaCoach")]
    PanoramaCoach,
    #[serde(rename = "pullmanCoach")]
    PullmanCoach,
    #[serde(rename = "pushchair")]
    Pushchair,
    #[serde(rename = "wheelchair")]
    Wheelchair,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservationList {
    #[serde(rename = "$text")]
    pub text: Vec<Reservation>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Reservation {
    #[serde(rename = "reservationsCompulsory")]
    ReservationsCompulsory,
    #[serde(rename = "reservationsCompulsoryForGroups")]
    ReservationsCompulsoryForGroups,
    #[serde(rename = "reservationsCompulsoryForFirstClass")]
    ReservationsCompulsoryForFirstClass,
    #[serde(rename = "reservationsCompulsoryFromOriginStation")]
    ReservationsCompulsoryFromOriginStation,
    #[serde(rename = "reservationsRecommended")]
    ReservationsRecommended,
    #[serde(rename = "reservationsPossible")]
    ReservationsPossible,
    #[serde(rename = "reservationsPossibleOnlyInFirstClass")]
    ReservationsPossibleOnlyInFirstClass,
    #[serde(rename = "reservationsPossibleOnlyInSecondClass")]
    ReservationsPossibleOnlyInSecondClass,
    #[serde(rename = "reservationsPossibleForCertainClasses")]
    ReservationsPossibleForCertainClasses,
    #[serde(rename = "groupBookingRestricted")]
    GroupBookingRestricted,
    #[serde(rename = "noGroupsAllowed")]
    NoGroupsAllowed,
    #[serde(rename = "noReservationsPossible")]
    NoReservationsPossible,
    #[serde(rename = "wheelchairOnlyReservations")]
    WheelchairOnlyReservations,
    #[serde(rename = "bicycleReservationsCompulsory")]
    BicycleReservationsCompulsory,
    #[serde(rename = "reservationsSupplementCharged")]
    ReservationsSupplementCharged,
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum GroupBooking {
    #[serde(rename = "groupsAllowed")]
    GroupsAllowed,
    #[serde(rename = "groupsNotAllowed")]
    GroupsNotAllowed,
    #[serde(rename = "groupsAllowedWithReservation")]
    GroupsAllowedWithReservation,
    #[serde(rename = "groupBookingsRestricted")]
    GroupBookingsRestricted,
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LuggageCarriageList {
    #[serde(rename = "$text")]
    pub text: Vec<LuggageCarriage>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LuggageCarriage {
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "noBaggageStorage")]
    NoBaggageStorage,
    #[serde(rename = "baggageStorage")]
    BaggageStorage,
    #[serde(rename = "luggageRacks")]
    LuggageRacks,
    #[serde(rename = "skiRacks")]
    SkiRacks,
    #[serde(rename = "skiRacksOnRear")]
    SkiRacksOnRear,
    #[serde(rename = "extraLargeLuggageRacks")]
    ExtraLargeLuggageRacks,
    #[serde(rename = "baggageVan")]
    BaggageVan,
    #[serde(rename = "noCycles")]
    NoCycles,
    #[serde(rename = "cyclesAllowed")]
    CyclesAllowed,
    #[serde(rename = "cyclesAllowedInVan")]
    CyclesAllowedInVan,
    #[serde(rename = "cyclesAllowedInCarriage")]
    CyclesAllowedInCarriage,
    #[serde(rename = "cyclesAllowedWithReservation")]
    CyclesAllowedWithReservation,
    #[serde(rename = "pushchairsAllowed")]
    PushchairsAllowed,
    #[serde(rename = "vehicleTransport")]
    VehicleTransport,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CateringFacilities {
    #[serde(rename = "$text")]
    pub text: Vec<CateringFacility>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CateringFacility {
    #[serde(rename = "bar")]
    Bar,
    #[serde(rename = "bistro")]
    Bistro,
    #[serde(rename = "buffet")]
    Buffet,
    #[serde(rename = "noFoodAvailable")]
    NoFoodAvailable,
    #[serde(rename = "noBeveragesAvailable")]
    NoBeveragesAvailable,
    #[serde(rename = "restaurant")]
    Restaurant,
    #[serde(rename = "firstClassRestaurant")]
    FirstClassRestaurant,
    #[serde(rename = "trolley")]
    Trolley,
    #[serde(rename = "coffeeShop")]
    CoffeeShop,
    #[serde(rename = "hotFoodService")]
    HotFoodService,
    #[serde(rename = "selfService")]
    SelfService,
    #[serde(rename = "snacks")]
    Snacks,
    #[serde(rename = "foodVendingMachine")]
    FoodVendingMachine,
    #[serde(rename = "beverageVendingMachine")]
    BeverageVendingMachine,
    #[serde(rename = "miniBar")]
    MiniBar,
    #[serde(rename = "breakfastInCar")]
    BreakfastInCar,
    #[serde(rename = "mealAtSeat")]
    MealAtSeat,
    #[serde(rename = "other")]
    Other,
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FamilyFacility {
    #[serde(rename = "none")]
    FamilyFacilityNone,
    #[serde(rename = "servicesForChildren")]
    ServicesForChildren,
    #[serde(rename = "servicesForArmyFamilies")]
    ServicesForArmyFamilies,
    #[serde(rename = "nurseryService")]
    NurseryService,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PassengerCommsFacilities {
    #[serde(rename = "$text")]
    pub text: Vec<PassengerCommsFacility>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PassengerCommsFacility {
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "freeWifi")]
    FreeWifi,
    #[serde(rename = "publicWifi")]
    PublicWifi,
    #[serde(rename = "powerSupplySockets")]
    PowerSupplySockets,
    #[serde(rename = "telephone")]
    Telephone,
    #[serde(rename = "audioEntertainment")]
    AudioEntertainment,
    #[serde(rename = "videoEntertainment")]
    VideoEntertainment,
    #[serde(rename = "businessServices")]
    BusinessServices,
    #[serde(rename = "internet")]
    Internet,
    #[serde(rename = "postOffice")]
    PostOffice,
    #[serde(rename = "postBox")]
    PostBox,
    #[serde(rename = "usbAPowerSocket")]
    UsbAPowerSocket,
    #[serde(rename = "usbCPowerSocket")]
    UsbCPowerSocket,
    #[serde(rename = "other")]
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssistanceFacilities {
    #[serde(rename = "$text")]
    pub text: Vec<AssistanceFacility>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AssistanceFacility {
    #[serde(rename = "personalAssistance")]
    PersonalAssistance,
    #[serde(rename = "boardingAssistance")]
    BoardingAssistance,
    #[serde(rename = "wheelchairAssistance")]
    WheelchairAssistance,
    #[serde(rename = "unaccompaniedMinorAssistance")]
    UnaccompaniedMinorAssistance,
    #[serde(rename = "wheelchairUse")]
    WheelchairUse,
    #[serde(rename = "conductor")]
    Conductor,
    #[serde(rename = "information")]
    Information,
    #[serde(rename = "other")]
    Other,
    #[serde(rename = "none")]
    AssistanceFacilityNone,
    #[serde(rename = "any")]
    Any,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AccessibilityInfoFacility {
    #[serde(rename = "audioInformation")]
    AudioInformation,
    #[serde(rename = "audioForHearingImpaired")]
    AudioForHearingImpaired,
    #[serde(rename = "visualDisplays")]
    VisualDisplays,
    #[serde(rename = "displaysForVisuallyImpaired")]
    DisplaysForVisuallyImpaired,
    #[serde(rename = "largePrintTimetables")]
    LargePrintTimetables,
    #[serde(rename = "other")]
    Other,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum GenderLimitation {
    #[serde(rename = "both")]
    Both,
    #[serde(rename = "femaleOnly")]
    FemaleOnly,
    #[serde(rename = "maleOnly")]
    MaleOnly,
    #[serde(rename = "sameSexOnly")]
    SameSexOnly,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PassengerInformationFacility {
    #[serde(rename = "nextStopIndicator")]
    NextStopIndicator,
    #[serde(rename = "stopAnnouncements")]
    StopAnnouncements,
    #[serde(rename = "passengerInformationDisplay")]
    PassengerInformationDisplay,
    #[serde(rename = "realTimeConnections")]
    RealTimeConnections,
    #[serde(rename = "other")]
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimetableFrameTrainNumbers {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "TrainNumber")]
    pub train_number: Vec<TrainNumber>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrainNumber {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "@responsibilitySetRef")]
    pub responsibility_set_ref: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "ForAdvertisement")]
    pub for_advertisement: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartCouples {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "JourneyPartCouple")]
    pub journey_part_couple: Vec<JourneyPartCouple>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartCouple {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@order")]
    pub order: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "StartTime")]
    pub start_time: NaiveTime,
    #[serde(rename = "EndTime")]
    pub end_time: NaiveTime,
    #[serde(rename = "FromStopPointRef")]
    pub from_stop_point_ref: JourneyPartCoupleFromStopPointRef,
    #[serde(rename = "ToStopPointRef")]
    pub to_stop_point_ref: JourneyPartCoupleToStopPointRef,
    #[serde(rename = "MainPartRef")]
    pub main_part_ref: JourneyPartCoupleMainPartRef,
    #[serde(rename = "journeyParts")]
    pub journey_parts: JourneyParts,
    #[serde(rename = "TrainNumberRef")]
    pub train_number_ref: JourneyPartCoupleTrainNumberRef,
    #[serde(rename = "StartTimeDayOffset")]
    pub start_time_day_offset: Option<u8>,
    #[serde(rename = "EndTimeDayOffset")]
    pub end_time_day_offset: Option<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartCoupleFromStopPointRef {
    #[serde(rename = "@ref")]
    pub from_stop_point_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartCoupleToStopPointRef {
    #[serde(rename = "@ref")]
    pub to_stop_point_ref_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartCoupleMainPartRef {
    #[serde(rename = "@ref")]
    pub main_part_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyParts {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "JourneyPartRef")]
    pub journey_part_ref: Vec<JourneyPartRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartRef {
    #[serde(rename = "@ref")]
    pub journey_part_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyPartCoupleTrainNumberRef {
    #[serde(rename = "@ref")]
    pub train_number_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoupledJourneys {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "CoupledJourney")]
    pub coupled_journey: Vec<CoupledJourney>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoupledJourney {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    pub journeys: Journeys,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Journeys {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "VehicleJourneyRef")]
    pub vehicle_journey_ref: Vec<VehicleJourneyRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VehicleJourneyRef {
    #[serde(rename = "@ref")]
    pub vehicle_journey_ref_ref: String,
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypesOfService {
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "TypeOfService")]
    pub type_of_service: Vec<TypeOfService>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeOfService {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@version")]
    pub version: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
    #[serde(rename = "Name")]
    pub name: TypeOfServiceName,
    #[serde(rename = "ShortName")]
    pub short_name: TypeOfServiceShortName,
    #[serde(rename = "PrivateCode")]
    pub private_code: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeOfServiceName {
    #[serde(rename = "@lang")]
    pub lang: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeOfServiceShortName {
    #[serde(rename = "@lang")]
    pub lang: String,
    #[serde(rename = "$text")]
    pub text: Option<String>,
}

#[derive(Clone, Debug)]
pub enum NetexErrorType {
    BadColour(ParseIntError),
    DayBitsDontMatchPeriodLength(i64),
    DayTypeAssignmentNotFound(String),
    DestinationDisplayNotFound(String),
    JourneyPartNotFound(String),
    JourneyPartCoupleNotFound(String),
    ScheduledStopPointNotFound(String),
    ServiceJourneyPatternNotFound(String),
    StopPlaceNotFound(String),
    StopPointInJourneyPatternNotFound(String),
    TrainNumberNotFound(String),
    UicOperatingPeriodNotFound(String),
    UnexpectedTransportMode(AllPublicTransportModes),
    UnexpectedTransportSubmode(String),
    UnsupportedTimezone(String, String),
}

impl fmt::Display for NetexErrorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NetexErrorType::BadColour(x) => write!(f, "Bad colour component {}", x),
            NetexErrorType::DayBitsDontMatchPeriodLength(x) => write!(
                f, "Number of day bits does not match period length {}", x
            ),
            NetexErrorType::DayTypeAssignmentNotFound(x) => write!(
                f, "Day type assignment not found for day type {}", x
            ),
            NetexErrorType::DestinationDisplayNotFound(x) => write!(
                f, "Destination display not found {}", x
            ),
            NetexErrorType::JourneyPartNotFound(x) => write!(
                f, "Journey part couple not found {}", x
            ),
            NetexErrorType::JourneyPartCoupleNotFound(x) => write!(
                f, "Journey part couple not found {}", x
            ),
            NetexErrorType::ScheduledStopPointNotFound(x) => write!(
                f, "Scheduled stop point not found {}", x
            ),
            NetexErrorType::ServiceJourneyPatternNotFound(x) => write!(
                f, "Service journey pattern not found {}", x
            ),
            NetexErrorType::StopPlaceNotFound(x) => write!(f, "Stop place not found {}", x),
            NetexErrorType::StopPointInJourneyPatternNotFound(x) => write!(
                f, "Stop point in journey pattern not found {}", x
            ),
            NetexErrorType::TrainNumberNotFound(x) => write!(f, "Train number not found {}", x),
            NetexErrorType::UicOperatingPeriodNotFound(x) => write!(
                f, "UIC Operating Period not found {}", x
            ),
            NetexErrorType::UnexpectedTransportMode(x) => write!(
                f, "Unexpected transport mode {:?}", x
            ),
            NetexErrorType::UnexpectedTransportSubmode(x) => write!(
                f, "Unexpected transport submode {}", x
            ),
            NetexErrorType::UnsupportedTimezone(
                offset, summer_offset
            ) => write!(
                f, "Unsupported Timezone Offset {} Summer Offset {}", offset, summer_offset
            ),
        }
    }
}

#[derive(Debug)]
pub struct NetexError {
    error_type: NetexErrorType,
}

impl fmt::Display for NetexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error reading NeTEx file: {}", self.error_type)
    }
}

#[derive(Default)]
pub struct NetexImporter {
    coupled_journey_by_id: HashMap<String, CoupledJourney>,
    coupled_train_ids_by_train_id: HashMap<String, HashSet<String>>,
    destination_display_by_id: HashMap<String, DestinationDisplay>,
    journey_part_by_id: HashMap<String, JourneyPart>,
    journey_part_couple_by_id: HashMap<String, JourneyPartCouple>,
    journey_part_couple_ids_by_train_id: HashMap<String, HashSet<String>>,
    scheduled_stop_point_by_id: HashMap<String, ScheduledStopPoint>,
    service_journey_ids_by_train_number_id: HashMap<String, HashSet<String>>,
    service_journey_pattern_by_id: HashMap<String, ServiceJourneyPattern>,
    stop_place_by_id: HashMap<String, StopPlace>,
    train_number_by_id: HashMap<String, TrainNumber>,
    uic_operating_period_by_id: HashMap<String, UicOperatingPeriod>,
    uic_operating_period_ids_by_day_type_id: HashMap<String, Vec<String>>,
}

impl NetexImporter {
    pub fn new() -> NetexImporter {
        NetexImporter {
            coupled_journey_by_id: HashMap::new(),
            coupled_train_ids_by_train_id: HashMap::new(),
            destination_display_by_id: HashMap::new(),
            journey_part_by_id: HashMap::new(),
            journey_part_couple_by_id: HashMap::new(),
            journey_part_couple_ids_by_train_id: HashMap::new(),
            scheduled_stop_point_by_id: HashMap::new(),
            service_journey_ids_by_train_number_id: HashMap::new(),
            service_journey_pattern_by_id: HashMap::new(),
            stop_place_by_id: HashMap::new(),
            train_number_by_id: HashMap::new(),
            uic_operating_period_by_id: HashMap::new(),
            uic_operating_period_ids_by_day_type_id: HashMap::new(),
            ..Default::default()
        }
    }

    fn get_timezone(&self, locale: &Locale) -> Result<String, NetexError> {
        match (&locale.time_zone_offset[..], &locale.summer_time_zone_offset[..]) {
            // There's a bug in some SNCF data that shows some stations as not having DST...
            ("+1", "+2") | ("+1", "+1") => Ok(CET.name().to_string()),
            (&_, &_) => Err(
                NetexError {
                    error_type: NetexErrorType::UnsupportedTimezone(
                        locale.time_zone_offset.to_string(),
                        locale.summer_time_zone_offset.to_string(),
                    ),
                }
            ),
        }
    }

    pub async fn read_publication_delivery(
        &mut self,
        publication_delivery: &PublicationDelivery,
        schedule: &schedule::ModelEx,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        let composite_frame = &publication_delivery.data_objects.composite_frame;
        let default_timezone = self.get_timezone(
            &composite_frame.frame_defaults.default_locale
        )?;

        let namespace = schedule.namespace.clone();

        let mut active_schedule: schedule::ActiveModelEx = schedule.clone().into();

        active_schedule.valid_begin = ActiveValue::Set(
            Some(composite_frame.valid_between.from_date.clone())
        );
        active_schedule.valid_end = ActiveValue::Set(
            Some(composite_frame.valid_between.to_date.clone())
        );
        active_schedule.last_updated = ActiveValue::Set(
            Some(publication_delivery.publication_timestamp.clone())
        );
        active_schedule.timezone = ActiveValue::Set(Some(default_timezone.clone()));

        println!("[{}] Updating root schedule...", namespace);
        active_schedule.update(transaction).await?;
        println!("[{}] Updated root schedule", namespace);

        // Stop places include crucial timezone info so we need to store them until we are ready for
        // the scheduled stop point
        for stop_place in &composite_frame.frames.site_frame.stop_places.stop_place {
            self.read_stop_place(&stop_place)?;
        }

        // Also save scheduled stop points
        for scheduled_stop_point
            in &composite_frame.frames.service_frame.scheduled_stop_points.scheduled_stop_point {
            self.read_scheduled_stop_point(&scheduled_stop_point)?;
        }

        // Now go through the stop assignments and convert to locations
        let mut location_count: usize = 0;
        println!("[{}] Loading locations...", namespace);
        for stop_assignment
            in &composite_frame.frames.service_frame.stop_assignments.passenger_stop_assignment {
            self.read_stop_assignment(&stop_assignment, &namespace, transaction).await?;
            location_count += 1;
        }
        println!("[{}] Persisted {} locations", namespace, location_count);

        // Store operating periods
        for uic_operating_period
            in &composite_frame.frames.service_calendar_frame.operating_periods.uic_operating_period
        {
            self.read_uic_operating_period(&uic_operating_period)?;
        }

        // Store the mapping between the two
        for day_type_assignment in &composite_frame
            .frames
            .service_calendar_frame
            .day_type_assignments
            .day_type_assignment {
            self.read_day_type_assignment(&day_type_assignment)?;
        }

        // Load train numbers
        for train_number in &composite_frame.frames.timetable_frame.train_numbers.train_number {
            self.read_train_number(&train_number)?;
        }

        // Load lines
        let mut line_count: usize = 0;
        println!("[{}] Loading lines...", namespace);
        for line in &composite_frame.frames.service_frame.lines.line {
            self.read_line(&line, &namespace, transaction).await?;
            line_count += 1;
        }
        println!("[{}] Persisted {} lines", namespace, line_count);

        // Load destination displays
        for destination_display
            in &composite_frame.frames.service_frame.destination_displays.destination_display {
            self.read_destination_display(&destination_display)?;
        }

        // Load operators
        let mut operator_count: usize = 0;
        println!("[{}] Loading operators...", namespace);
        for operator in &composite_frame.frames.resource_frame.organisations.operator {
            self.read_operator(&operator, &namespace, transaction).await?;
            operator_count += 1;
        }
        println!("[{}] Persisted {} operators", namespace, operator_count);

        // Load journey patterns
        for service_journey_pattern
            in &composite_frame.frames.service_frame.journey_patterns.service_journey_pattern {
            self.read_service_journey_pattern(&service_journey_pattern)?;
        }

        // Load journey parts
        for journey_part in &composite_frame.frames.general_frame.members.journey_part {
            self.read_journey_part(&journey_part)?;
        }

        // Load coupled journey parts
        for journey_part_couple
            in &composite_frame.frames.timetable_frame.journey_part_couples.journey_part_couple {
            self.read_journey_part_couple(&journey_part_couple)?;
        }

        // Load coupled journeys
        for coupled_journey
            in &composite_frame.frames.timetable_frame.coupled_journeys.coupled_journey {
            self.read_coupled_journey(&coupled_journey)?;
        }

        // Generate map of trains to other coupled trains
        for coupled_journey
            in &composite_frame.frames.timetable_frame.coupled_journeys.coupled_journey {
            self.fill_train_couple_map(&coupled_journey)?;
        }

        // Generate map of trains to journey part couples
        for journey_part in &composite_frame.frames.general_frame.members.journey_part {
            self.fill_journey_part_map(&journey_part)?;
        }

        // We have to do an initial pass first to get a map of train number ID -> service journey
        // IDs. This is because coupled trains are keyed off both train number ID and service
        // journey ID in different places, and so without a first pass we don't know if we've
        // already seen that train or not. No, I've no idea why it's done this way...
        for service_journey
            in &composite_frame.frames.timetable_frame.vehicle_journeys.service_journey {
            self.fill_train_id_service_journey_map(&service_journey)?;
        }

        // Now we can load the trains into the schedule
        let mut train_count: usize = 0;
        println!("[{}] Loading trains...", namespace);
        for service_journey
            in &composite_frame.frames.timetable_frame.vehicle_journeys.service_journey {
            self.read_service_journey(
                &service_journey, &namespace, &default_timezone, transaction
            ).await?;
            train_count += 1;
        }
        println!("[{}] Persisted {} trains", namespace, train_count);

        Ok(())
    }

    fn fill_train_id_service_journey_map(
        &mut self, service_journey: &ServiceJourney
    ) -> Result<(), NetexError> {
        let train_number_ref = &service_journey.train_numbers.train_number_ref;
        self.service_journey_ids_by_train_number_id
            .entry(train_number_ref.train_number_ref_ref.clone())
            .or_insert(HashSet::new())
            .insert(service_journey.id.clone());
        Ok(())
    }

    fn fill_journey_part_map(&mut self, journey_part: &JourneyPart) -> Result<(), NetexError> {
        match &journey_part.journey_part_couple_ref {
            Some(journey_part_couple_ref) => {
                self.journey_part_couple_ids_by_train_id
                    .entry(journey_part.parent_journey_ref.parent_journey_ref_ref.clone())
                    .or_insert(HashSet::new())
                    .insert(journey_part_couple_ref.journey_part_couple_ref_ref.clone());
                for other_train_id in self.coupled_train_ids_by_train_id.get(
                    &journey_part.parent_journey_ref.parent_journey_ref_ref
                ).unwrap_or(&HashSet::new()) {
                    self.journey_part_couple_ids_by_train_id
                        .entry(other_train_id.clone())
                        .or_insert(HashSet::new())
                        .insert(journey_part_couple_ref.journey_part_couple_ref_ref.clone());
                }
            },
            None => (),
        };
        Ok(())
    }

    fn fill_train_couple_map(
        &mut self, coupled_journey: &CoupledJourney
    ) -> Result<(), NetexError> {
        for vehicle_journey_1 in &coupled_journey.journeys.vehicle_journey_ref {
            for vehicle_journey_2 in &coupled_journey.journeys.vehicle_journey_ref {
                if vehicle_journey_2.vehicle_journey_ref_ref
                    != vehicle_journey_1.vehicle_journey_ref_ref {
                    self.coupled_train_ids_by_train_id
                        .entry(vehicle_journey_1.vehicle_journey_ref_ref.clone())
                        .or_insert(HashSet::new())
                        .insert(vehicle_journey_2.vehicle_journey_ref_ref.clone());
                }
            }
        }
        Ok(())
    }

    fn read_coupled_journey(
        &mut self, coupled_journey: &CoupledJourney
    ) -> Result<(), NetexError> {
        self.coupled_journey_by_id.insert(coupled_journey.id.clone(), coupled_journey.clone());
        Ok(())
    }

    fn read_journey_part_couple(
        &mut self, journey_part_couple: &JourneyPartCouple
    ) -> Result<(), NetexError> {
        self.journey_part_couple_by_id.insert(
            journey_part_couple.id.clone(), journey_part_couple.clone()
        );
        Ok(())
    }

    fn read_journey_part(&mut self, journey_part: &JourneyPart) -> Result<(), NetexError> {
        self.journey_part_by_id.insert(journey_part.id.clone(), journey_part.clone());
        Ok(())
    }

    fn read_service_journey_pattern(
        &mut self, service_journey_pattern: &ServiceJourneyPattern
    ) -> Result<(), NetexError> {
        self.service_journey_pattern_by_id.insert(
            service_journey_pattern.id.clone(), service_journey_pattern.clone()
        );
        Ok(())
    }

    async fn read_operator(
        &mut self, operator: &Operator, namespace: &str, transaction: &DatabaseTransaction
    ) -> Result<(), Error> {
        let operator = train_operator::ActiveModelEx {
            id: ActiveValue::Set(operator.id.clone()),
            namespace: ActiveValue::Set(namespace.to_string()),
            public_id: ActiveValue::Set(Some(operator.public_code.clone())),
            description: ActiveValue::Set(Some(operator.name.clone())),
            ..Default::default()
        };

        operator.insert(transaction).await?;

        Ok(())
    }

    fn read_destination_display(
        &mut self, destination_display: &DestinationDisplay
    ) -> Result<(), NetexError> {
        self.destination_display_by_id.insert(
            destination_display.id.clone(), destination_display.clone()
        );
        Ok(())
    }

    async fn read_line(
        &mut self, line: &Line, namespace: &str, transaction: &DatabaseTransaction
    ) -> Result<(), Error> {
        let line = line::ActiveModelEx {
            id: ActiveValue::Set(line.id.clone()),
            namespace: ActiveValue::Set(namespace.to_string()),
            public_id: ActiveValue::Set(Some(line.public_code.clone())),
            name: ActiveValue::Set(Some(line.name.clone())),
            description: ActiveValue::Set(Some(line.description.clone())),
            url: ActiveValue::Set(None),
            background_colour: ActiveValue::Set(match &line.presentation {
                Some(presentation) => match &presentation.colour {
                    Some(colour) => Some(
                        (match u32::from_str_radix(&colour[0..2], 16) {
                            Ok(component) => component,
                            Err(x) => return Err(NetexError {
                                error_type: NetexErrorType::BadColour(x)
                            }.into()),
                        } << 16) + 
                        (match u32::from_str_radix(&colour[2..4], 16){
                            Ok(component) => component,
                            Err(x) => return Err(NetexError {
                                error_type: NetexErrorType::BadColour(x)
                            }.into()),
                        } << 8) + 
                        match u32::from_str_radix(&colour[4..6], 16){
                            Ok(component) => component,
                            Err(x) => return Err(NetexError {
                                error_type: NetexErrorType::BadColour(x)
                            }.into()),
                        }
                    ),
                    None => None,
                },
                None => None,
            }),
            foreground_colour: ActiveValue::Set(match &line.presentation {
                Some(presentation) => match &presentation.text_colour {
                    Some(colour) => Some(
                        (match u32::from_str_radix(&colour[0..2], 16) {
                            Ok(component) => component,
                            Err(x) => return Err(NetexError {
                                error_type: NetexErrorType::BadColour(x)
                            }.into()),
                        } << 16) + 
                        (match u32::from_str_radix(&colour[2..4], 16){
                            Ok(component) => component,
                            Err(x) => return Err(NetexError {
                                error_type: NetexErrorType::BadColour(x)
                            }.into()),
                        } << 8) + 
                        match u32::from_str_radix(&colour[4..6], 16){
                            Ok(component) => component,
                            Err(x) => return Err(NetexError {
                                error_type: NetexErrorType::BadColour(x)
                            }.into()),
                        }
                    ),
                    None => None,
                },
                None => None,
            }),
            ..Default::default()
        };

        line.insert(transaction).await?;

        Ok(())
    }

    fn read_train_number(&mut self, train_number: &TrainNumber) -> Result<(), NetexError> {
        self.train_number_by_id.insert(train_number.id.clone(), train_number.clone());
        Ok(())
    }

    fn read_stop_place(
        &mut self,
        stop_place: &StopPlace,
    ) -> Result<(), NetexError> {
        let stop_place = match stop_place.modification {
            Modification::New => stop_place,
            Modification::Revise | Modification::Delta | Modification::Unchanged => {
                self.stop_place_by_id.remove(&stop_place.id);
                // We don't actually care whether we found one to revise or not
                stop_place
            }
            Modification::Delete => {
                self.stop_place_by_id.remove(&stop_place.id); // it's OK if the ID isn't found
                return Ok(());
            }
        };
        self.stop_place_by_id.insert(stop_place.id.clone(), stop_place.clone());
        Ok(())
    }

    fn read_scheduled_stop_point(
        &mut self,
        scheduled_stop_point: &ScheduledStopPoint,
    ) -> Result<(), NetexError> {
        self.scheduled_stop_point_by_id.insert(
            scheduled_stop_point.id.clone(), scheduled_stop_point.clone()
        );
        Ok(())
    }

    async fn read_stop_assignment(
        &self,
        passenger_stop_assignment: &PassengerStopAssignment,
        namespace: &str,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        let stop_place = match self.stop_place_by_id.get(
            &passenger_stop_assignment.stop_place_ref.stop_place_ref_ref
        ) {
            Some(x) => x,
            None => return Err(
                NetexError {
                    error_type: NetexErrorType::StopPlaceNotFound(
                        passenger_stop_assignment.stop_place_ref.stop_place_ref_ref.clone()
                    )
                }.into()
            ),
        };

        let scheduled_stop_point = match self.scheduled_stop_point_by_id.get(
            &passenger_stop_assignment.scheduled_stop_point_ref.scheduled_stop_point_ref_ref
        ) {
            Some(x) => x,
            None => return Err(
                NetexError {
                    error_type: NetexErrorType::ScheduledStopPointNotFound(
                        passenger_stop_assignment
                            .scheduled_stop_point_ref
                            .scheduled_stop_point_ref_ref
                            .clone()
                    )
                }.into()
            ),
        };

        let location = location::ActiveModelEx {
            id: ActiveValue::Set(scheduled_stop_point.id.clone()),
            namespace: ActiveValue::Set(namespace.to_owned()),
            name: ActiveValue::Set(scheduled_stop_point.name.text.clone()),
            public_id: ActiveValue::Set(Some(scheduled_stop_point.public_code.clone())),
            timezone: ActiveValue::Set(self.get_timezone(&stop_place.locale)?),
            ..Default::default()
        };

        location.insert(transaction).await?;

        Ok(())
    }

    fn read_uic_operating_period(
        &mut self,
        uic_operating_period: &UicOperatingPeriod,
    ) -> Result<(), NetexError> {
        self.uic_operating_period_by_id.insert(
            uic_operating_period.id.clone(), uic_operating_period.clone()
        );
        Ok(())
    }

    fn read_day_type_assignment(
        &mut self,
        day_type_assignment: &DayTypeAssignment,
    ) -> Result<(), NetexError> {
        self
            .uic_operating_period_ids_by_day_type_id
            .entry(day_type_assignment.day_type_ref.day_type_ref_ref.clone())
            .or_insert(vec![])
            .push(day_type_assignment.operating_period_ref.operating_period_ref_ref.clone());
        Ok(())
    }

    fn try_overlay_days(
        &self,
        mut original: [Option::<bool>; 7],
        overlay: [Option::<bool>; 7],
    ) -> Option<[Option<bool>; 7]> {
        for i in 0..7 {
            if original[i] != overlay[i] {
                match original[i] {
                    Some(_) => {
                        match overlay[i] {
                            Some(_) => { return None; },
                            None => (),
                        }
                    },
                    None => {
                        original[i] = overlay[i];
                    },
                }
            }
        }

        Some(original)
    }

    fn calculate_validities(
        &self,
        valid_between: &ServiceJourneyValidBetween,
        operating_periods: &Vec<&UicOperatingPeriod>,
        default_timezone: &str,
    ) -> Result<Vec<train_validity_period::ActiveModelEx>, NetexError> {
        // Nothing outside `valid_between` is valid so we keep that as a hard cap
        let min_date = &valid_between.from_date.date();
        let max_date = &valid_between.to_date.date();

        let mut train_validity_periods = vec![];

        for operating_period in operating_periods {
            // valid_day_bits should be the same length as the period between the two dates plus one
            // (valid days are inclusive)
            let operating_period_days = (
                operating_period.to_date.date() - operating_period.from_date.date()
            ).num_days() + 1;
            if operating_period.valid_day_bits.len()
                != usize::try_from(operating_period_days).unwrap() {
                return Err(
                    NetexError {
                        error_type: NetexErrorType::DayBitsDontMatchPeriodLength(
                            operating_period_days
                        )
                    }
                );
            }
            let start_date = cmp::max(*min_date, operating_period.from_date.date()).clone();
            let end_date = cmp::min(*max_date, operating_period.to_date.date()).clone();
            let start_index = if start_date > operating_period.from_date.date() {
                usize::try_from(
                    (start_date - operating_period.from_date.date()).num_days()
                ).unwrap()
            } else {
                0
            };

            let end_index = if end_date < operating_period.to_date.date() {
                operating_period.valid_day_bits.len() - 
                    usize::try_from(
                        (operating_period.to_date.date() - end_date).num_days()
                    ).unwrap()
            } else {
                operating_period.valid_day_bits.len()
            };

            let bit_portion = &operating_period.valid_day_bits[start_index..end_index];

            let first_day_offset = usize::try_from(
                start_date.weekday().num_days_from_monday()
            ).unwrap();

            let first_full_week_index = (7 - first_day_offset) % 7;

            // First extract the weeks into vectors
            let mut weeks = vec![];
            let mut idx: usize = 0;

            if first_full_week_index > 0 {
                let mut first_week = [None; 7];
                while idx < first_full_week_index && idx < bit_portion.len() {
                    first_week[idx + first_day_offset] = Some(bit_portion.as_bytes()[idx] == b'1');
                    idx += 1;
                }
                weeks.push(first_week);
            }

            while idx < bit_portion.len() {
                let mut week = [None; 7];
                loop {
                    week[(idx - first_full_week_index) % 7] = Some(
                        bit_portion.as_bytes()[idx] == b'1'
                    );
                    idx += 1;
                    if (idx - first_full_week_index) % 7 == 0 || idx >= bit_portion.len() {
                        break
                    }
                }
                weeks.push(week);
            }

            // now we have a vector of each week, we can check each's consistency against the last
            let mut maybe_days_of_week = [None; 7];
            let mut cur_start = start_date.clone();
            let mut cur_end = cur_start.clone();
            for week in weeks {
                maybe_days_of_week = match self.try_overlay_days(
                    maybe_days_of_week.clone(), week.clone()
                ) {
                    Some(new_days_of_week) => {
                        cur_end = cur_end + Days::new(
                            u64::try_from(week.iter().filter(|x| { x.is_some() }).count()).unwrap()
                        );
                        new_days_of_week
                    },
                    None => {
                        let train_validity_period = train_validity_period::ActiveModelEx {
                            valid_begin:
                                ActiveValue::Set(cur_start.and_hms_opt(0, 0, 0).unwrap()),
                            valid_end: ActiveValue::Set(
                                    (cur_end - Days::new(1)).and_hms_opt(0, 0, 0).unwrap()
                                ),
                            timezone: ActiveValue::Set(default_timezone.to_string()),
                            monday: ActiveValue::Set(maybe_days_of_week[0].unwrap_or(false)),
                            tuesday: ActiveValue::Set(maybe_days_of_week[1].unwrap_or(false)),
                            wednesday: ActiveValue::Set(maybe_days_of_week[2].unwrap_or(false)),
                            thursday: ActiveValue::Set(maybe_days_of_week[3].unwrap_or(false)),
                            friday: ActiveValue::Set(maybe_days_of_week[4].unwrap_or(false)),
                            saturday: ActiveValue::Set(maybe_days_of_week[5].unwrap_or(false)),
                            sunday: ActiveValue::Set(maybe_days_of_week[6].unwrap_or(false)),
                            ..Default::default()
                        };
                        if maybe_days_of_week.iter().filter(
                            |x| { x.unwrap_or(false) }
                        ).count() != 0 {
                            train_validity_periods.push(train_validity_period);
                        }
                        cur_start = cur_end.clone();
                        cur_end = cur_end + Days::new(
                            u64::try_from(week.iter().filter(|x| { x.is_some() }).count()).unwrap()
                        );
                        week
                    },
                };
            };
        }

        Ok(train_validity_periods)
    }

    fn assert_submodes_not_present(
        &self,
        rail: bool,
        coach: bool,
        transport_submode: &ServiceJourneyTransportSubmode
    ) -> Result<(), NetexError> {
        if rail && transport_submode.rail_submode.is_some() {
            return Err(
                NetexError {
                    error_type: NetexErrorType::UnexpectedTransportSubmode(
                        "rail_submode".to_string()
                    )
                }
            )
        }
        if coach && transport_submode.coach_submode.is_some() {
            return Err(
                NetexError {
                    error_type: NetexErrorType::UnexpectedTransportSubmode(
                        "coach_submode".to_string()
                    )
                }
            )
        }

        Ok(())
    }

    fn get_train_type(
        &self,
        transport_mode: AllPublicTransportModes,
        transport_submode: &ServiceJourneyTransportSubmode
    ) -> Result<TrainType, NetexError> {
        match transport_mode {
            AllPublicTransportModes::All => return Err(
                NetexError {
                    error_type: NetexErrorType::UnexpectedTransportMode(
                        AllPublicTransportModes::All
                    )
                }
            ),
            AllPublicTransportModes::Unknown => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::Unknown)
            },
            AllPublicTransportModes::Bus => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::Bus)
            },
            AllPublicTransportModes::TrolleyBus => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::Trolleybus)
            },
            AllPublicTransportModes::Tram => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::Tram)
            },
            AllPublicTransportModes::Coach => {
                self.assert_submodes_not_present(true, false, transport_submode)?;
                match &transport_submode.coach_submode {
                    Some(coach_submode) => match coach_submode {
                        CoachSubmode::Unknown => Ok(TrainType::Coach),
                        CoachSubmode::Undefined => Ok(TrainType::UndefinedCoach),
                        CoachSubmode::InternationalCoach => Ok(TrainType::InternationalCoach),
                        CoachSubmode::NationalCoach => Ok(TrainType::NationalCoach),
                        CoachSubmode::ShuttleCoach => Ok(TrainType::ShuttleCoach),
                        CoachSubmode::RegionalCoach => Ok(TrainType::RegionalCoach),
                        CoachSubmode::SpecialCoach => Ok(TrainType::SpecialCoach),
                        CoachSubmode::SchoolCoach => Ok(TrainType::SchoolCoach),
                        CoachSubmode::SightseeingCoach => Ok(TrainType::SightseeingCoach),
                        CoachSubmode::TouristCoach => Ok(TrainType::TouristCoach),
                        CoachSubmode::CommuterCoach => Ok(TrainType::CommuterCoach),
                    },
                    None => Ok(TrainType::Coach),
                }
            },
            AllPublicTransportModes::Rail => {
                self.assert_submodes_not_present(false, true, transport_submode)?;
                match &transport_submode.rail_submode {
                    Some(rail_submode) => match rail_submode {
                        RailSubmode::Unknown => Ok(TrainType::Passenger),
                        RailSubmode::Local => Ok(TrainType::LocalPassenger),
                        RailSubmode::HighSpeedRail => Ok(TrainType::HighSpeedPassenger),
                        RailSubmode::SuburbanRailway => Ok(TrainType::SuburbanPassenger),
                        RailSubmode::RegionalRail => Ok(TrainType::RegionalPassenger),
                        RailSubmode::InterregionalRail => Ok(TrainType::InterregionalPassenger),
                        RailSubmode::LongDistance => Ok(TrainType::LongDistancePassenger),
                        RailSubmode::International => Ok(TrainType::InternationalPassenger),
                        RailSubmode::SleeperRailService => Ok(TrainType::SleeperPassenger),
                        RailSubmode::NightRail => Ok(TrainType::NightPassenger),
                        RailSubmode::CarTransportRailService => Ok(TrainType::CarCarryingPassenger),
                        RailSubmode::LargeVehicleTransportRailService => Ok(
                            TrainType::LorryCarryingPassenger
                        ),
                        RailSubmode::TouristRailway => Ok(TrainType::TouristPassenger),
                        RailSubmode::AirportLinkRail => Ok(TrainType::AirportLinkPassenger),
                        RailSubmode::RailShuttle => Ok(TrainType::ShuttlePassenger),
                        RailSubmode::ReplacementRailService => Ok(TrainType::ReplacementPassenger),
                        RailSubmode::SpecialTrain => Ok(TrainType::SpecialPassenger),
                        RailSubmode::CrossCountryRail => Ok(TrainType::CrossCountryPassenger),
                        RailSubmode::RackAndPinionRailway => Ok(TrainType::RackAndPinionPassenger),
                    },
                    None => Ok(TrainType::Passenger),
                }
            },
            AllPublicTransportModes::IntercityRail => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::IntercityPassenger)
            },
            AllPublicTransportModes::UrbanRail => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::UrbanPassenger)
            },
            AllPublicTransportModes::Metro => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::Metro)
            },
            AllPublicTransportModes::Air => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::Air)
            },
            AllPublicTransportModes::Water => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::Water)
            },
            AllPublicTransportModes::Cableway => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::CableCar)
            },
            AllPublicTransportModes::Funicular => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::Funicular)
            },
            AllPublicTransportModes::SnowAndIce => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::SnowAndIce)
            },
            AllPublicTransportModes::Taxi => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::Taxi)
            },
            AllPublicTransportModes::Ferry => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::Ship)
            },
            AllPublicTransportModes::Lift => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::Lift)
            },
            AllPublicTransportModes::SelfDrive => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::SelfDrive)
            },
            AllPublicTransportModes::AnyMode => {
                self.assert_submodes_not_present(true, true, transport_submode)?;
                Ok(TrainType::IntercityPassenger)
            },
            AllPublicTransportModes::Other => return Err(
                NetexError {
                    error_type: NetexErrorType::UnexpectedTransportMode(
                        AllPublicTransportModes::All
                    )
                }
            ),
        }
    }

    fn get_accommodation(
        &self, service_facility_set: &ServiceFacilitySet
    ) -> Result<Vec<accommodation_types::ActiveModelEx>, NetexError> {
        // TODO can this be written in a more match-y way for completeness checks?
        // Sometimes these appear to be in some sort of order, but I don't think this is officially
        // defined so let's just be vague about it — I suspect the intent is to have multiple
        // ServiceFacilitySets but SNCF don't seem to do this
        let accommodation_facility_list = &service_facility_set.accommodation_facility_list;
        let populated_accommodation_types = accommodation_types::ActiveModelEx {
            standing: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::Standing
                )
            )),
            seating: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::Seating
                )
            )),
            reclining_seating: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::RecliningSeats
                )
            )),
            special_seating: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::SpecialSeating
                )
            )),
            sleeper: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::Sleeper
                )
            )),
            single_sleeper: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::SingleSleeper
                )
            )),
            double_sleeper: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::DoubleSleeper
                )
            )),
            special_sleeper: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::SpecialSleeper
                )
            )),
            couchette: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::Couchette
                )
            )),
            single_couchette: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::SingleCouchette
                )
            )),
            double_couchette: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::DoubleCouchette
                )
            )),
            baby: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::BabyCompartment
                )
            )),
            family: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::FamilyCarriage
                )
            )),
            recreation: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::RecreationArea
                )
            )),
            panoramic: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::PanoramaCoach
                )
            )),
            pullman: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::PullmanCoach
                )
            )),
            pushchair: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::Pushchair
                )
            )),
            wheelchair: ActiveValue::Set(accommodation_facility_list.as_ref().map(
                |accommodation_facility_list| accommodation_facility_list.text.contains(
                    &AccommodationFacility::Wheelchair
                )
            )),
            has_male_only: ActiveValue::Set(match &service_facility_set.gender_limitation {
                Some(gender_limitation) => Some(*gender_limitation == GenderLimitation::MaleOnly
                    || *gender_limitation == GenderLimitation::Both),
                None => None,
            }),
            has_female_only: ActiveValue::Set(match &service_facility_set.gender_limitation {
                Some(gender_limitation) => Some(*gender_limitation == GenderLimitation::FemaleOnly
                    || *gender_limitation == GenderLimitation::Both),
                None => None,
            }),
            has_same_sex_only: ActiveValue::Set(match &service_facility_set.gender_limitation {
                Some(gender_limitation) =>
                    Some(*gender_limitation == GenderLimitation::SameSexOnly),
                None => None,
            }),
            ..Default::default()
        };
        let mut result = vec![];
        match &service_facility_set.fare_classes {
            Some(fare_classes) => {
                if fare_classes.text.contains(&FareClass::Unknown)
                    || fare_classes.text.is_empty() {
                    let mut accommodation = populated_accommodation_types.clone();
                    accommodation.class = ActiveValue::Set(AccommodationClass::Unknown);
                    result.push(accommodation);
                }
                if fare_classes.text.contains(&FareClass::BusinessClass) {
                    let mut accommodation = populated_accommodation_types.clone();
                    accommodation.class = ActiveValue::Set(AccommodationClass::FirstPremium);
                    result.push(accommodation);
                }
                if fare_classes.text.contains(&FareClass::FirstClass)
                    || fare_classes.text.contains(&FareClass::Preferente) {
                    let mut accommodation = populated_accommodation_types.clone();
                    accommodation.class = ActiveValue::Set(AccommodationClass::First);
                    result.push(accommodation);
                }
                if fare_classes.text.contains(&FareClass::PremiumClass) {
                    let mut accommodation = populated_accommodation_types.clone();
                    accommodation.class = ActiveValue::Set(AccommodationClass::SecondPremium);
                    result.push(accommodation);
                }
                if fare_classes.text.contains(&FareClass::SecondClass)
                    || fare_classes.text.contains(&FareClass::StandardClass)
                    || fare_classes.text.contains(&FareClass::EconomyClass)
                    || fare_classes.text.contains(&FareClass::Turista) {
                    let mut accommodation = populated_accommodation_types.clone();
                    accommodation.class = ActiveValue::Set(AccommodationClass::Second);
                    result.push(accommodation);
                }
                if fare_classes.text.contains(&FareClass::ThirdClass) {
                    let mut accommodation = populated_accommodation_types.clone();
                    accommodation.class = ActiveValue::Set(AccommodationClass::Third);
                    result.push(accommodation);
                }
                if fare_classes.text.contains(&FareClass::Any) {
                    let mut accommodation = populated_accommodation_types.clone();
                    accommodation.class = ActiveValue::Set(AccommodationClass::Unclassified);
                    result.push(accommodation);
                }
            },
            None => {
                let mut accommodation = populated_accommodation_types.clone();
                accommodation.class = ActiveValue::Set(AccommodationClass::Unknown);
                result.push(accommodation);
            },
        };
        Ok(result)
    }

    fn get_reservations(
        &self, service_facility_set: &ServiceFacilitySet
    ) -> Result<Option<variable_train::Reservations>, NetexError> {
        let accommodation_facility_list = match &service_facility_set.accommodation_facility_list {
            Some(accommodation_facility_list) => accommodation_facility_list.text.clone(),
            None => vec![],
        };
        let fare_classes = match &service_facility_set.fare_classes {
            Some(fare_classes) => fare_classes.text.clone(),
            None => vec![],
        };
        match &service_facility_set.service_reservation_facility_list {
            Some(service_reservation_facility_list)
                => return Ok(Some(variable_train::Reservations {
                seats: if accommodation_facility_list.contains(
                        &AccommodationFacility::Seating
                    ) {
                        if service_reservation_facility_list.text.contains(
                            &Reservation::ReservationsCompulsory
                        ) {
                            ReservationField::Mandatory
                        } else if service_reservation_facility_list.text.contains(
                            &Reservation::ReservationsCompulsoryFromOriginStation
                        ) {
                            ReservationField::MandatoryFromOrigin
                        } else if service_reservation_facility_list.text.contains(
                            &Reservation::ReservationsRecommended
                        ) {
                            ReservationField::Recommended
                        } else if service_reservation_facility_list.text.contains(
                            &Reservation::ReservationsRecommended
                        ) {
                            ReservationField::Recommended
                        } else if service_reservation_facility_list.text.contains(
                            &Reservation::ReservationsPossible
                        ) {
                            ReservationField::Possible
                        } else if service_reservation_facility_list.text.contains(
                            &Reservation::NoReservationsPossible
                        ) {
                            ReservationField::Impossible
                        } else {
                            ReservationField::Unknown
                        }
                    } else {
                        ReservationField::NotApplicable
                    },
                groups: if service_reservation_facility_list.text.contains(
                    &Reservation::ReservationsCompulsoryForGroups
                ) || service_facility_set.group_booking_facility.clone().unwrap_or(
                    GroupBooking::Unknown
                ) == GroupBooking::GroupsAllowedWithReservation
                {
                    ReservationField::Mandatory
                } else if service_reservation_facility_list.text.contains(
                    &Reservation::GroupBookingRestricted
                ) || service_facility_set.group_booking_facility.clone().unwrap_or(
                    GroupBooking::Unknown
                ) == GroupBooking::GroupBookingsRestricted
                {
                    ReservationField::Restricted
                } else if service_reservation_facility_list.text.contains(
                    &Reservation::NoGroupsAllowed
                ) || service_facility_set.group_booking_facility.clone().unwrap_or(
                    GroupBooking::Unknown
                ) == GroupBooking::GroupsNotAllowed
                {
                    ReservationField::NotAllowed
                } else if service_facility_set.group_booking_facility.clone().unwrap_or(
                    GroupBooking::Unknown
                ) == GroupBooking::GroupsAllowed
                {
                    ReservationField::NotMandatory
                } else {
                    ReservationField::Unknown
                },
                first_class: if fare_classes.contains(
                    &FareClass::FirstClass
                ) {
                    if service_reservation_facility_list.text.contains(
                        &Reservation::ReservationsCompulsoryForFirstClass
                    ) {
                        ReservationField::Mandatory
                    } else if service_reservation_facility_list.text.contains(
                        &Reservation::ReservationsPossibleOnlyInFirstClass
                    ) {
                        ReservationField::Possible
                    } else if service_reservation_facility_list.text.contains(
                        &Reservation::ReservationsPossibleOnlyInSecondClass
                    ) {
                        ReservationField::Impossible
                    } else {
                        ReservationField::NotApplicable
                    }
                } else {
                    ReservationField::NotApplicable
                },
                second_class: if fare_classes.contains(
                    &FareClass::SecondClass
                ) {
                    if service_reservation_facility_list.text.contains(
                        &Reservation::ReservationsCompulsoryForFirstClass
                    ) {
                        ReservationField::NotMandatory
                    } else if service_reservation_facility_list.text.contains(
                        &Reservation::ReservationsPossibleOnlyInSecondClass
                    ) {
                        ReservationField::Possible
                    } else if service_reservation_facility_list.text.contains(
                        &Reservation::ReservationsPossibleOnlyInFirstClass
                    ) {
                        ReservationField::Impossible
                    } else {
                        ReservationField::NotApplicable
                    }
                } else {
                    ReservationField::NotApplicable
                },
                not_every_class:
                    if service_reservation_facility_list.text.contains(
                        &Reservation::ReservationsPossibleForCertainClasses
                    ) {
                        ReservationField::Possible
                    } else {
                        ReservationField::NotApplicable
                    },
                bicycles: if service_reservation_facility_list.text.contains(
                    &Reservation::BicycleReservationsCompulsory
                ) || service_facility_set.luggage_carriage_facility_list.clone().unwrap_or(
                    LuggageCarriageList { text: vec![] }
                ).text.contains(
                    &LuggageCarriage::CyclesAllowedWithReservation
                ) {
                    ReservationField::Mandatory
                } else {
                    ReservationField::NotMandatory
                },
                sleepers:
                    if accommodation_facility_list.contains(
                        &AccommodationFacility::Sleeper
                    ) {
                        if service_reservation_facility_list.text.contains(
                            &Reservation::ReservationsCompulsory
                        ) {
                            ReservationField::Mandatory
                        } else if service_reservation_facility_list.text.contains(
                            &Reservation::ReservationsCompulsoryFromOriginStation
                        ) {
                            ReservationField::MandatoryFromOrigin
                        } else if service_reservation_facility_list.text.contains(
                            &Reservation::ReservationsRecommended
                        ) {
                            ReservationField::Recommended
                        } else if service_reservation_facility_list.text.contains(
                            &Reservation::ReservationsRecommended
                        ) {
                            ReservationField::Recommended
                        } else if service_reservation_facility_list.text.contains(
                            &Reservation::ReservationsPossible
                        ) {
                            ReservationField::Possible
                        } else if service_reservation_facility_list.text.contains(
                            &Reservation::NoReservationsPossible
                        ) {
                            ReservationField::Impossible
                        } else {
                            ReservationField::Unknown
                        }
                    } else {
                        ReservationField::NotApplicable
                    },
                vehicles: ReservationField::Unknown,
                wheelchairs: if service_reservation_facility_list.text.contains(
                    &Reservation::WheelchairOnlyReservations
                ) {
                    ReservationField::Possible
                } else {
                    ReservationField::Impossible
                },
                supplement_charged: Some(
                    service_reservation_facility_list.text.contains(
                        &Reservation::ReservationsSupplementCharged
                    )
                ),
            })),
            None => return Ok(None),
        };
    }

    fn get_catering(
        &self, service_facility_set: &ServiceFacilitySet
    ) -> Result<Option<variable_train::Catering>, NetexError> {
        match &service_facility_set.catering_facility_list {
            Some(catering_facility_list) => Ok(Some(variable_train::Catering {
                at_seat_meal: catering_facility_list.text.contains(&CateringFacility::MealAtSeat),
                bar: catering_facility_list.text.contains(&CateringFacility::Bar),
                bistro: catering_facility_list.text.contains(&CateringFacility::Bistro),
                breakfast_in_car:
                    catering_facility_list.text.contains(&CateringFacility::BreakfastInCar),
                buffet: catering_facility_list.text.contains(&CateringFacility::Buffet),
                coffee_shop: catering_facility_list.text.contains(&CateringFacility::CoffeeShop),
                self_service: catering_facility_list.text.contains(&CateringFacility::SelfService),
                trolley: catering_facility_list.text.contains(&CateringFacility::Trolley),
                vending_machine_food:
                    catering_facility_list.text.contains(&CateringFacility::FoodVendingMachine),
                vending_machine_drink: catering_facility_list.text.contains(
                    &CateringFacility::BeverageVendingMachine
                ),
                mini_bar: catering_facility_list.text.contains(&CateringFacility::MiniBar),
                restaurant: catering_facility_list.text.contains(&CateringFacility::Restaurant),
                first_class_restaurant:
                    catering_facility_list.text.contains(&CateringFacility::FirstClassRestaurant),
                first_class_meal: false,
                other: catering_facility_list.text.contains(&CateringFacility::Other),
                food_available: if catering_facility_list.text.contains(
                    &CateringFacility::NoFoodAvailable
                ) {
                    Some(false)
                } else {
                    None
                },
                hot_food_available: if catering_facility_list.text.contains(
                    &CateringFacility::HotFoodService
                ) {
                    Some(true)
                } else {
                    None
                },
                drink_available: if catering_facility_list.text.contains(
                    &CateringFacility::NoBeveragesAvailable
                ) {
                    Some(false)
                } else {
                    None
                },
                snacks_available: if catering_facility_list.text.contains(
                    &CateringFacility::Snacks
                ) {
                    Some(true)
                } else {
                    None
                },
            })),
            None => Ok(None),
        }
    }

    fn get_operator(&self, operator_ref: &str) -> Result<Option<String>, NetexError> {
        if operator_ref == "" {
            Ok(None)
        } else {
            Ok(Some(operator_ref.to_string()))
        }
    }

    fn get_toilets(
        &self, service_facility_set: &ServiceFacilitySet
    ) -> Result<Option<variable_train::Toilets>, NetexError> {
        match &service_facility_set.sanitary_facility_list {
            Some(sanitary_facility_list) => Ok(
                if sanitary_facility_list.text.contains(&SanitaryFacility::SanitaryFacilityNone) {
                    Some(variable_train::Toilets {
                        toilet: Some(false),
                        sink: Some(false),
                        disabled_toilet: Some(false),
                        shower: Some(false),
                        changing: Some(false),
                        baby_changing: Some(false),
                        disabled_baby_changing: Some(false),
                        shoe_shiner: Some(false),
                        other: Some(false),
                    })
                } else {
                    Some(variable_train::Toilets {
                        toilet: Some(
                            sanitary_facility_list.text.contains(&SanitaryFacility::Toilet)
                        ),
                        sink: Some(
                            sanitary_facility_list.text.contains(&SanitaryFacility::Washbasin)
                        ),
                        disabled_toilet: Some(sanitary_facility_list.text.contains(
                            &SanitaryFacility::WheelChairAccessToilet
                        )),
                        shower: Some(
                            sanitary_facility_list.text.contains(&SanitaryFacility::Shower)
                        ),
                        changing: Some(sanitary_facility_list.text.contains(
                            &SanitaryFacility::WashingAndChangeFacilities
                        )),
                        baby_changing: Some(sanitary_facility_list.text.contains(
                            &SanitaryFacility::BabyChange
                        )),
                        disabled_baby_changing: Some(
                            sanitary_facility_list.text.contains(
                                &SanitaryFacility::WheelchairBabyChange
                            )
                        ),
                        shoe_shiner: Some(sanitary_facility_list.text.contains(
                            &SanitaryFacility::ShoeShiner
                        )),
                        other: Some(sanitary_facility_list.text.contains(
                            &SanitaryFacility::Other
                        )),
                    })
                }
            ),
            None => Ok(None),
        }
    }

    fn get_luggage(
        &self, service_facility_set: &ServiceFacilitySet
    ) -> Result<Option<variable_train::Luggage>, NetexError> {
        match &service_facility_set.luggage_carriage_facility_list {
            Some(luggage_carriage_facility_list) => Ok(
                if luggage_carriage_facility_list.text.contains(&LuggageCarriage::Unknown) {
                    None
                } else {
                    let some_baggage_storage = luggage_carriage_facility_list.text.contains(
                        &LuggageCarriage::BaggageStorage
                    ) || luggage_carriage_facility_list.text.contains(
                        &LuggageCarriage::NoBaggageStorage
                    );
                    let some_bicycles = luggage_carriage_facility_list.text.contains(
                        &LuggageCarriage::CyclesAllowed
                    ) || luggage_carriage_facility_list.text.contains(
                        &LuggageCarriage::NoCycles
                    ) || luggage_carriage_facility_list.text.contains(
                        &LuggageCarriage::CyclesAllowedInVan
                    ) || luggage_carriage_facility_list.text.contains(
                        &LuggageCarriage::CyclesAllowedInCarriage
                    ) || luggage_carriage_facility_list.text.contains(
                        &LuggageCarriage::CyclesAllowedWithReservation
                    );
                    Some(variable_train::Luggage {
                        bag_storage: if some_baggage_storage {
                            Some(luggage_carriage_facility_list.text.contains(
                                &LuggageCarriage::BaggageStorage
                            ))
                        } else {
                            None
                        },
                        racks: Some(luggage_carriage_facility_list.text.contains(
                            &LuggageCarriage::LuggageRacks
                        )),
                        skis: Some(luggage_carriage_facility_list.text.contains(
                            &LuggageCarriage::SkiRacks
                        )),
                        skis_on_rear: Some(
                            luggage_carriage_facility_list.text.contains(
                                &LuggageCarriage::SkiRacksOnRear
                            )
                        ),
                        extra_large_racks: Some(
                            luggage_carriage_facility_list.text.contains(
                                &LuggageCarriage::ExtraLargeLuggageRacks
                            )
                        ),
                        van: Some(luggage_carriage_facility_list.text.contains(
                            &LuggageCarriage::BaggageVan
                        )),
                        bicycles: if some_bicycles {
                            Some(!luggage_carriage_facility_list.text.contains(
                                &LuggageCarriage::NoCycles
                            ))
                        } else {
                            None
                        },
                        bicycles_in_van: Some(
                            luggage_carriage_facility_list.text.contains(
                                &LuggageCarriage::CyclesAllowedInVan
                            )
                        ),
                        bicycles_in_carriage: Some(
                            luggage_carriage_facility_list.text.contains(
                                &LuggageCarriage::CyclesAllowedInCarriage
                            )
                        ),
                        pushchairs: Some(
                            luggage_carriage_facility_list.text.contains(
                                &LuggageCarriage::PushchairsAllowed
                            )
                        ),
                        vehicles: Some(
                            luggage_carriage_facility_list.text.contains(
                                &LuggageCarriage::VehicleTransport
                            )
                        ),
                    })
                }
            ),
            None => Ok(None),
        }
    }

    fn get_families(
        &self, service_facility_set: &ServiceFacilitySet
    ) -> Result<Option<variable_train::Families>, NetexError> {
        match &service_facility_set.family_facility_list {
            Some(family_facility_list) => Ok(
                Some(variable_train::Families {
                    children_facilities: Some(
                        *family_facility_list == FamilyFacility::ServicesForChildren
                    ),
                    military_family_facilities: Some(
                        *family_facility_list == FamilyFacility::ServicesForArmyFamilies
                    ),
                    nursery: Some(
                        *family_facility_list == FamilyFacility::NurseryService
                    ),
                })
            ),
            None => Ok(None),
        }
    }

    fn get_passenger_communications(
        &self, service_facility_set: &ServiceFacilitySet
    ) -> Result<Option<variable_train::PassengerCommunications>, NetexError> {
        match &service_facility_set.passenger_comms_facility_list {
            Some(passenger_comms_facility_list) => Ok(
                Some(variable_train::PassengerCommunications {
                    free_wifi: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::FreeWifi
                    )),
                    wifi: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::PublicWifi
                    )),
                    mains_sockets: Some(
                        passenger_comms_facility_list.text.contains(
                            &PassengerCommsFacility::PowerSupplySockets
                        )
                    ),
                    telephone: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::Telephone
                    )),
                    radio: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::AudioEntertainment
                    )),
                    video: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::VideoEntertainment
                    )),
                    business: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::BusinessServices
                    )),
                    internet: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::Internet
                    )),
                    post_office: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::PostOffice
                    )),
                    postbox: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::PostBox
                    )),
                    usb_a: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::UsbAPowerSocket
                    )),
                    usb_c: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::UsbCPowerSocket
                    )),
                    other: Some(passenger_comms_facility_list.text.contains(
                        &PassengerCommsFacility::Other
                    )),
                })
            ),
            None => Ok(None),
        }
    }

    fn get_assistance(
        &self, service_facility_set: &ServiceFacilitySet
    ) -> Result<Option<variable_train::Assistance>, NetexError> {
        let assistance_facility_merged = match &service_facility_set.assistance_facility_list {
            Some(assistance_facility_list) => Some(assistance_facility_list.text.clone()),
            None => match &service_facility_set.assistance_facility_enumeration {
                Some(assistance_facility_enumeration) =>
                    Some(vec![assistance_facility_enumeration.clone()]),
                None => None,
            }
        };
        match &assistance_facility_merged {
            Some(assistance_facility_list) => Ok(
                Some(variable_train::Assistance {
                    personal: Some(assistance_facility_list.contains(
                        &AssistanceFacility::PersonalAssistance
                    )
                    || service_facility_set.assistance_facility_enumeration == Some(
                        AssistanceFacility::PersonalAssistance
                    )),
                    boarding: Some(assistance_facility_list.contains(
                        &AssistanceFacility::BoardingAssistance
                    )
                    || service_facility_set.assistance_facility_enumeration == Some(
                        AssistanceFacility::BoardingAssistance
                    )),
                    wheelchair: Some(assistance_facility_list.contains(
                        &AssistanceFacility::WheelchairAssistance
                    )
                    || service_facility_set.assistance_facility_enumeration == Some(
                        AssistanceFacility::WheelchairAssistance
                    )),
                    unaccompanied_minor: Some(assistance_facility_list.contains(
                        &AssistanceFacility::UnaccompaniedMinorAssistance
                    )
                    || service_facility_set.assistance_facility_enumeration == Some(
                        AssistanceFacility::UnaccompaniedMinorAssistance
                    )),
                    use_of_wheelchair: Some(assistance_facility_list.contains(
                        &AssistanceFacility::WheelchairUse
                    )
                    || service_facility_set.assistance_facility_enumeration == Some(
                        AssistanceFacility::WheelchairUse
                    )),
                    guard: Some(assistance_facility_list.contains(
                        &AssistanceFacility::Conductor
                    )
                    || service_facility_set.assistance_facility_enumeration == Some(
                        AssistanceFacility::Conductor
                    )),
                    information: Some(assistance_facility_list.contains(
                        &AssistanceFacility::Information
                    )
                    || service_facility_set.assistance_facility_enumeration == Some(
                        AssistanceFacility::Information
                    )),
                    other: Some(assistance_facility_list.contains(
                        &AssistanceFacility::Other
                    )
                    || service_facility_set.assistance_facility_enumeration == Some(
                        AssistanceFacility::Other
                    )),
                })
            ),
            None => Ok(None),
        }
    }

    fn get_passenger_information(
        &self, service_facility_set: &ServiceFacilitySet
    ) -> Result<Option<variable_train::PassengerInformation>, NetexError> {
        if service_facility_set.passenger_information_facility_enumeration.is_none()
            && service_facility_set.accessibility_info_facility_enumeration.is_none() {
            return Ok(None);
        }
        let mut passenger_info = variable_train::PassengerInformation {
            next_stop_indication: Some(false),
            stop_announcements: Some(false),
            information_display: Some(false),
            realtime_connections: Some(false),
            audible_information: Some(false),
            hearing_impaired_audible_information: Some(false),
            visible_information: Some(false),
            visually_impaired_visible_information: Some(false),
            large_print_timetable: Some(false),
            other: Some(false),
        };
        match &service_facility_set.passenger_information_facility_enumeration {
            Some(passenger_information_facility_enumeration) =>
                match passenger_information_facility_enumeration {
                    PassengerInformationFacility::NextStopIndicator => {
                        passenger_info.next_stop_indication = Some(true);
                    },
                    PassengerInformationFacility::StopAnnouncements => {
                        passenger_info.stop_announcements = Some(true);
                    },
                    PassengerInformationFacility::PassengerInformationDisplay => {
                        passenger_info.information_display = Some(true);
                    },
                    PassengerInformationFacility::RealTimeConnections => {
                        passenger_info.realtime_connections = Some(true);
                    },
                    PassengerInformationFacility::Other => {
                        passenger_info.other = Some(true);
                    },
                },
            None => (),
        }
        match &service_facility_set.accessibility_info_facility_enumeration {
            Some(accessibility_info_facility_enumeration) =>
                match accessibility_info_facility_enumeration {
                    AccessibilityInfoFacility::AudioInformation => {
                        passenger_info.audible_information = Some(true);
                    },
                    AccessibilityInfoFacility::AudioForHearingImpaired => {
                        passenger_info.hearing_impaired_audible_information = Some(true);
                    },
                    AccessibilityInfoFacility::VisualDisplays => {
                        passenger_info.visible_information = Some(true);
                    },
                    AccessibilityInfoFacility::DisplaysForVisuallyImpaired => {
                        passenger_info.visually_impaired_visible_information = Some(true);
                    },
                    AccessibilityInfoFacility::LargePrintTimetables => {
                        passenger_info.large_print_timetable = Some(true);
                    },
                    AccessibilityInfoFacility::Other => {
                        passenger_info.other = Some(true);
                    },
                },
            None => (),
        }

        Ok(Some(passenger_info))
    }

    fn get_headcode(
        &self, service_journey_pattern: &ServiceJourneyPattern
    ) -> Result<Option<String>, NetexError> {

        match self.destination_display_by_id.get(
            &service_journey_pattern.destination_display_ref.destination_display_ref_ref
        ) {
            Some(destination_display) => Ok(Some(destination_display.front_text.clone())),
            // SNCF data has empty string destination displays sometimes
            None =>
                if service_journey_pattern.destination_display_ref.destination_display_ref_ref == ""
            {
                Ok(None)
            } else {
                return Err(
                    NetexError {
                        error_type: NetexErrorType::DestinationDisplayNotFound(
                            service_journey_pattern
                            .destination_display_ref
                            .destination_display_ref_ref
                            .clone()
                        )
                    }
                )
            },
        }
    }

    fn get_route(
        &self,
        service_journey_pattern: &ServiceJourneyPattern,
        timetabled_passing_times: &Vec<TimetabledPassingTime>,
        validity: &Vec<train_validity_period::ActiveModelEx>,
        train_id: &str,
        namespace: &str,
    ) -> Result<Vec<train_location::ActiveModelEx>, NetexError> {
        // OK, so, we use the `service_journey_pattern` to figure out the stopping points, and we
        // use `timetabled_passing_times` to figure out the times of those stopping points.
        let mut train_locations = vec![];
        let mut stop_point_in_journey_pattern_by_id = HashMap::new();
        for stop_point_in_journey_pattern
            in &service_journey_pattern.points_in_sequence.stop_point_in_journey_pattern {
            stop_point_in_journey_pattern_by_id.insert(
                stop_point_in_journey_pattern.id.clone(),
                stop_point_in_journey_pattern.clone(),
            );
        }
        for (i, timetabled_passing_time) in timetabled_passing_times.iter().enumerate() {
            let stop_point_in_journey_pattern = match stop_point_in_journey_pattern_by_id.get(
                &timetabled_passing_time
                .point_in_journey_pattern_ref
                .point_in_journey_pattern_ref_ref
            ) {
                Some(stop_point_in_journey_pattern) => stop_point_in_journey_pattern,
                None => return Err(
                    NetexError {
                        error_type: NetexErrorType::StopPointInJourneyPatternNotFound(
                            timetabled_passing_time
                            .point_in_journey_pattern_ref
                            .point_in_journey_pattern_ref_ref
                            .clone()
                        )
                    }
                ),
            };
            let mut associations = vec![];
            // OK so now we have everything, we are nearly ready to produce the train location.
            // However, now we have coupling to worry about. Coupling data is suuuper weird and I
            // might have got this wrong but I reckon you basically have to match by hand.
            for maybe_coupled_train
                in self.coupled_train_ids_by_train_id.get(train_id).unwrap_or(&HashSet::new()) {
                // TODO figure out if validities are compatible and skip if not
                let mut association_node = association_node::ActiveModelEx {
                    other_train_id: ActiveValue::Set(maybe_coupled_train.clone()),
                    // TODO as elsewhere, departure/arrival time
                    other_train_location_id_suffix: ActiveValue::Set(None), 
                    namespace: ActiveValue::Set(namespace.to_string()),
                    // No specific separate validity for joins in NeTEx, TODO could eventually
                    // intersection the two in a two-pass solution
                    validity: ActiveHasMany::Append(validity.clone()),
                    // day_diff is set later
                    for_passengers: ActiveValue::Set(true),
                    // Unlike trains there's nothing to say where a coupling came from
                    source: ActiveValue::Set(None),
                    ..Default::default()
                };
                for journey_part_couple_id in
                    self.journey_part_couple_ids_by_train_id
                        .get(train_id)
                        .unwrap_or(&HashSet::new())
                        .iter() {
                    let journey_part_couple =
                        match self.journey_part_couple_by_id.get(journey_part_couple_id) {
                        Some(journey_part_couple) => journey_part_couple,
                        None => return Err(
                            NetexError {
                                error_type: NetexErrorType::JourneyPartCoupleNotFound(
                                    journey_part_couple_id.clone()
                                )
                            }
                        ),
                    };

                    // Get the main part
                    let main_journey_part = match self.journey_part_by_id.get(
                        &journey_part_couple.main_part_ref.main_part_ref_ref
                    ) {
                        Some(journey_part) => journey_part,
                        None => return Err(
                            NetexError {
                                error_type: NetexErrorType::JourneyPartNotFound(
                                    journey_part_couple.main_part_ref.main_part_ref_ref.clone()
                                )
                            }
                        ),
                    };
                    // We need to find the other train via the JourneyPartCouple to figure out the
                    // two train number refs involved in this. Don't ask me why this is how it's
                    // keyed, but it is. This is also needed to calculate the day offset in the
                    // event that we are the main train.
                    for journey_part_ref
                        in &journey_part_couple.journey_parts.journey_part_ref {
                        if journey_part_ref.journey_part_ref_ref != main_journey_part.id {
                            let other_journey_part = match self.journey_part_by_id.get(
                                &journey_part_ref.journey_part_ref_ref
                            ) {
                                Some(journey_part) => journey_part,
                                None => return Err(
                                    NetexError {
                                        error_type: NetexErrorType::JourneyPartNotFound(
                                            journey_part_ref.journey_part_ref_ref.clone()
                                        )
                                    }
                                ),
                            };

                            // Continue inside this loop although I'm not sure if it can ever have
                            // more than two entries.
                            if self.service_journey_ids_by_train_number_id.get(
                                &main_journey_part.train_number_ref.train_number_ref_ref
                            ).unwrap_or(&HashSet::new()).contains(train_id) {
                                // We are the main part — sanity check that the parent journey ref
                                // also matches
                                if main_journey_part.parent_journey_ref.parent_journey_ref_ref
                                    != train_id {
                                    continue;
                                };

                                // Now we check the other part matches the train we are searching
                                if !self.service_journey_ids_by_train_number_id.get(
                                    &other_journey_part.train_number_ref.train_number_ref_ref
                                ).unwrap_or(&HashSet::new()).contains(maybe_coupled_train) {
                                    continue;
                                }

                                // OK, now we know:
                                // 1) We are the main journey part
                                // 2) The other train we are searching matches up
                                // We are now ready to associate the two trains.

                                // We use the backwards forms of the association if _we_ are the
                                // main part.

                                // You might naively think we could prevent it showing spurious
                                // "joins to" and "divides from" messages at the start/end of
                                // journeys respectively. Unfortunately if we want to indicate
                                // trains that detach and terminate (for instance), we can't, as we
                                // don't know the other train's stopping pattern at this point. So
                                // just let those messages happen.
                                if journey_part_couple.from_stop_point_ref.from_stop_point_ref_ref
                                    == stop_point_in_journey_pattern
                                        .scheduled_stop_point_ref
                                        .scheduled_stop_point_ref_ref {
                                    // If only WE have a positive value here, that means that OUR
                                    // train started the PREVIOUS day, so the day diff will be
                                    // POSITIVE to get to the NEXT day that the associated part will
                                    // be running on. If only the OTHER TRAIN has a positive value
                                    // here, that means that THE OTHER TRAIN started on the PREVIOUS
                                    // day, so the day diff will be NEGATIVE to get to the PREVIOUS
                                    // day that the associated part will be running on.
                                    association_node.day_diff = ActiveValue::Set(
                                        i8::try_from(
                                            main_journey_part.start_time_day_offset.unwrap_or(0)
                                        ).unwrap()
                                        - i8::try_from(
                                            other_journey_part.start_time_day_offset.unwrap_or(0)
                                        ).unwrap()
                                    );
                                    association_node.association_type = ActiveValue::Set(
                                        AssociationType::MainJoinsToOther
                                    );

                                    associations.push(association_node.clone());
                                } else
                                    if journey_part_couple.to_stop_point_ref.to_stop_point_ref_ref
                                    == stop_point_in_journey_pattern
                                        .scheduled_stop_point_ref
                                        .scheduled_stop_point_ref_ref {
                                    association_node.day_diff = ActiveValue::Set(
                                        i8::try_from(
                                            main_journey_part.end_time_day_offset.unwrap_or(0)
                                        ).unwrap()
                                        - i8::try_from(
                                            other_journey_part.end_time_day_offset.unwrap_or(0)
                                        ).unwrap()
                                    );
                                    association_node.association_type = ActiveValue::Set(
                                        AssociationType::MainDividesToFormOther
                                    );

                                    associations.push(association_node.clone());
                                }
                            } else if self.service_journey_ids_by_train_number_id.get(
                                &other_journey_part.train_number_ref.train_number_ref_ref
                            ).unwrap_or(&HashSet::new()).contains(train_id) {
                                // We are the other part — now we check the main part matches the
                                // train we are searching for
                                if !self.service_journey_ids_by_train_number_id.get(
                                    &main_journey_part.train_number_ref.train_number_ref_ref
                                ).unwrap_or(&HashSet::new()).contains(maybe_coupled_train) {
                                    continue;
                                }

                                // Sanity check that the main journey part's parent journey ref also
                                // matches the train we are searching for
                                if main_journey_part.parent_journey_ref.parent_journey_ref_ref
                                    != *maybe_coupled_train {
                                    continue;
                                };

                                // OK, now we know:
                                // 1) We are the other journey part
                                // 2) The other train we are searching matches up
                                // We are now ready to associate the two trains.

                                // If we are the other part, we use the forwards forms of the
                                // association.

                                // You might naively think we could prevent it showing spurious
                                // "joins to" and "divides from" messages at the start/end of
                                // journeys respectively. Unfortunately if we want to indicate
                                // trains that detach and terminate (for instance), we can't, as we
                                // don't know the other train's stopping pattern at this point. So
                                // just let those messages happen.
                                if journey_part_couple.from_stop_point_ref.from_stop_point_ref_ref
                                    == stop_point_in_journey_pattern
                                        .scheduled_stop_point_ref
                                        .scheduled_stop_point_ref_ref {
                                    // If only WE have a positive value here, that means that OUR
                                    // train started the PREVIOUS day, so the day diff will be
                                    // POSITIVE to get to the NEXT day that the associated part will
                                    // be running on. If only the OTHER TRAIN has a positive value
                                    // here, that means that THE OTHER TRAIN started on the PREVIOUS
                                    // day, so the day diff will be NEGATIVE to get to the PREVIOUS
                                    // day that the associated part will be running on.
                                    association_node.day_diff = ActiveValue::Set(
                                        i8::try_from(
                                            other_journey_part.start_time_day_offset.unwrap_or(0)
                                        ).unwrap()
                                        - i8::try_from(
                                            main_journey_part.start_time_day_offset.unwrap_or(0)
                                        ).unwrap()
                                    );
                                    association_node.association_type = ActiveValue::Set(
                                        AssociationType::MainIsJoinedToByOther
                                    );

                                    associations.push(association_node.clone());
                                } else
                                    if journey_part_couple.to_stop_point_ref.to_stop_point_ref_ref
                                    == stop_point_in_journey_pattern
                                        .scheduled_stop_point_ref
                                        .scheduled_stop_point_ref_ref {
                                    association_node.day_diff = ActiveValue::Set(
                                        i8::try_from(
                                            other_journey_part.end_time_day_offset.unwrap_or(0)
                                        ).unwrap()
                                        - i8::try_from(
                                            main_journey_part.end_time_day_offset.unwrap_or(0)
                                        ).unwrap()
                                    );
                                    association_node.association_type = ActiveValue::Set(
                                        AssociationType::MainDividesFromOther
                                    );

                                    associations.push(association_node.clone());
                                }
                            }
                        }
                    }
                }
            }
            let train_location = train_location::ActiveModelEx {
                timing_tz: ActiveValue::Set(None),
                location_id: ActiveValue::Set(stop_point_in_journey_pattern
                    .scheduled_stop_point_ref
                    .scheduled_stop_point_ref_ref
                    .clone()),
                // The actual keying here is off departure/arrival time, TODO split this key into
                // two to allow either one to be used as appropriate.
                id_suffix: ActiveValue::Set(None), 
                namespace: ActiveValue::Set(namespace.to_string()),
                index: ActiveValue::Set(i.try_into().unwrap()),
                working_arr: ActiveValue::Set(None),
                working_arr_day: ActiveValue::Set(None),
                working_dep: ActiveValue::Set(None),
                working_dep_day: ActiveValue::Set(None),
                working_pass: ActiveValue::Set(None),
                working_pass_day: ActiveValue::Set(None),
                public_arr: ActiveValue::Set(timetabled_passing_time.arrival_time.clone()),
                public_arr_day: ActiveValue::Set(match timetabled_passing_time.arrival_day_offset {
                    Some(x) => Some(x),
                    None => match timetabled_passing_time.arrival_time {
                        Some(_) => Some(0),
                        None => None,
                    },
                }),
                public_dep: ActiveValue::Set(timetabled_passing_time.departure_time.clone()),
                public_dep_day:
                    ActiveValue::Set(match timetabled_passing_time.departure_day_offset {
                        Some(x) => Some(x),
                        None => match timetabled_passing_time.departure_time {
                            Some(_) => Some(0),
                            None => None,
                        },
                    }),
                platform: ActiveValue::Set(None),
                platform_zone: ActiveValue::Set(None),
                line: ActiveValue::Set(None),
                path: ActiveValue::Set(None),
                engineering_allowance_s: ActiveValue::Set(None),
                pathing_allowance_s: ActiveValue::Set(None),
                performance_allowance_s: ActiveValue::Set(None),
                set_down_only: ActiveValue::Set(timetabled_passing_time.departure_time.is_none()
                    && timetabled_passing_time.arrival_time.is_some()),
                pick_up_only: ActiveValue::Set(timetabled_passing_time.arrival_time.is_none()
                    && timetabled_passing_time.departure_time.is_some()),
                unadvertised_stop: ActiveValue::Set(
                    timetabled_passing_time.arrival_time.is_none()
                    && timetabled_passing_time.departure_time.is_none()
                ),
                normal_passenger_stop: ActiveValue::Set(
                    timetabled_passing_time.departure_time.is_some()
                    && timetabled_passing_time.arrival_time.is_some()
                ),
                train_begins: ActiveValue::Set(i == 0),
                train_finishes: ActiveValue::Set(i == timetabled_passing_times.len() - 1),
                request_pick_up: ActiveValue::Set(false),
                request_set_down: ActiveValue::Set(false),
                request_pick_up_by_telephone: ActiveValue::Set(false),
                request_set_down_by_telephone: ActiveValue::Set(false),
                times_approximate: ActiveValue::Set(false),
                detach: ActiveValue::Set(false),
                attach: ActiveValue::Set(false),
                other_trains_pass: ActiveValue::Set(false),
                attach_or_detach_assisting_loco: ActiveValue::Set(false),
                x_on_arrival: ActiveValue::Set(false),
                banking_loco: ActiveValue::Set(false),
                crew_change: ActiveValue::Set(false),
                examination: ActiveValue::Set(false),
                gbprtt: ActiveValue::Set(false),
                prevent_column_merge: ActiveValue::Set(false),
                prevent_third_column_merge: ActiveValue::Set(false),
                passenger_count: ActiveValue::Set(false),
                ticket_collection: ActiveValue::Set(false),
                ticket_examination: ActiveValue::Set(false),
                first_class_ticket_examination: ActiveValue::Set(false),
                selective_ticket_examination: ActiveValue::Set(false),
                change_loco: ActiveValue::Set(false),
                operational_stop: ActiveValue::Set(false),
                train_locomotive_on_rear: ActiveValue::Set(false),
                propelling: ActiveValue::Set(false),
                reversing_move: ActiveValue::Set(false),
                run_round: ActiveValue::Set(false),
                staff_stop: ActiveValue::Set(false),
                tops_reporting: ActiveValue::Set(false),
                token_etc: ActiveValue::Set(false),
                watering_stock: ActiveValue::Set(false),
                cross_at_passing_point: ActiveValue::Set(false),
                association_nodes: ActiveHasMany::Append(associations),
                ..Default::default()
            };

            train_locations.push(train_location);
        }
        Ok(train_locations)
    }

    async fn read_service_journey(
        &self,
        service_journey: &ServiceJourney,
        namespace: &str,
        default_timezone: &str,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        let mut operating_periods = vec![];
        let day_type_ref = &service_journey.day_types.day_type_ref.day_type_ref_ref;
        match self.uic_operating_period_ids_by_day_type_id.get(day_type_ref) {
            Some(uic_operating_period_ids) => for id in uic_operating_period_ids {
                match self.uic_operating_period_by_id.get(id) {
                    Some(operating_period) => operating_periods.push(operating_period),
                    None => return Err(
                        NetexError {
                            error_type: NetexErrorType::UicOperatingPeriodNotFound(id.clone())
                        }.into()
                    ),
                }
            },
            None => return Err(
                NetexError {
                    error_type: NetexErrorType::DayTypeAssignmentNotFound(day_type_ref.clone())
                }.into()
            ),
        }
        let validity = self.calculate_validities(
            &service_journey.valid_between, &operating_periods, default_timezone
        )?;
        let train_type = self.get_train_type(
            service_journey.transport_mode.clone(), &service_journey.transport_submode
        )?;
        let train_number = match self.train_number_by_id.get(
            &service_journey.train_numbers.train_number_ref.train_number_ref_ref
        ) {
            Some(train_number) => train_number.for_advertisement.clone(),
            None => return Err(
                NetexError {
                    error_type: NetexErrorType::TrainNumberNotFound(
                        service_journey.train_numbers.train_number_ref.train_number_ref_ref.clone()
                    )
                }.into()
            ),
        };
        let accommodation = match &service_journey.facilities {
            Some(facilities) => self.get_accommodation(
                &facilities.service_facility_set
            )?,
            None => vec![],
        };
        let reservations = match &service_journey.facilities {
            Some(facilities) => self.get_reservations(
                &facilities.service_facility_set
            )?,
            None => None,
        };
        let catering = match &service_journey.facilities {
            Some(facilities) => self.get_catering(
                &facilities.service_facility_set
            )?,
            None => None,
        };
        let line_id = Some(service_journey.line_ref.line_ref_ref.clone());
        let operator_id = self.get_operator(&service_journey.operator_ref.operator_ref_ref)?;
        let toilets = match &service_journey.facilities {
            Some(facilities) => self.get_toilets(
                &facilities.service_facility_set
            )?,
            None => None,
        };
        let luggage = match &service_journey.facilities {
            Some(facilities) => self.get_luggage(
                &facilities.service_facility_set
            )?,
            None => None,
        };
        let families = match &service_journey.facilities {
            Some(facilities) => self.get_families(
                &facilities.service_facility_set
            )?,
            None => None,
        };
        let passenger_communications = match &service_journey.facilities {
            Some(facilities) => self.get_passenger_communications(
                &facilities.service_facility_set
            )?,
            None => None,
        };
        let assistance = match &service_journey.facilities {
            Some(facilities) => self.get_assistance(
                &facilities.service_facility_set
            )?,
            None => None,
        };
        let passenger_information = match &service_journey.facilities {
            Some(facilities) => self.get_passenger_information(
                &facilities.service_facility_set
            )?,
            None => None,
        };
        let service_journey_pattern = match self.service_journey_pattern_by_id.get(
            &service_journey.journey_pattern_ref.journey_pattern_ref_ref
        ) {
            Some(service_journey_pattern) => service_journey_pattern,
            None => return Err(
                NetexError {
                    error_type: NetexErrorType::ServiceJourneyPatternNotFound(
                        service_journey.journey_pattern_ref.journey_pattern_ref_ref.clone()
                    )
                }.into()
            ),
        };
        let headcode = self.get_headcode(service_journey_pattern)?;
        let route = self.get_route(
            service_journey_pattern,
            &service_journey.passing_times.timetabled_passing_time,
            &validity,
            &service_journey.id,
            namespace,
        )?;
        let source = Some(match service_journey.service_alteration {
            ServiceAlteration::Planned => TrainSource::LongTerm,
            ServiceAlteration::ExtraJourney => TrainSource::ShortTerm,
            ServiceAlteration::Provisional => TrainSource::Provisional,
            ServiceAlteration::Cancellation => TrainSource::LongTerm, // The original
            ServiceAlteration::Replaced => TrainSource::LongTerm, // The original
        });
        let mut variable_train = variable_train::ActiveModelEx {
            namespace: ActiveValue::Set(namespace.to_string()),
            train_type: ActiveValue::Set(train_type),
            public_id: ActiveValue::Set(Some(train_number)),
            headcode: ActiveValue::Set(headcode),
            power_type: ActiveValue::Set(None),
            timing_speed_m_per_s: ActiveValue::Set(None),
            accommodation: ActiveHasMany::Append(accommodation),
            brand: ActiveValue::Set(Some(service_journey.branding_ref.branding_ref_ref.clone())),
            name: ActiveValue::Set(None),
            line_id: ActiveValue::Set(line_id),
            // This may or may not be == public_id, but we can't tell
            uic_code: ActiveValue::Set(None),
            operator_id: ActiveValue::Set(operator_id),
            // TODO does this need removing now?
            wheelchair_accessible: ActiveValue::Set(None),
            has_operating_characteristics: ActiveValue::Set(false),
            ..Default::default()
        };
        match &catering {
            Some(x) => variable_train.populate_catering(x),
            None => variable_train.has_catering = ActiveValue::Set(false),
        };
        match &reservations {
            Some(x) => variable_train.populate_reservations(x),
            None => variable_train.has_reservations = ActiveValue::Set(false),
        };
        match &toilets {
            Some(x) => variable_train.populate_toilets(x),
            None => variable_train.has_toilets = ActiveValue::Set(false),
        };
        match &luggage {
            Some(x) => variable_train.populate_luggage(x),
            None => variable_train.has_luggage = ActiveValue::Set(false),
        };
        match &families {
            Some(x) => variable_train.populate_families(x),
            None => variable_train.has_families = ActiveValue::Set(false),
        };
        match &passenger_communications {
            Some(x) => variable_train.populate_passenger_communications(x),
            None => variable_train.has_passenger_communications = ActiveValue::Set(false),
        };
        match &assistance {
            Some(x) => variable_train.populate_assistance(x),
            None => variable_train.has_assistance = ActiveValue::Set(false),
        };
        match &passenger_information {
            Some(x) => variable_train.populate_passenger_information(x),
            None => variable_train.has_passenger_information = ActiveValue::Set(false),
        };
        let cancellations = if service_journey.service_alteration == ServiceAlteration::Cancellation
            || service_journey.service_alteration == ServiceAlteration::Replaced {
            // If a train is cancelled or replaced, it means its whole validity is cancelled. TODO
            // maybe later try to match up with the original train and merge the validity periods
            // and mark that as cancelled instead?
            vec![train_cancellation::ActiveModelEx {
                validity: ActiveHasMany::Append(validity.clone()),
                source: ActiveValue::Set(Some(TrainSource::ShortTerm)),
                ..Default::default()
            }]
        } else {
            vec![]
        };
        let train_variant = train_variant::ActiveModelEx {
            validity: ActiveHasMany::Append(validity),
            cancellations: ActiveHasMany::Append(cancellations),
            // I don't think we can match up replacements with originals easily? So just have them
            // as cancelled instead sadly.
            replacements: ActiveHasMany::Append(vec![]),
            variable_train: ActiveHasOne::Set(Some(Box::new(variable_train))),
            source: ActiveValue::Set(source),
            runs_as_required: ActiveValue::Set(false),
            performance_monitoring: ActiveValue::Set(None),
            route: ActiveHasMany::Append(route),
            ..Default::default()
        };

        let train = train::ActiveModelEx {
            id: ActiveValue::Set(service_journey.id.clone()),
            namespace: ActiveValue::Set(namespace.to_string()),
            train_variants: ActiveHasMany::Append(vec![train_variant]),
            ..Default::default()
        };

        train.insert(transaction).await?;

        Ok(())
    }
}

#[async_trait]
impl SlowStreamingImporter for NetexImporter {
    async fn overlay(
        &mut self,
        mut reader: impl AsyncBufReadExt + Unpin + Send,
        schedule: &schedule::ModelEx,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        // Can't seem to stream this for now
        let mut read_xml = Vec::new();
        reader.read_to_end(&mut read_xml).await?;
        let mut deserializer = de::Deserializer::from_reader(read_xml.as_slice());
        let publication_delivery: PublicationDelivery = serde_path_to_error::deserialize(
            &mut deserializer
        )?;

        let namespace = schedule.namespace.clone();
        self.read_publication_delivery(&publication_delivery, schedule, transaction).await?;

        println!("[{}] Successfully loaded trains from NeTEx", namespace);
        Ok(())
    }
}
