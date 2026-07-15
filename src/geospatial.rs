use bytes::Bytes;
use std::collections::HashMap;
use std::f64::consts::PI;

// Redis-compatible geographic bounds (WGS84 Mercator limits)
const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LONG_MIN: f64 = -180.0;
const GEO_LONG_MAX: f64 = 180.0;

/// Base32 alphabet used by Redis for GEOHASH strings
const GEO_ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// Sort order for geospatial search results
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
    None,
}

/// Options for GEOADD
#[derive(Debug, Default)]
pub struct GeoAddOptions {
    /// Only add new members (skip existing)
    pub nx: bool,
    /// Only update existing members (skip new)
    pub xx: bool,
    /// Return old position before update
    pub get: bool,
    /// Count changed (added + updated) instead of only added
    pub ch: bool,
}

/// Result of a single GEOADD operation for one member
#[derive(Debug)]
pub enum GeoAddResult {
    /// Member was newly added; no previous position
    Added,
    /// Member already existed and was updated; contains old (lon, lat)
    Updated(f64, f64),
    /// Skipped due to NX / XX constraint
    Skipped,
    /// Invalid coordinates
    InvalidCoords,
}

/// A geographic point with longitude and latitude
#[derive(Debug, Clone, PartialEq)]
pub struct GeoPoint {
    pub member: Bytes,
    pub longitude: f64,
    pub latitude: f64,
}

impl GeoPoint {
    /// Create a new geographic point
    pub fn new(member: Bytes, longitude: f64, latitude: f64) -> Option<Self> {
        if !Self::is_valid_coordinates(longitude, latitude) {
            return None;
        }
        Some(Self {
            member,
            longitude,
            latitude,
        })
    }

    /// Validate longitude and latitude
    pub fn is_valid_coordinates(longitude: f64, latitude: f64) -> bool {
        longitude >= GEO_LONG_MIN
            && longitude <= GEO_LONG_MAX
            && latitude >= GEO_LAT_MIN
            && latitude <= GEO_LAT_MAX
    }

    /// Calculate geohash for this point
    pub fn geohash(&self) -> u64 {
        geohash_encode(self.longitude, self.latitude)
    }

    /// Calculate distance to another point in meters using Haversine formula
    pub fn distance_to(&self, other: &GeoPoint) -> f64 {
        haversine_distance(self.longitude, self.latitude, other.longitude, other.latitude)
    }
}

/// Geospatial Set implementation using internal sorted set based on geohash
pub struct GeoSet {
    /// HashMap for storing member -> GeoPoint
    members: HashMap<Bytes, GeoPoint>,
}

impl GeoSet {
    /// Create a new empty geospatial set
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
        }
    }

    /// Add a member with its coordinates
    /// Returns true if the member was newly added, false if updated
    pub fn add(&mut self, member: Bytes, longitude: f64, latitude: f64) -> Option<bool> {
        let point = GeoPoint::new(member.clone(), longitude, latitude)?;
        let is_new = !self.members.contains_key(&member);
        self.members.insert(member, point);
        Some(is_new)
    }

    /// Add a member with NX/XX/GET/CH options.
    pub fn add_with_opts(
        &mut self,
        member: Bytes,
        longitude: f64,
        latitude: f64,
        opts: &GeoAddOptions,
    ) -> GeoAddResult {
        if !GeoPoint::is_valid_coordinates(longitude, latitude) {
            return GeoAddResult::InvalidCoords;
        }

        let existing = self.members.get(&member).map(|p| (p.longitude, p.latitude));

        match existing {
            Some(old) => {
                // Member exists
                if opts.nx {
                    return GeoAddResult::Skipped;
                }
                let point = GeoPoint { member: member.clone(), longitude, latitude };
                self.members.insert(member, point);
                GeoAddResult::Updated(old.0, old.1)
            }
            None => {
                // New member
                if opts.xx {
                    return GeoAddResult::Skipped;
                }
                let point = GeoPoint { member: member.clone(), longitude, latitude };
                self.members.insert(member, point);
                GeoAddResult::Added
            }
        }
    }

    /// Calculate the distance between two members in the given unit.
    /// Returns None if either member does not exist.
    pub fn distance_between(&self, member1: &Bytes, member2: &Bytes) -> Option<f64> {
        let p1 = self.members.get(member1)?;
        let p2 = self.members.get(member2)?;
        Some(haversine_distance(p1.longitude, p1.latitude, p2.longitude, p2.latitude))
    }

    /// Get the position (longitude, latitude) of a member
    pub fn get_position(&self, member: &Bytes) -> Option<(f64, f64)> {
        self.members
            .get(member)
            .map(|p| (p.longitude, p.latitude))
    }

    /// Get a member by name
    pub fn get_member(&self, member: &Bytes) -> Option<&GeoPoint> {
        self.members.get(member)
    }

    /// Remove a member from the set
    pub fn remove(&mut self, member: &Bytes) -> bool {
        self.members.remove(member).is_some()
    }

    /// Iterate all geo members (for persistence / snapshot).
    pub fn iter_members(&self) -> impl Iterator<Item = (Bytes, f64, f64)> + '_ {
        self.members
            .values()
            .map(|p| (p.member.clone(), p.longitude, p.latitude))
    }

    /// Search for members within a radius from a center point
    pub fn search_radius(
        &self,
        center_lon: f64,
        center_lat: f64,
        radius_m: f64,
        unit: DistanceUnit,
    ) -> Vec<GeoSearchResult> {
        self.search_radius_sorted(center_lon, center_lat, radius_m, unit, SortOrder::Asc)
    }

    /// Search for members within a radius from a center point with configurable sort order
    pub fn search_radius_sorted(
        &self,
        center_lon: f64,
        center_lat: f64,
        radius_m: f64,
        unit: DistanceUnit,
        sort: SortOrder,
    ) -> Vec<GeoSearchResult> {
        if !GeoPoint::is_valid_coordinates(center_lon, center_lat) {
            return vec![];
        }

        let radius_meters = unit.to_meters(radius_m);
        let mut results = Vec::new();

        for point in self.members.values() {
            let distance = haversine_distance(center_lon, center_lat, point.longitude, point.latitude);
            if distance <= radius_meters {
                results.push(GeoSearchResult {
                    member: point.member.clone(),
                    longitude: point.longitude,
                    latitude: point.latitude,
                    distance,
                });
            }
        }

        Self::sort_results(&mut results, sort);
        results
    }

    /// Search for members within an axis-aligned bounding box
    pub fn search_box(
        &self,
        center_lon: f64,
        center_lat: f64,
        width_m: f64,
        height_m: f64,
        sort: SortOrder,
    ) -> Vec<GeoSearchResult> {
        if !GeoPoint::is_valid_coordinates(center_lon, center_lat) {
            return vec![];
        }

        // Convert half-dimensions to approximate degree offsets
        let half_h = height_m / 2.0;
        let half_w = width_m / 2.0;
        let lat_rad = center_lat * PI / 180.0;
        let lat_delta = half_h / 111320.0;
        let lon_delta = half_w / (111320.0 * lat_rad.cos().max(1e-10));

        let mut results = Vec::new();
        for point in self.members.values() {
            if (point.latitude - center_lat).abs() <= lat_delta
                && (point.longitude - center_lon).abs() <= lon_delta
            {
                let distance =
                    haversine_distance(center_lon, center_lat, point.longitude, point.latitude);
                results.push(GeoSearchResult {
                    member: point.member.clone(),
                    longitude: point.longitude,
                    latitude: point.latitude,
                    distance,
                });
            }
        }

        Self::sort_results(&mut results, sort);
        results
    }

    fn sort_results(results: &mut Vec<GeoSearchResult>, sort: SortOrder) {
        match sort {
            SortOrder::Asc | SortOrder::None => {
                results.sort_by(|a, b| {
                    a.distance
                        .partial_cmp(&b.distance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
            SortOrder::Desc => {
                results.sort_by(|a, b| {
                    b.distance
                        .partial_cmp(&a.distance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        }
    }

    /// Search for members from a member position within a radius
    pub fn search_from_member(
        &self,
        member: &Bytes,
        radius: f64,
        unit: DistanceUnit,
    ) -> Vec<GeoSearchResult> {
        if let Some(point) = self.members.get(member) {
            self.search_radius(point.longitude, point.latitude, radius, unit)
        } else {
            vec![]
        }
    }

    /// Get the number of members in the set
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Approximate heap size of geo set contents (members only; key charged separately).
    pub fn memory_usage(&self) -> usize {
        use crate::memory::{with_alloc_overhead, BYTES_OVERHEAD, DICT_ENTRY_OVERHEAD};
        use std::mem;

        let mut raw = mem::size_of::<Self>();
        raw += self.members.capacity().saturating_mul(8);
        for (key, point) in &self.members {
            // member name stored as map key + inside GeoPoint
            raw += key.len()
                + point.member.len()
                + BYTES_OVERHEAD * 2
                + DICT_ENTRY_OVERHEAD
                + mem::size_of::<GeoPoint>();
        }
        with_alloc_overhead(raw)
    }
}

/// Result of a geospatial search
#[derive(Debug, Clone)]
pub struct GeoSearchResult {
    pub member: Bytes,
    pub longitude: f64,
    pub latitude: f64,
    pub distance: f64,
}

/// Distance unit for geospatial operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceUnit {
    Meters,
    Kilometers,
    Miles,
    Feet,
}

impl DistanceUnit {
    /// Parse unit from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "M" => Some(DistanceUnit::Meters),
            "KM" => Some(DistanceUnit::Kilometers),
            "MI" => Some(DistanceUnit::Miles),
            "FT" => Some(DistanceUnit::Feet),
            _ => None,
        }
    }

    /// Convert distance to meters
    pub fn to_meters(&self, distance: f64) -> f64 {
        match self {
            DistanceUnit::Meters => distance,
            DistanceUnit::Kilometers => distance * 1000.0,
            DistanceUnit::Miles => distance * 1609.34,
            DistanceUnit::Feet => distance * 0.3048,
        }
    }

    /// Convert distance from meters
    pub fn from_meters(&self, meters: f64) -> f64 {
        match self {
            DistanceUnit::Meters => meters,
            DistanceUnit::Kilometers => meters / 1000.0,
            DistanceUnit::Miles => meters / 1609.34,
            DistanceUnit::Feet => meters / 0.3048,
        }
    }
}

/// Earth radius in meters
const EARTH_RADIUS_METERS: f64 = 6372797.560856;

/// Calculate distance between two points using Haversine formula
/// Returns distance in meters
pub fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1_rad = lat1 * PI / 180.0;
    let lat2_rad = lat2 * PI / 180.0;
    let delta_lat = (lat2 - lat1) * PI / 180.0;
    let delta_lon = (lon2 - lon1) * PI / 180.0;

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_METERS * c
}

/// Encode longitude and latitude to geohash (52-bit precision, Redis-compatible)
/// Uses the Mercator latitude range (-85.05112878 … +85.05112878) to match Redis's
/// internal geohash representation exactly.
pub fn geohash_encode(longitude: f64, latitude: f64) -> u64 {
    let lon_norm = (longitude - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN);
    let lat_norm = (latitude - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);

    let lon_bits = (lon_norm * ((1u64 << 26) as f64)) as u64;
    let lat_bits = (lat_norm * ((1u64 << 26) as f64)) as u64;

    interleave_bits(lon_bits, lat_bits)
}

/// Decode geohash to approximate longitude and latitude
pub fn geohash_decode(hash: u64) -> (f64, f64) {
    let (lon_bits, lat_bits) = deinterleave_bits(hash);

    let lon_norm = lon_bits as f64 / ((1u64 << 26) as f64);
    let lat_norm = lat_bits as f64 / ((1u64 << 26) as f64);

    let longitude = lon_norm * (GEO_LONG_MAX - GEO_LONG_MIN) + GEO_LONG_MIN;
    let latitude = lat_norm * (GEO_LAT_MAX - GEO_LAT_MIN) + GEO_LAT_MIN;

    (longitude, latitude)
}

/// Encode a 52-bit geohash integer as the 11-character Redis-compatible base32 string.
///
/// Redis left-shifts the 52-bit value by 3 (padding to 55 bits = 11 × 5) and then
/// maps each 5-bit group from MSB to LSB through the standard geohash alphabet.
pub fn geohash_to_string(hash: u64) -> [u8; 11] {
    let mut result = [0u8; 11];
    // Shift left by 3 so the 52 useful bits occupy positions [54..3]
    let bits: u64 = hash << 3;
    for i in 0..11usize {
        let shift = 55 - 5 * (i + 1);
        let idx = ((bits >> shift) & 0x1f) as usize;
        result[i] = GEO_ALPHABET[idx];
    }
    result
}

/// Interleave bits of two 26-bit numbers into a 52-bit number
fn interleave_bits(x: u64, y: u64) -> u64 {
    let mut result = 0u64;
    for i in 0..26 {
        result |= ((x >> i) & 1) << (2 * i);
        result |= ((y >> i) & 1) << (2 * i + 1);
    }
    result
}

/// Deinterleave a 52-bit number into two 26-bit numbers
fn deinterleave_bits(hash: u64) -> (u64, u64) {
    let mut x = 0u64;
    let mut y = 0u64;
    for i in 0..26 {
        x |= ((hash >> (2 * i)) & 1) << i;
        y |= ((hash >> (2 * i + 1)) & 1) << i;
    }
    (x, y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geopoint_creation() {
        let point = GeoPoint::new(Bytes::from("Seoul"), 126.9780, 37.5665);
        assert!(point.is_some());

        let invalid = GeoPoint::new(Bytes::from("Invalid"), 200.0, 37.5665);
        assert!(invalid.is_none());
    }

    #[test]
    fn test_haversine_distance() {
        // Distance between Seoul and Busan
        let seoul_lon = 126.9780;
        let seoul_lat = 37.5665;
        let busan_lon = 129.0756;
        let busan_lat = 35.1796;

        let distance = haversine_distance(seoul_lon, seoul_lat, busan_lon, busan_lat);
        // Approximate distance is about 325 km
        assert!((distance - 325000.0).abs() < 10000.0);
    }

    #[test]
    fn test_geoset_operations() {
        let mut geoset = GeoSet::new();

        assert_eq!(geoset.add(Bytes::from("Seoul"), 126.9780, 37.5665), Some(true));
        assert_eq!(geoset.add(Bytes::from("Busan"), 129.0756, 35.1796), Some(true));
        assert_eq!(geoset.add(Bytes::from("Seoul"), 126.9780, 37.5665), Some(false));

        assert_eq!(geoset.len(), 2);

        let pos = geoset.get_position(&Bytes::from("Seoul"));
        assert!(pos.is_some());
        if let Some((lon, lat)) = pos {
            assert!((lon - 126.9780).abs() < 0.0001);
            assert!((lat - 37.5665).abs() < 0.0001);
        }
    }

    #[test]
    fn test_geosearch_radius() {
        let mut geoset = GeoSet::new();
        geoset.add(Bytes::from("Seoul"), 126.9780, 37.5665);
        geoset.add(Bytes::from("Busan"), 129.0756, 35.1796);
        geoset.add(Bytes::from("Incheon"), 126.7052, 37.4563);

        // Search within 50km from Seoul
        let results = geoset.search_radius(126.9780, 37.5665, 50.0, DistanceUnit::Kilometers);
        assert_eq!(results.len(), 2); // Seoul and Incheon
    }

    #[test]
    fn test_geohash_encode_decode() {
        let lon = 126.9780;
        let lat = 37.5665;

        let hash = geohash_encode(lon, lat);
        let (decoded_lon, decoded_lat) = geohash_decode(hash);

        assert!((decoded_lon - lon).abs() < 0.001);
        assert!((decoded_lat - lat).abs() < 0.001);
    }
}
