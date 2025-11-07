use bytes::Bytes;
use std::collections::HashMap;
use std::f64::consts::PI;

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
        longitude >= -180.0 && longitude <= 180.0 && latitude >= -85.05112878 && latitude <= 85.05112878
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

    /// Search for members within a radius from a center point
    pub fn search_radius(
        &self,
        center_lon: f64,
        center_lat: f64,
        radius_m: f64,
        unit: DistanceUnit,
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

                // Sort results by distance
        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results
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

    /// Calculate approximate memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        use std::mem;
        
        // Base HashMap overhead
        let mut size = mem::size_of::<Self>();
        
        // Each entry: key (Bytes) + value (GeoPoint)
        for (key, point) in &self.members {
            // Bytes key size (includes capacity)
            size += mem::size_of::<Bytes>() + key.len();
            
            // GeoPoint size (includes Bytes member + 2 f64s)
            size += mem::size_of::<GeoPoint>() + point.member.len();
        }
        
        size
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

/// Encode longitude and latitude to geohash (52-bit precision)
/// This is a simplified geohash implementation for internal use
pub fn geohash_encode(longitude: f64, latitude: f64) -> u64 {
    // Normalize longitude and latitude to 0-1 range
    let lon_norm = (longitude + 180.0) / 360.0;
    let lat_norm = (latitude + 90.0) / 180.0;

    // Interleave bits of longitude and latitude
    let lon_bits = (lon_norm * ((1u64 << 26) as f64)) as u64;
    let lat_bits = (lat_norm * ((1u64 << 26) as f64)) as u64;

    interleave_bits(lon_bits, lat_bits)
}

/// Decode geohash to approximate longitude and latitude
pub fn geohash_decode(hash: u64) -> (f64, f64) {
    let (lon_bits, lat_bits) = deinterleave_bits(hash);

    let lon_norm = lon_bits as f64 / ((1u64 << 26) as f64);
    let lat_norm = lat_bits as f64 / ((1u64 << 26) as f64);

    let longitude = lon_norm * 360.0 - 180.0;
    let latitude = lat_norm * 180.0 - 90.0;

    (longitude, latitude)
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
