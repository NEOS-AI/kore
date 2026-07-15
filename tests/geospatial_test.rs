use bytes::Bytes;
use kore::{Cache, GeoSet, GeoPoint, DistanceUnit};

#[cfg(test)]
mod geospatial_tests {
    use super::*;

    #[test]
    fn test_geopoint_creation() {
        let point = GeoPoint::new(Bytes::from("Seoul"), 126.9780, 37.5665);
        assert!(point.is_some());
        let point = point.unwrap();
        assert_eq!(point.longitude, 126.9780);
        assert_eq!(point.latitude, 37.5665);

        // Test invalid coordinates
        let invalid = GeoPoint::new(Bytes::from("Invalid"), 200.0, 37.5665);
        assert!(invalid.is_none());

        let invalid = GeoPoint::new(Bytes::from("Invalid"), 126.9780, 90.0);
        assert!(invalid.is_none());
    }

    #[test]
    fn test_geoset_add() {
        let mut geoset = GeoSet::new();

        // Add new member
        assert_eq!(geoset.add(Bytes::from("Seoul"), 126.9780, 37.5665), Some(true));
        assert_eq!(geoset.len(), 1);

        // Update existing member
        assert_eq!(geoset.add(Bytes::from("Seoul"), 126.9780, 37.5666), Some(false));
        assert_eq!(geoset.len(), 1);

        // Add another member
        assert_eq!(geoset.add(Bytes::from("Busan"), 129.0756, 35.1796), Some(true));
        assert_eq!(geoset.len(), 2);

        // Invalid coordinates
        assert_eq!(geoset.add(Bytes::from("Invalid"), 200.0, 37.5665), None);
    }

    #[test]
    fn test_geoset_get_position() {
        let mut geoset = GeoSet::new();
        geoset.add(Bytes::from("Seoul"), 126.9780, 37.5665);

        let pos = geoset.get_position(&Bytes::from("Seoul"));
        assert!(pos.is_some());
        let (lon, lat) = pos.unwrap();
        assert!((lon - 126.9780).abs() < 0.0001);
        assert!((lat - 37.5665).abs() < 0.0001);

        // Non-existent member
        let pos = geoset.get_position(&Bytes::from("Tokyo"));
        assert!(pos.is_none());
    }

    #[test]
    fn test_geoset_remove() {
        let mut geoset = GeoSet::new();
        geoset.add(Bytes::from("Seoul"), 126.9780, 37.5665);
        assert_eq!(geoset.len(), 1);

        assert!(geoset.remove(&Bytes::from("Seoul")));
        assert_eq!(geoset.len(), 0);

        // Remove non-existent member
        assert!(!geoset.remove(&Bytes::from("Tokyo")));
    }

    #[test]
    fn test_geosearch_radius() {
        let mut geoset = GeoSet::new();
        geoset.add(Bytes::from("Seoul"), 126.9780, 37.5665);
        geoset.add(Bytes::from("Busan"), 129.0756, 35.1796);
        geoset.add(Bytes::from("Incheon"), 126.7052, 37.4563);
        geoset.add(Bytes::from("Daejeon"), 127.3845, 36.3504);

        // Search within 50km from Seoul
        let results = geoset.search_radius(126.9780, 37.5665, 50.0, DistanceUnit::Kilometers);
        assert_eq!(results.len(), 2); // Seoul and Incheon
        
        // Results should be sorted by distance
        assert_eq!(results[0].member, Bytes::from("Seoul"));
        assert_eq!(results[1].member, Bytes::from("Incheon"));

        // Search within 200km from Seoul
        let results = geoset.search_radius(126.9780, 37.5665, 200.0, DistanceUnit::Kilometers);
        assert_eq!(results.len(), 3); // Seoul, Incheon, Daejeon

        // Search within 500km from Seoul
        let results = geoset.search_radius(126.9780, 37.5665, 500.0, DistanceUnit::Kilometers);
        assert_eq!(results.len(), 4); // All cities
    }

    #[test]
    fn test_geosearch_from_member() {
        let mut geoset = GeoSet::new();
        geoset.add(Bytes::from("Seoul"), 126.9780, 37.5665);
        geoset.add(Bytes::from("Busan"), 129.0756, 35.1796);
        geoset.add(Bytes::from("Incheon"), 126.7052, 37.4563);

        // Search within 50km from Seoul
        let results = geoset.search_from_member(&Bytes::from("Seoul"), 50.0, DistanceUnit::Kilometers);
        assert_eq!(results.len(), 2); // Seoul and Incheon

        // Search from non-existent member
        let results = geoset.search_from_member(&Bytes::from("Tokyo"), 100.0, DistanceUnit::Kilometers);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_distance_units() {
        let mut geoset = GeoSet::new();
        geoset.add(Bytes::from("Seoul"), 126.9780, 37.5665);
        geoset.add(Bytes::from("Incheon"), 126.7052, 37.4563);

        // Search in different units
        let results_m = geoset.search_radius(126.9780, 37.5665, 30000.0, DistanceUnit::Meters);
        let results_km = geoset.search_radius(126.9780, 37.5665, 30.0, DistanceUnit::Kilometers);
        let results_mi = geoset.search_radius(126.9780, 37.5665, 18.64, DistanceUnit::Miles);
        
        // All should return the same results
        assert_eq!(results_m.len(), results_km.len());
        assert_eq!(results_km.len(), results_mi.len());
    }

    #[tokio::test]
    async fn test_cache_integration() {
        let cache = Cache::new(4, 1024 * 1024 * 1024);

        // Get or create geospatial set
        let geoset = cache.get_or_create_geo_set(&Bytes::from("cities")).unwrap();
        {
            let mut set = geoset.write().unwrap();
            set.add(Bytes::from("Seoul"), 126.9780, 37.5665);
            set.add(Bytes::from("Busan"), 129.0756, 35.1796);
        }

        // Retrieve the same set
        let geoset2 = cache.get_geo_set(&Bytes::from("cities"));
        assert!(geoset2.is_some());
        {
            let geoset_guard = geoset2.unwrap();
            let set = geoset_guard.read().unwrap();
            assert_eq!(set.len(), 2);
        }

        // Check count
        assert_eq!(cache.geo_set_count(), 1);

        // Remove set
        assert!(cache.remove_geo_set(&Bytes::from("cities")));
        assert_eq!(cache.geo_set_count(), 0);
    }

    #[test]
    fn test_distance_calculation() {
        // Distance between Seoul and Busan
        let point1 = GeoPoint::new(Bytes::from("Seoul"), 126.9780, 37.5665).unwrap();
        let point2 = GeoPoint::new(Bytes::from("Busan"), 129.0756, 35.1796).unwrap();

        let distance = point1.distance_to(&point2);
        // Approximate distance is about 325 km
        assert!((distance - 325000.0).abs() < 10000.0);

        // Distance between Incheon and Seoul
        let point3 = GeoPoint::new(Bytes::from("Incheon"), 126.7052, 37.4563).unwrap();
        let distance2 = point1.distance_to(&point3);
        // Approximate distance is about 27 km
        assert!((distance2 - 27000.0).abs() < 5000.0);
    }

    #[test]
    fn test_geohash_encoding() {
        use kore::geospatial::{geohash_encode, geohash_decode};

        let lon = 126.9780;
        let lat = 37.5665;

        let hash = geohash_encode(lon, lat);
        let (decoded_lon, decoded_lat) = geohash_decode(hash);

        // Check precision (should be within 0.001 degrees)
        assert!((decoded_lon - lon).abs() < 0.001);
        assert!((decoded_lat - lat).abs() < 0.001);
    }

    #[test]
    fn test_distance_unit_conversion() {
        let unit_m = DistanceUnit::Meters;
        let unit_km = DistanceUnit::Kilometers;
        let unit_mi = DistanceUnit::Miles;
        let unit_ft = DistanceUnit::Feet;

        // Test to_meters
        assert_eq!(unit_m.to_meters(100.0), 100.0);
        assert_eq!(unit_km.to_meters(1.0), 1000.0);
        assert!((unit_mi.to_meters(1.0) - 1609.34).abs() < 0.01);
        assert!((unit_ft.to_meters(1.0) - 0.3048).abs() < 0.0001);

        // Test from_meters
        assert_eq!(unit_m.from_meters(100.0), 100.0);
        assert_eq!(unit_km.from_meters(1000.0), 1.0);
        assert!((unit_mi.from_meters(1609.34) - 1.0).abs() < 0.01);
        assert!((unit_ft.from_meters(0.3048) - 1.0).abs() < 0.0001);
    }

    #[test]
    fn test_unit_parsing() {
        assert_eq!(DistanceUnit::from_str("M"), Some(DistanceUnit::Meters));
        assert_eq!(DistanceUnit::from_str("m"), Some(DistanceUnit::Meters));
        assert_eq!(DistanceUnit::from_str("KM"), Some(DistanceUnit::Kilometers));
        assert_eq!(DistanceUnit::from_str("km"), Some(DistanceUnit::Kilometers));
        assert_eq!(DistanceUnit::from_str("MI"), Some(DistanceUnit::Miles));
        assert_eq!(DistanceUnit::from_str("mi"), Some(DistanceUnit::Miles));
        assert_eq!(DistanceUnit::from_str("FT"), Some(DistanceUnit::Feet));
        assert_eq!(DistanceUnit::from_str("ft"), Some(DistanceUnit::Feet));
        assert_eq!(DistanceUnit::from_str("INVALID"), None);
    }

    #[test]
    fn test_geoset_memory_usage() {
        let mut geoset = GeoSet::new();
        
        // Empty set should have minimal memory
        let empty_memory = geoset.memory_usage();
        assert!(empty_memory > 0);

        // Add some members
        geoset.add(Bytes::from("Seoul"), 126.9780, 37.5665);
        geoset.add(Bytes::from("Busan"), 129.0756, 35.1796);
        geoset.add(Bytes::from("Tokyo"), 139.6917, 35.6895);

        let used_memory = geoset.memory_usage();
        assert!(used_memory > empty_memory);
        
        // Memory should grow with more members
        geoset.add(Bytes::from("New York"), -74.0060, 40.7128);
        let more_memory = geoset.memory_usage();
        assert!(more_memory > used_memory);
    }

    #[tokio::test]
    async fn test_geo_sets_memory_tracking() {
        let cache = Cache::new(4, 1024 * 1024 * 1024);

        // Initially no memory used
        assert_eq!(cache.geo_sets_memory(), 0);
        assert_eq!(cache.geo_set_count(), 0);

        // Create first geo set
        let geoset1 = cache.get_or_create_geo_set(&Bytes::from("cities")).unwrap();
        {
            let mut set = geoset1.write().unwrap();
            set.add(Bytes::from("Seoul"), 126.9780, 37.5665);
            set.add(Bytes::from("Busan"), 129.0756, 35.1796);
        }

        let memory1 = cache.geo_sets_memory();
        assert!(memory1 > 0);
        assert_eq!(cache.geo_set_count(), 1);

        // Create second geo set
        let geoset2 = cache.get_or_create_geo_set(&Bytes::from("landmarks")).unwrap();
        {
            let mut set = geoset2.write().unwrap();
            set.add(Bytes::from("Eiffel Tower"), 2.2945, 48.8584);
            set.add(Bytes::from("Big Ben"), -0.1246, 51.5007);
        }

        let memory2 = cache.geo_sets_memory();
        assert!(memory2 > memory1);
        assert_eq!(cache.geo_set_count(), 2);

        // Remove one set
        cache.remove_geo_set(&Bytes::from("cities"));
        let memory3 = cache.geo_sets_memory();
        assert!(memory3 < memory2);
        assert_eq!(cache.geo_set_count(), 1);
    }

    #[tokio::test]
    async fn test_geospatial_stats_tracking() {
        let cache = Cache::new(4, 1024 * 1024 * 1024);

        // Check initial stats
        assert_eq!(cache.stats.cmd_geoadd.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(cache.stats.cmd_geosearch.load(std::sync::atomic::Ordering::Relaxed), 0);

        // Stats tracking is done in command handlers
        // This test verifies that the stats fields exist and are accessible
        cache.stats.incr(&cache.stats.cmd_geoadd);
        assert_eq!(cache.stats.cmd_geoadd.load(std::sync::atomic::Ordering::Relaxed), 1);

        cache.stats.incr(&cache.stats.cmd_geosearch);
        cache.stats.incr(&cache.stats.cmd_geosearch);
        assert_eq!(cache.stats.cmd_geosearch.load(std::sync::atomic::Ordering::Relaxed), 2);
    }
}
