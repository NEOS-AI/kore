use crate::geospatial::GeoSet;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

use super::Cache;

pub type SharedGeoSet = Arc<RwLock<GeoSet>>;

impl Cache {
    /// Get a geospatial set by key
    pub fn get_geo_set(&self, key: &Bytes) -> Option<SharedGeoSet> {
        let sets = self.geo_sets.read().unwrap();
        sets.get(key).cloned()
    }

    /// Get or create a geospatial set
    pub fn get_or_create_geo_set(&self, key: &Bytes) -> SharedGeoSet {
        let mut sets = self.geo_sets.write().unwrap();
        sets.entry(key.clone())
            .or_insert_with(|| Arc::new(RwLock::new(GeoSet::new())))
            .clone()
    }

    /// Remove a geospatial set
    pub fn remove_geo_set(&self, key: &Bytes) -> bool {
        let mut sets = self.geo_sets.write().unwrap();
        sets.remove(key).is_some()
    }

    /// Get the number of geospatial sets
    pub fn geo_set_count(&self) -> usize {
        let sets = self.geo_sets.read().unwrap();
        sets.len()
    }

    /// Calculate total memory usage of all geospatial sets
    pub fn geo_sets_memory(&self) -> usize {
        let sets = self.geo_sets.read().unwrap();
        sets.iter()
            .map(|(key, geo_set)| {
                let set = geo_set.read().unwrap();
                key.len() + set.memory_usage()
            })
            .sum()
    }

    /// Clear all geospatial sets
    pub fn clear_geo_sets(&self) {
        let mut sets = self.geo_sets.write().unwrap();
        sets.clear();
    }
}
