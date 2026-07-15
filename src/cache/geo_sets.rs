use crate::error::Result;
use crate::geospatial::GeoSet;
use crate::memory::MemoryCategory;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use super::storage::KeyType;
use super::Cache;

pub type SharedGeoSet = Arc<RwLock<GeoSet>>;

impl Cache {
    /// Account a net memory change for geo sets.
    pub(crate) fn account_geo_set_delta(&self, old_size: usize, new_size: usize) {
        if old_size > 0 {
            self.memory_tracker
                .deallocate(old_size, MemoryCategory::GeoSets);
        }
        if new_size > 0 {
            self.memory_tracker
                .account(new_size, MemoryCategory::GeoSets);
        }
    }

    /// Get a geospatial set by key
    pub fn get_geo_set(&self, key: &Bytes) -> Option<SharedGeoSet> {
        self.geo_sets.get(key)
    }

    /// Get or create a geospatial set.
    /// Returns WrongType if the key already holds a different type.
    pub fn get_or_create_geo_set(&self, key: &Bytes) -> Result<SharedGeoSet> {
        self.ensure_type(key, KeyType::Geo)?;
        if let Some(existing) = self.geo_sets.get(key) {
            return Ok(existing);
        }
        let base = crate::memory::estimate_keyed_object(
            key.len(),
            GeoSet::new().memory_usage(),
        );
        self.ensure_non_string_capacity(base)?;
        Ok(self.geo_sets.get_or_insert_with(key.clone(), || {
            self.memory_tracker
                .account(base, MemoryCategory::GeoSets);
            Arc::new(RwLock::new(GeoSet::new()))
        }))
    }

    /// Remove a geospatial set and free its tracked memory
    pub fn remove_geo_set(&self, key: &Bytes) -> bool {
        if let Some(set) = self.geo_sets.remove(key) {
            let size =
                crate::memory::estimate_keyed_object(key.len(), set.read().memory_usage());
            self.memory_tracker
                .deallocate(size, MemoryCategory::GeoSets);
            true
        } else {
            false
        }
    }

    /// Get the number of geospatial sets
    pub fn geo_set_count(&self) -> usize {
        self.geo_sets.len()
    }

    /// Calculate total memory usage of all geospatial sets
    pub fn geo_sets_memory(&self) -> usize {
        let mut total = 0usize;
        self.geo_sets.for_each(|key, geo_set| {
            { let set = geo_set.read();
                total += crate::memory::estimate_keyed_object(key.len(), set.memory_usage());
            }
        });
        total
    }

    /// Clear all geospatial sets
    pub fn clear_geo_sets(&self) {
        self.geo_sets.clear();
    }

    /// Export all geo sets for persistence: (key, [(member, lon, lat), ...]).
    pub fn export_geos(&self) -> Vec<(Bytes, Vec<(Bytes, f64, f64)>)> {
        let mut out = Vec::new();
        self.geo_sets.for_each(|key, geoset| {
            { let set = geoset.read();
                out.push((key.clone(), set.iter_members().collect()));
            }
        });
        out
    }
}
