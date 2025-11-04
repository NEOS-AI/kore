use crate::cache::Cache;
use crate::deadlock::{DeadlockDetector, DeadlockStatus};
use crate::entry::{LoadOptions, StoreOptions};
use crate::error::{Error, Result};
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use rand::{thread_rng, Rng};

/// Redlock distributed lock implementation
/// 
/// Redlock is a distributed lock algorithm that provides mutual exclusion
/// across multiple independent instances. It requires a quorum of instances
/// to agree on lock acquisition for it to succeed.
/// 
/// Reference: https://redis.io/docs/manual/patterns/distributed-locks/#the-redlock-algorithm
#[derive(Clone)]
pub struct Redlock {
    /// List of cache instances (can be multiple Kore instances)
    instances: Vec<Arc<Cache>>,
    
    /// Quorum size (typically N/2 + 1 where N is number of instances)
    pub quorum: usize,
    
    /// Clock drift factor (small percentage to account for clock differences)
    /// Default: 0.01 (1%)
    clock_drift_factor: f64,
    
    /// Retry configuration
    retry_count: usize,
    retry_delay_ms: u64,
    
    /// Deadlock detector (optional)
    deadlock_detector: Option<Arc<DeadlockDetector>>,
}

impl Redlock {
    /// Create a new Redlock instance
    /// 
    /// # Arguments
    /// * `instances` - List of cache instances to use for distributed locking
    /// 
    /// # Returns
    /// * `Result<Self>` - New Redlock instance or error if instances < 1
    pub fn new(instances: Vec<Arc<Cache>>) -> Result<Self> {
        let instance_count = instances.len();
        
        if instance_count == 0 {
            return Err(Error::InvalidArgument("At least one instance required for Redlock".to_string()));
        }
        
        // Quorum is majority: N/2 + 1
        let quorum = instance_count / 2 + 1;
        
        Ok(Self {
            instances,
            quorum,
            clock_drift_factor: 0.01,
            retry_count: 3,
            retry_delay_ms: 200,
            deadlock_detector: None,
        })
    }
    
    /// Create a Redlock instance with custom configuration
    pub fn with_config(
        instances: Vec<Arc<Cache>>,
        retry_count: usize,
        retry_delay_ms: u64,
        clock_drift_factor: f64,
    ) -> Result<Self> {
        let mut redlock = Self::new(instances)?;
        redlock.retry_count = retry_count;
        redlock.retry_delay_ms = retry_delay_ms;
        redlock.clock_drift_factor = clock_drift_factor;
        Ok(redlock)
    }
    
    /// Enable deadlock detection
    /// 
    /// # Arguments
    /// * `max_wait_time_ms` - Maximum time to wait before flagging potential deadlock
    /// * `auto_resolve` - Automatically resolve deadlocks by releasing victim locks
    pub fn with_deadlock_detection(mut self, max_wait_time_ms: u64, auto_resolve: bool) -> Self {
        self.deadlock_detector = Some(Arc::new(DeadlockDetector::new(max_wait_time_ms, auto_resolve)));
        self
    }
    
    /// Check for deadlocks
    pub fn check_deadlock(&self) -> Option<DeadlockStatus> {
        self.deadlock_detector.as_ref().map(|d| d.detect_deadlock())
    }
    
    /// Get deadlock statistics
    pub fn get_deadlock_stats(&self) -> Option<crate::deadlock::DeadlockStats> {
        self.deadlock_detector.as_ref().map(|d| d.get_stats())
    }
    
    /// Acquire a distributed lock using the Redlock algorithm
    /// 
    /// # Arguments
    /// * `resource` - Name of the resource to lock
    /// * `val` - Unique identifier for this lock (e.g., UUID)
    /// * `ttl_ms` - Time-to-live for the lock in milliseconds
    /// 
    /// # Returns
    /// * `Result<Lock>` - Lock handle if successful, error otherwise
    pub fn lock(&self, resource: &str, val: Bytes, ttl_ms: u64) -> Result<Lock> {
        // Record wait start if deadlock detection is enabled
        if let Some(ref detector) = self.deadlock_detector {
            detector.record_lock_wait(resource.to_string(), val.clone(), ttl_ms);
        }
        
        for attempt in 0..self.retry_count {
            // Check for deadlock before attempting
            if let Some(ref detector) = self.deadlock_detector {
                match detector.detect_deadlock() {
                    DeadlockStatus::Deadlock { cycle, resources } => {
                        // Clean up wait record
                        detector.remove_from_waiting(&val);
                        
                        return Err(Error::DeadlockDetected(format!(
                            "Deadlock detected involving {} clients and resources: {:?}",
                            cycle.len(),
                            resources
                        )));
                    }
                    DeadlockStatus::NoDeadlock => {
                        // Continue with lock acquisition
                    }
                }
            }
            
            match self.try_lock(resource, val.clone(), ttl_ms) {
                Ok(lock) => {
                    // Record successful acquisition
                    if let Some(ref detector) = self.deadlock_detector {
                        detector.record_lock_acquired(resource.to_string(), val.clone(), ttl_ms);
                    }
                    return Ok(lock);
                }
                Err(e) => {
                    if attempt == self.retry_count - 1 {
                        // Clean up wait record on final failure
                        if let Some(ref detector) = self.deadlock_detector {
                            detector.remove_from_waiting(&val);
                        }
                        return Err(e);
                    }
                    // Add random jitter to prevent thundering herd
                    let jitter = thread_rng().gen_range(0..50);
                    std::thread::sleep(Duration::from_millis(self.retry_delay_ms + jitter));
                }
            }
        }
        
        // Clean up wait record
        if let Some(ref detector) = self.deadlock_detector {
            detector.remove_from_waiting(&val);
        }
        
        Err(Error::LockAcquisitionFailed("Failed to acquire lock after retries".to_string()))
    }
    
    /// Try to acquire lock once (without retries)
    fn try_lock(&self, resource: &str, val: Bytes, ttl_ms: u64) -> Result<Lock> {
        let key = Bytes::from(format!("lock:{}", resource));
        let start_time = Self::current_time_ms();
        
        // Try to acquire lock on all instances
        let mut acquired_count = 0;
        let mut acquired_instances = Vec::new();
        
        for (idx, instance) in self.instances.iter().enumerate() {
            if self.lock_instance(instance, &key, val.clone(), ttl_ms)? {
                acquired_count += 1;
                acquired_instances.push(idx);
            }
        }
        
        let elapsed_ms = Self::current_time_ms() - start_time;
        
        // Calculate validity time (TTL minus elapsed time and drift)
        let drift = (ttl_ms as f64 * self.clock_drift_factor) as u64 + 2;
        let validity_time = ttl_ms.saturating_sub(elapsed_ms).saturating_sub(drift);
        
        // Check if we got quorum and still have valid time
        if acquired_count >= self.quorum && validity_time > 0 {
            Ok(Lock {
                redlock: self.clone(),
                resource: resource.to_string(),
                val,
                validity_time,
            })
        } else {
            // Failed to acquire quorum, unlock acquired instances
            self.unlock_instances(&key, &val, &acquired_instances);
            Err(Error::LockAcquisitionFailed(
                format!("Quorum not reached: {}/{}", acquired_count, self.quorum)
            ))
        }
    }
    
    /// Acquire lock on a single instance
    fn lock_instance(
        &self,
        instance: &Arc<Cache>,
        key: &Bytes,
        val: Bytes,
        ttl_ms: u64,
    ) -> Result<bool> {
        let opts = StoreOptions {
            nx: true,
            ttl_ms: Some(ttl_ms),
            ..Default::default()
        };
        
        match instance.store(key.clone(), val, opts) {
            Ok(old_value) => Ok(old_value.is_none()),
            Err(_) => Ok(false),
        }
    }
    
    /// Unlock (release) the distributed lock
    pub fn unlock(&self, lock: &Lock) -> Result<()> {
        let key = Bytes::from(format!("lock:{}", lock.resource));
        let all_instances: Vec<usize> = (0..self.instances.len()).collect();
        self.unlock_instances(&key, &lock.val, &all_instances);
        
        // Record lock release
        if let Some(ref detector) = self.deadlock_detector {
            detector.record_lock_released(&lock.resource);
        }
        
        Ok(())
    }
    
    /// Unlock specific instances
    fn unlock_instances(&self, key: &Bytes, expected_val: &Bytes, instance_indices: &[usize]) {
        for &idx in instance_indices {
            if let Some(instance) = self.instances.get(idx) {
                // Verify ownership before deleting (get and compare)
                if let Ok(Some(entry)) = instance.load(key, LoadOptions::default()) {
                    if entry.value == *expected_val {
                        let _ = instance.delete(key);
                    }
                }
            }
        }
    }
    
    /// Extend the lock's TTL (time-to-live)
    /// 
    /// This is useful for long-running operations that need to keep the lock
    /// 
    /// # Arguments
    /// * `lock` - The lock to extend
    /// * `additional_ttl_ms` - Additional time to add in milliseconds
    /// 
    /// # Returns
    /// * `Result<()>` - Ok if majority of instances were updated
    pub fn extend(&self, lock: &Lock, additional_ttl_ms: u64) -> Result<()> {
        let key = Bytes::from(format!("lock:{}", lock.resource));
        let mut extended_count = 0;
        
        for instance in &self.instances {
            // Verify ownership first
            if let Ok(Some(entry)) = instance.load(&key, LoadOptions::default()) {
                if entry.value == lock.val {
                    if instance.expire(&key, additional_ttl_ms).is_ok() {
                        extended_count += 1;
                    }
                }
            }
        }
        
        if extended_count >= self.quorum {
            Ok(())
        } else {
            Err(Error::LockExtensionFailed(
                format!("Could not extend lock on quorum: {}/{}", extended_count, self.quorum)
            ))
        }
    }
    
    /// Get current time in milliseconds
    fn current_time_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

/// A distributed lock handle
/// 
/// This represents an acquired lock. The lock should be released
/// by calling `redlock.unlock(&lock)` when done.
pub struct Lock {
    redlock: Redlock,
    resource: String,
    val: Bytes,
    validity_time: u64,
}

impl Lock {
    /// Get the lock's resource name
    pub fn resource(&self) -> &str {
        &self.resource
    }
    
    /// Get the lock's value (unique identifier)
    pub fn value(&self) -> &Bytes {
        &self.val
    }
    
    /// Get remaining validity time in milliseconds
    pub fn validity_time(&self) -> u64 {
        self.validity_time
    }
    
    /// Extend this lock's TTL
    pub fn extend(&self, additional_ttl_ms: u64) -> Result<()> {
        self.redlock.extend(self, additional_ttl_ms)
    }
}

impl Drop for Lock {
    /// Automatically unlock when the Lock is dropped
    fn drop(&mut self) {
        let _ = self.redlock.unlock(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_redlock_creation() {
        let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
        let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
        let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
        
        let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
        assert_eq!(redlock.quorum, 2); // 3/2 + 1 = 2
    }
    
    #[test]
    fn test_redlock_single_instance() {
        let cache = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
        let redlock = Redlock::new(vec![cache]).unwrap();
        assert_eq!(redlock.quorum, 1); // 1/2 + 1 = 1
    }
    
    #[test]
    fn test_redlock_no_instances() {
        let result = Redlock::new(vec![]);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_lock_acquisition() {
        let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
        let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
        let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
        
        let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
        
        let lock_val = Bytes::from("unique-id-123");
        let lock = redlock.lock("my-resource", lock_val.clone(), 10000).unwrap();
        
        assert_eq!(lock.resource(), "my-resource");
        assert_eq!(lock.value(), &lock_val);
    }
    
    #[test]
    fn test_lock_mutual_exclusion() {
        let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
        let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
        let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
        
        let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
        
        // First client acquires lock
        let lock1_val = Bytes::from("client-1");
        let _lock1 = redlock.lock("shared-resource", lock1_val, 10000).unwrap();
        
        // Second client tries to acquire same lock (should fail)
        let lock2_val = Bytes::from("client-2");
        let result = redlock.try_lock("shared-resource", lock2_val, 10000);
        
        assert!(result.is_err());
    }
}
