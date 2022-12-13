use crate::{RwDreamer, RwLock};
use alloc::sync::Arc;
use hashbrown::{hash_map::Entry, HashMap};

pub struct Cache<K, V, D>
where
    K: core::hash::Hash + Eq + Clone + core::fmt::Debug + Send,
    V: Send + Sync,
    D: RwDreamer,
{
    inner: RwLock<HashMap<K, Arc<V>>, D>,
}

impl<K, V, D> Cache<K, V, D>
where
    K: core::hash::Hash + Eq + Clone + core::fmt::Debug + Send,
    V: Send + Sync,
    D: RwDreamer,
{
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    #[inline]
    pub fn get(&self, k: &K) -> Option<Arc<V>> {
        self.inner.read().get(k).cloned()
    }

    #[inline]
    pub fn get_or_insert<F, E>(&self, k: K, f: F) -> Result<Arc<V>, E>
    where
        F: FnOnce(K) -> Result<V, E>,
    {
        if let Some(v) = self.get(&k) {
            Ok(v)
        } else {
            match self.inner.write().entry(k.clone()) {
                Entry::Vacant(e) => f(k).map(|v| e.insert(Arc::new(v)).clone()),
                Entry::Occupied(e) => Ok(e.into_mut().clone()),
            }
        }
    }

    #[inline]
    pub fn get_or_insert_arc<F, E>(&self, k: K, f: F) -> Result<Arc<V>, E>
    where
        F: FnOnce(K) -> Result<Arc<V>, E>,
    {
        if let Some(v) = self.get(&k) {
            Ok(v)
        } else {
            match self.inner.write().entry(k.clone()) {
                Entry::Vacant(e) => f(k).map(|v| e.insert(v).clone()),
                Entry::Occupied(e) => Ok(e.into_mut().clone()),
            }
        }
    }

    #[inline]
    pub fn take(&self, k: &K) -> Option<Arc<V>> {
        self.inner.write().remove(k)
    }

    #[inline]
    pub fn flush(&self) {
        *self.inner.write() = HashMap::new();
    }
}
