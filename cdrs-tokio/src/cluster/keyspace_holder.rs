use std::sync::Arc;

use arc_swap::ArcSwapOption;

/// Holds currently set global keyspace.
#[derive(Default, Debug)]
pub struct KeyspaceHolder {
    current_keyspace: ArcSwapOption<String>,
}

impl KeyspaceHolder {
    #[inline]
    pub fn current_keyspace(&self) -> Option<Arc<String>> {
        self.current_keyspace.load().clone()
    }

    #[inline]
    pub fn update_current_keyspace(&self, keyspace: String) {
        self.current_keyspace.store(Some(Arc::new(keyspace)));
    }
}
