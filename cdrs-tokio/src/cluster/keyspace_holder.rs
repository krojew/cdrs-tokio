use std::sync::Arc;

use arc_swap::ArcSwapOption;
use tokio::sync::watch::Sender;

/// Holds currently set global keyspace.
#[derive(Debug)]
pub struct KeyspaceHolder {
    current_keyspace: ArcSwapOption<String>,
    keyspace_sender: Sender<Option<String>>,
}

impl KeyspaceHolder {
    pub fn new(keyspace_sender: Sender<Option<String>>) -> Self {
        KeyspaceHolder {
            current_keyspace: Default::default(),
            keyspace_sender,
        }
    }

    #[inline]
    pub fn current_keyspace(&self) -> Option<Arc<String>> {
        self.current_keyspace.load().clone()
    }

    #[inline]
    pub fn update_current_keyspace(&self, keyspace: String) {
        let old_keyspace = self.current_keyspace.swap(Some(Arc::new(keyspace.clone())));
        match &old_keyspace {
            None => {
                self.send_notification(keyspace);
            }
            Some(old_keyspace) if **old_keyspace != keyspace => {
                self.send_notification(keyspace);
            }
            _ => {}
        }
    }

    #[inline]
    pub fn update_current_keyspace_without_notification(&self, keyspace: String) {
        self.current_keyspace.store(Some(Arc::new(keyspace)));
    }

    #[inline]
    fn send_notification(&self, keyspace: String) {
        let _ = self.keyspace_sender.send(Some(keyspace));
    }
}
