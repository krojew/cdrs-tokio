use derive_more::Constructor;

/// Special type that represents Cassandra blob type.
#[derive(PartialEq, Eq, Hash, Debug, Clone, Constructor)]
#[repr(transparent)]
pub struct Blob(Vec<u8>);

impl Blob {
    /// Returns a mutable reference to an underlying slice of bytes.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &[u8] {
        self.0.as_mut_slice()
    }

    /// Returns underlying vector of bytes.
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for Blob {
    #[inline]
    fn from(vec: Vec<u8>) -> Self {
        Blob::new(vec)
    }
}

impl From<&[u8]> for Blob {
    #[inline]
    fn from(value: &[u8]) -> Self {
        Blob::new(value.to_vec())
    }
}
