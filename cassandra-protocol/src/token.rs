use derive_more::Constructor;
use std::convert::TryFrom;

use crate::error::Error;

/// A token on the ring. Only Murmur3 tokens are supported for now.
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Default, Debug, Hash, Constructor)]
pub struct Murmur3Token {
    pub value: i64,
}

impl TryFrom<String> for Murmur3Token {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value
            .parse()
            .map_err(|error| format!("Error parsing token: {}", error).into())
            .map(Murmur3Token::new)
    }
}

impl From<i64> for Murmur3Token {
    fn from(value: i64) -> Self {
        Murmur3Token::new(value)
    }
}
