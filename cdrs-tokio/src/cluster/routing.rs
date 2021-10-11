use self::cdrs_tokio::frame::Serialize;
use self::cdrs_tokio::types::prelude::*;
use crate as cdrs_tokio;
use crate::types::from_cdrs::FromCdrsByName;
use cdrs_tokio_helpers_derive::IntoCdrsValue;
use cdrs_tokio_helpers_derive::TryFromUdt;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, IntoCdrsValue, TryFromUdt)]
pub struct Token {
    pub value: i64,
}

impl std::str::FromStr for Token {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> std::result::Result<Token, std::num::ParseIntError> {
        Ok(Token { value: s.parse()? })
    }
}
