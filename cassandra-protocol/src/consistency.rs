#![warn(missing_docs)]
//! The module contains Rust representation of Cassandra consistency levels.
use crate::error;
use crate::frame::{FromBytes, FromCursor, Serialize, Version};
use crate::types::*;
use derive_more::Display;
use std::convert::{From, TryFrom, TryInto};
use std::default::Default;
use std::io;
use std::str::FromStr;

/// `Consistency` is an enum which represents Cassandra's consistency levels.
/// To find more details about each consistency level please refer to the following documentation:
/// <https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cqlshConsistency.html>
#[derive(Debug, PartialEq, Clone, Copy, Display, Ord, PartialOrd, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Consistency {
    /// Closest replica, as determined by the snitch.
    /// If all replica nodes are down, write succeeds after a hinted handoff.
    /// Provides low latency, guarantees writes never fail.
    /// Note: this consistency level can only be used for writes.
    /// It provides the lowest consistency and the highest availability.
    Any,
    ///
    /// A write must be written to the commit log and memtable of at least one replica node.
    /// Satisfies the needs of most users because consistency requirements are not stringent.
    #[default]
    One,
    /// A write must be written to the commit log and memtable of at least two replica nodes.
    /// Similar to ONE.
    Two,
    /// A write must be written to the commit log and memtable of at least three replica nodes.
    /// Similar to TWO.
    Three,
    /// A write must be written to the commit log and memtable on a quorum of replica nodes.
    /// Provides strong consistency if you can tolerate some level of failure.
    Quorum,
    /// A write must be written to the commit log and memtable on all replica nodes in the cluster
    /// for that partition key.
    /// Provides the highest consistency and the lowest availability of any other level.
    All,
    /// Strong consistency. A write must be written to the commit log and memtable on a quorum
    /// of replica nodes in the same data center as thecoordinator node.
    /// Avoids latency of inter-data center communication.
    /// Used in multiple data center clusters with a rack-aware replica placement strategy,
    /// such as NetworkTopologyStrategy, and a properly configured snitch.
    /// Use to maintain consistency locally (within the single data center).
    /// Can be used with SimpleStrategy.
    LocalQuorum,
    /// Strong consistency. A write must be written to the commit log and memtable on a quorum of
    /// replica nodes in all data center.
    /// Used in multiple data center clusters to strictly maintain consistency at the same level
    /// in each data center. For example, choose this level
    /// if you want a read to fail when a data center is down and the QUORUM
    /// cannot be reached on that data center.
    EachQuorum,
    /// Achieves linearizable consistency for lightweight transactions by preventing unconditional
    /// updates. You cannot configure this level as a normal consistency level,
    /// configured at the driver level using the consistency level field.
    /// You configure this level using the serial consistency field
    /// as part of the native protocol operation. See failure scenarios.
    Serial,
    /// Same as SERIAL but confined to the data center. A write must be written conditionally
    /// to the commit log and memtable on a quorum of replica nodes in the same data center.
    /// Same as SERIAL. Used for disaster recovery. See failure scenarios.
    LocalSerial,
    /// A write must be sent to, and successfully acknowledged by,
    /// at least one replica node in the local data center.
    /// In a multiple data center clusters, a consistency level of ONE is often desirable,
    /// but cross-DC traffic is not. LOCAL_ONE accomplishes this.
    /// For security and quality reasons, you can use this consistency level
    /// in an offline datacenter to prevent automatic connection
    /// to online nodes in other data centers if an offline node goes down.
    LocalOne,
}

impl FromStr for Consistency {
    type Err = error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let consistency = match s {
            "Any" => Consistency::Any,
            "One" => Consistency::One,
            "Two" => Consistency::Two,
            "Three" => Consistency::Three,
            "Quorum" => Consistency::Quorum,
            "All" => Consistency::All,
            "LocalQuorum" => Consistency::LocalQuorum,
            "EachQuorum" => Consistency::EachQuorum,
            "Serial" => Consistency::Serial,
            "LocalSerial" => Consistency::LocalSerial,
            "LocalOne" => Consistency::LocalOne,
            _ => {
                return Err(error::Error::General(format!(
                    "Invalid consistency provided: {s}"
                )))
            }
        };

        Ok(consistency)
    }
}

impl Serialize for Consistency {
    fn serialize(&self, cursor: &mut io::Cursor<&mut Vec<u8>>, version: Version) {
        let value: i16 = (*self).into();
        value.serialize(cursor, version)
    }
}

impl TryFrom<CIntShort> for Consistency {
    type Error = error::Error;

    fn try_from(value: CIntShort) -> Result<Self, Self::Error> {
        match value {
            0x0000 => Ok(Consistency::Any),
            0x0001 => Ok(Consistency::One),
            0x0002 => Ok(Consistency::Two),
            0x0003 => Ok(Consistency::Three),
            0x0004 => Ok(Consistency::Quorum),
            0x0005 => Ok(Consistency::All),
            0x0006 => Ok(Consistency::LocalQuorum),
            0x0007 => Ok(Consistency::EachQuorum),
            0x0008 => Ok(Consistency::Serial),
            0x0009 => Ok(Consistency::LocalSerial),
            0x000A => Ok(Consistency::LocalOne),
            _ => Err(Self::Error::UnknownConsistency(value)),
        }
    }
}

impl From<Consistency> for CIntShort {
    fn from(value: Consistency) -> Self {
        match value {
            Consistency::Any => 0x0000,
            Consistency::One => 0x0001,
            Consistency::Two => 0x0002,
            Consistency::Three => 0x0003,
            Consistency::Quorum => 0x0004,
            Consistency::All => 0x0005,
            Consistency::LocalQuorum => 0x0006,
            Consistency::EachQuorum => 0x0007,
            Consistency::Serial => 0x0008,
            Consistency::LocalSerial => 0x0009,
            Consistency::LocalOne => 0x000A,
        }
    }
}

impl FromBytes for Consistency {
    fn from_bytes(bytes: &[u8]) -> error::Result<Consistency> {
        try_i16_from_bytes(bytes)
            .map_err(Into::into)
            .and_then(TryInto::try_into)
    }
}

impl FromCursor for Consistency {
    fn from_cursor(cursor: &mut io::Cursor<&[u8]>, version: Version) -> error::Result<Consistency> {
        CIntShort::from_cursor(cursor, version).and_then(TryInto::try_into)
    }
}

impl Consistency {
    /// Does this consistency require local dc.
    #[inline]
    pub fn is_dc_local(self) -> bool {
        matches!(
            self,
            Consistency::LocalOne | Consistency::LocalQuorum | Consistency::LocalSerial
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::{FromBytes, FromCursor};
    use std::io::Cursor;

    #[test]
    fn test_consistency_serialize() {
        assert_eq!(Consistency::Any.serialize_to_vec(Version::V4), &[0, 0]);
        assert_eq!(Consistency::One.serialize_to_vec(Version::V4), &[0, 1]);
        assert_eq!(Consistency::Two.serialize_to_vec(Version::V4), &[0, 2]);
        assert_eq!(Consistency::Three.serialize_to_vec(Version::V4), &[0, 3]);
        assert_eq!(Consistency::Quorum.serialize_to_vec(Version::V4), &[0, 4]);
        assert_eq!(Consistency::All.serialize_to_vec(Version::V4), &[0, 5]);
        assert_eq!(
            Consistency::LocalQuorum.serialize_to_vec(Version::V4),
            &[0, 6]
        );
        assert_eq!(
            Consistency::EachQuorum.serialize_to_vec(Version::V4),
            &[0, 7]
        );
        assert_eq!(Consistency::Serial.serialize_to_vec(Version::V4), &[0, 8]);
        assert_eq!(
            Consistency::LocalSerial.serialize_to_vec(Version::V4),
            &[0, 9]
        );
        assert_eq!(
            Consistency::LocalOne.serialize_to_vec(Version::V4),
            &[0, 10]
        );
    }

    #[test]
    fn test_consistency_from() {
        assert_eq!(Consistency::try_from(0).unwrap(), Consistency::Any);
        assert_eq!(Consistency::try_from(1).unwrap(), Consistency::One);
        assert_eq!(Consistency::try_from(2).unwrap(), Consistency::Two);
        assert_eq!(Consistency::try_from(3).unwrap(), Consistency::Three);
        assert_eq!(Consistency::try_from(4).unwrap(), Consistency::Quorum);
        assert_eq!(Consistency::try_from(5).unwrap(), Consistency::All);
        assert_eq!(Consistency::try_from(6).unwrap(), Consistency::LocalQuorum);
        assert_eq!(Consistency::try_from(7).unwrap(), Consistency::EachQuorum);
        assert_eq!(Consistency::try_from(8).unwrap(), Consistency::Serial);
        assert_eq!(Consistency::try_from(9).unwrap(), Consistency::LocalSerial);
        assert_eq!(Consistency::try_from(10).unwrap(), Consistency::LocalOne);
    }

    #[test]
    fn test_consistency_from_bytes() {
        assert_eq!(Consistency::from_bytes(&[0, 0]).unwrap(), Consistency::Any);
        assert_eq!(Consistency::from_bytes(&[0, 1]).unwrap(), Consistency::One);
        assert_eq!(Consistency::from_bytes(&[0, 2]).unwrap(), Consistency::Two);
        assert_eq!(
            Consistency::from_bytes(&[0, 3]).unwrap(),
            Consistency::Three
        );
        assert_eq!(
            Consistency::from_bytes(&[0, 4]).unwrap(),
            Consistency::Quorum
        );
        assert_eq!(Consistency::from_bytes(&[0, 5]).unwrap(), Consistency::All);
        assert_eq!(
            Consistency::from_bytes(&[0, 6]).unwrap(),
            Consistency::LocalQuorum
        );
        assert_eq!(
            Consistency::from_bytes(&[0, 7]).unwrap(),
            Consistency::EachQuorum
        );
        assert_eq!(
            Consistency::from_bytes(&[0, 8]).unwrap(),
            Consistency::Serial
        );
        assert_eq!(
            Consistency::from_bytes(&[0, 9]).unwrap(),
            Consistency::LocalSerial
        );
        assert_eq!(
            Consistency::from_bytes(&[0, 10]).unwrap(),
            Consistency::LocalOne
        );
        assert!(Consistency::from_bytes(&[0, 11]).is_err());
    }

    #[test]
    fn test_consistency_from_cursor() {
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 0]), Version::V4).unwrap(),
            Consistency::Any
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 1]), Version::V4).unwrap(),
            Consistency::One
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 2]), Version::V4).unwrap(),
            Consistency::Two
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 3]), Version::V4).unwrap(),
            Consistency::Three
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 4]), Version::V4).unwrap(),
            Consistency::Quorum
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 5]), Version::V4).unwrap(),
            Consistency::All
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 6]), Version::V4).unwrap(),
            Consistency::LocalQuorum
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 7]), Version::V4).unwrap(),
            Consistency::EachQuorum
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 8]), Version::V4).unwrap(),
            Consistency::Serial
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 9]), Version::V4).unwrap(),
            Consistency::LocalSerial
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 10]), Version::V4).unwrap(),
            Consistency::LocalOne
        );
    }
}
