use std::io::Cursor;

/// Returns the identifier in a format appropriate for concatenation in a CQL query.
#[inline]
pub fn quote(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

/// Limits the pre-allocation for `count` elements read from the wire to the number of elements
/// the unread bytes could hold, given that each element occupies at least `min_element_len`
/// bytes. A corrupt count then fails on missing data instead of forcing a huge allocation.
pub(crate) fn bounded_capacity(
    cursor: &Cursor<&[u8]>,
    count: usize,
    min_element_len: usize,
) -> usize {
    let remaining = (cursor.get_ref().len() as u64).saturating_sub(cursor.position()) as usize;
    count.min(remaining / min_element_len.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_capacity_is_limited_by_unread_bytes() {
        let bytes = [0u8; 10];
        let mut cursor: Cursor<&[u8]> = Cursor::new(&bytes);
        cursor.set_position(2);

        assert_eq!(bounded_capacity(&cursor, 3, 2), 3);
        assert_eq!(bounded_capacity(&cursor, 100, 2), 4);
        assert_eq!(bounded_capacity(&cursor, 100, 0), 8);

        cursor.set_position(20);
        assert_eq!(bounded_capacity(&cursor, 100, 1), 0);
    }
}
