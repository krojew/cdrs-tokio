/// Returns the identifier in a format appropriate for concatenation in a CQL query.
#[inline]
pub fn quote(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}
