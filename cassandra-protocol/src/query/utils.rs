use crate::frame::Flags;

#[inline]
pub fn prepare_flags(with_tracing: bool, with_warnings: bool) -> Flags {
    let mut flags = Flags::empty();

    if with_tracing {
        flags.insert(Flags::TRACING);
    }

    if with_warnings {
        flags.insert(Flags::WARNING);
    }

    flags
}

/// Returns the identifier in a format appropriate for concatenation in a CQL query.
#[inline]
pub fn quote(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn prepare_flags_test() {
        assert!(prepare_flags(true, false).contains(Flags::TRACING));
        assert!(prepare_flags(false, true).contains(Flags::WARNING));

        let both = prepare_flags(true, true);
        assert!(both.contains(Flags::TRACING));
        assert!(both.contains(Flags::WARNING));
    }
}
