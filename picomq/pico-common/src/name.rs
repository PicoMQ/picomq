pub fn canonical(spelling: &str) -> String {
    spelling
        .strip_prefix('/')
        .unwrap_or(spelling)
        .replace('/', ".")
}

fn canonical_chars(spelling: &str) -> impl Iterator<Item = char> + '_ {
    spelling
        .strip_prefix('/')
        .unwrap_or(spelling)
        .chars()
        .map(|c| if c == '/' { '.' } else { c })
}

pub fn canonical_eq(a: &str, b: &str) -> bool {
    canonical_chars(a).eq(canonical_chars(b))
}

pub fn canonical_starts_with(spelling: &str, prefix: &str) -> bool {
    let mut name = canonical_chars(spelling);

    canonical_chars(prefix).all(|expected| name.next() == Some(expected))
}

pub fn strip_canonical_prefix<'a>(spelling: &'a str, prefix: &str) -> Option<&'a str> {
    let prefix = canonical(prefix);
    let offset = usize::from(spelling.starts_with('/'));

    canonical(spelling)
        .starts_with(&prefix)
        .then(|| &spelling[offset + prefix.len()..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slashes_and_dots_share_one_form() {
        assert_eq!(canonical("/a/b"), "a.b");
        assert_eq!(canonical("/a.b"), "a.b");
        assert_eq!(canonical("a.b"), "a.b");
        assert_eq!(canonical("/"), "");
    }

    #[test]
    fn comparisons_see_one_form_without_building_it() {
        assert!(canonical_eq("/a/b", "a.b"));
        assert!(canonical_eq("/a.b", "/a/b"));
        assert!(!canonical_eq("/a/b", "/a/c"));
        assert!(canonical_starts_with("/acct/orders", "/acct/"));
        assert!(canonical_starts_with("/acct.orders", "acct."));
        assert!(canonical_starts_with("/anything", ""));
        assert!(!canonical_starts_with("/acct", "/acct/"));
        assert!(!canonical_starts_with("/other/orders", "/acct/"));
    }

    #[test]
    fn prefixes_strip_from_either_spelling() {
        assert_eq!(
            strip_canonical_prefix("/acct/orders", "/acct/"),
            Some("orders")
        );
        assert_eq!(
            strip_canonical_prefix("/acct.orders", "/acct/"),
            Some("orders")
        );
        assert_eq!(strip_canonical_prefix("/other/orders", "/acct/"), None);
    }
}
