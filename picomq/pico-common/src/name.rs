pub fn canonical(spelling: &str) -> String {
    spelling
        .strip_prefix('/')
        .unwrap_or(spelling)
        .replace('/', ".")
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
