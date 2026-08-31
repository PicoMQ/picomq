pub fn is_json(mime: &str) -> bool {
    mime == "application/json" || mime.ends_with("+json")
}

pub fn mime_of(content_type: Option<&str>) -> String {
    let Some(ct) = content_type else {
        return String::new();
    };
    let base = ct.split(';').next().unwrap_or("");
    base.trim().to_ascii_lowercase()
}

pub fn mime_equals(a: Option<&str>, b: Option<&str>) -> bool {
    mime_of(a) == mime_of(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mime_helpers() {
        assert!(is_json("application/json"));
        assert!(is_json("application/vnd.foo+json"));
        assert!(!is_json("text/plain"));
        assert_eq!(mime_of(Some("Text/Plain; charset=utf-8")), "text/plain");
        assert_eq!(mime_of(None), "");
        assert!(mime_equals(
            Some("application/json; x=1"),
            Some("APPLICATION/JSON")
        ));
        assert!(!mime_equals(Some("text/plain"), Some("application/json")));
    }
}
