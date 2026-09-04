use picomq_connector_sdk::Error;

#[inline]
pub(crate) fn write_measurement(buf: &mut String, value: &str) -> Result<(), Error> {
    let bytes = value.as_bytes();
    let mut last = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        let escaped = match byte {
            b'\t' => {
                return Err(Error::InvalidConfigValue(
                    "measurement name must not contain tab characters, they are not valid in the InfluxDB line protocol".into(),
                ));
            }
            b'\\' => "\\\\",
            b',' => "\\,",
            b' ' => "\\ ",
            b'\n' => "\\n",
            b'\r' => "\\r",
            _ => continue,
        };
        buf.push_str(&value[last..index]);
        buf.push_str(escaped);
        last = index + 1;
    }
    buf.push_str(&value[last..]);
    Ok(())
}

#[inline]
pub(crate) fn write_tag_value(buf: &mut String, value: &str) -> Result<(), Error> {
    let bytes = value.as_bytes();
    let mut last = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        let escaped = match byte {
            b'\t' => {
                return Err(Error::CannotStoreData(
                    "tag value must not contain tab characters, they are not valid in the InfluxDB line protocol".into(),
                ));
            }
            b'\\' => "\\\\",
            b',' => "\\,",
            b'=' => "\\=",
            b' ' => "\\ ",
            b'\n' => "\\n",
            b'\r' => "\\r",
            _ => continue,
        };
        buf.push_str(&value[last..index]);
        buf.push_str(escaped);
        last = index + 1;
    }
    buf.push_str(&value[last..]);
    Ok(())
}

#[inline]
pub(crate) fn write_field_string(buf: &mut String, value: &str) {
    let bytes = value.as_bytes();
    let mut last = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        let escaped = match byte {
            b'\\' => "\\\\",
            b'"' => "\\\"",
            b'\n' => "\\n",
            b'\r' => "\\r",
            _ => continue,
        };
        buf.push_str(&value[last..index]);
        buf.push_str(escaped);
        last = index + 1;
    }
    buf.push_str(&value[last..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_measurement_with_comma_space_backslash_should_escape() {
        let mut buf = String::new();
        write_measurement(&mut buf, "m\\eas,urea meant").unwrap();
        assert_eq!(buf, "m\\\\eas\\,urea\\ meant");
    }

    #[test]
    fn given_measurement_with_newlines_should_escape() {
        let mut buf = String::new();
        write_measurement(&mut buf, "meas\nurea\rment").unwrap();
        assert_eq!(buf, "meas\\nurea\\rment");
    }

    #[test]
    fn given_tag_value_with_equals_sign_should_escape() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "a=b,c d\\e").unwrap();
        assert_eq!(buf, "a\\=b\\,c\\ d\\\\e");
    }

    #[test]
    fn given_tag_value_with_newlines_should_escape() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "line1\nline2\r").unwrap();
        assert_eq!(buf, "line1\\nline2\\r");
    }

    #[test]
    fn given_field_string_with_quote_and_backslash_should_escape() {
        let mut buf = String::new();
        write_field_string(&mut buf, r#"say "hello" \world\"#);
        assert_eq!(buf, r#"say \"hello\" \\world\\"#);
    }

    #[test]
    fn given_field_string_with_newlines_should_escape() {
        let mut buf = String::new();
        write_field_string(&mut buf, "line1\nline2\r");
        assert_eq!(buf, "line1\\nline2\\r");
    }

    #[test]
    fn given_plain_ascii_measurement_should_stay_unchanged() {
        let mut buf = String::new();
        write_measurement(&mut buf, "cpu_usage").unwrap();
        assert_eq!(buf, "cpu_usage");
    }

    #[test]
    fn given_plain_ascii_tag_value_should_stay_unchanged() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "server01").unwrap();
        assert_eq!(buf, "server01");
    }

    #[test]
    fn given_plain_ascii_field_string_should_stay_unchanged() {
        let mut buf = String::new();
        write_field_string(&mut buf, "hello world");
        assert_eq!(buf, "hello world");
    }

    #[test]
    fn given_empty_measurement_should_produce_empty_output() {
        let mut buf = String::new();
        write_measurement(&mut buf, "").unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn given_empty_tag_value_should_produce_empty_output() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "").unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn given_empty_field_string_should_produce_empty_output() {
        let mut buf = String::new();
        write_field_string(&mut buf, "");
        assert!(buf.is_empty());
    }

    #[test]
    fn given_measurement_with_tab_should_fail() {
        let mut buf = String::new();
        assert!(write_measurement(&mut buf, "m\teasure").is_err());
    }

    #[test]
    fn given_tag_value_with_tab_should_fail() {
        let mut buf = String::new();
        assert!(write_tag_value(&mut buf, "val\tue").is_err());
    }

    #[test]
    fn given_unicode_measurement_should_pass_through() {
        let mut buf = String::new();
        write_measurement(&mut buf, "温度").unwrap();
        assert_eq!(buf, "温度");
    }

    #[test]
    fn given_unicode_tag_value_should_pass_through() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "µ-sensor").unwrap();
        assert_eq!(buf, "µ-sensor");
    }

    #[test]
    fn given_unicode_field_string_should_pass_through() {
        let mut buf = String::new();
        write_field_string(&mut buf, "café");
        assert_eq!(buf, "café");
    }
}
