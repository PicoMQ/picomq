use std::collections::BTreeMap;

pub fn parse(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .filter_map(|line| {
            let (api, range) = line.trim().trim_end_matches(',').split_once("): ")?;

            let mut words = range.split_whitespace();
            let min = words.next()?.parse::<i16>().ok()?;
            let max = words.nth(1)?.parse::<i16>().ok()?;

            Some(format!("{api}) {min} {max}"))
        })
        .collect()
}

pub fn mismatches(reference: &[String], actual: &[String]) -> Vec<String> {
    if actual.is_empty() {
        return vec!["no api versions advertised".to_owned()];
    }

    let ranges: BTreeMap<&str, (i16, i16)> = reference.iter().filter_map(|l| split(l)).collect();

    actual
        .iter()
        .filter_map(|line| {
            let (api, (min, max)) = split(line)?;

            match ranges.get(api) {
                Some((floor, ceiling)) if *floor == min && max <= *ceiling => None,
                Some((floor, ceiling)) => Some(format!(
                    "{api}: picomq {min}..{max}, kafka {floor}..{ceiling}"
                )),
                None => Some(format!(
                    "{api}: picomq {min}..{max}, kafka does not advertise it"
                )),
            }
        })
        .collect()
}

fn split(line: &str) -> Option<(&str, (i16, i16))> {
    let mut words = line.rsplitn(3, ' ');
    let max = words.next()?.parse().ok()?;
    let min = words.next()?.parse().ok()?;

    Some((words.next()?, (min, max)))
}
