use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use crate::suite::{Outcome, Suite};

const DETAIL_LINES: usize = 20;

pub struct KnownFailures {
    cases: BTreeSet<String>,
}

pub struct Report {
    outcomes: Vec<(Status, Outcome)>,
    stale: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Pass,
    Known,
    Regressed,
}

impl KnownFailures {
    pub fn load(suite: Suite) -> anyhow::Result<Self> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("known")
            .join(format!("{}.txt", suite.name()));

        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error.into()),
        };

        let cases = text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect();

        Ok(Self { cases })
    }

    pub fn classify(&self, outcomes: Vec<Outcome>) -> Report {
        let failed: BTreeSet<&str> = outcomes
            .iter()
            .filter(|o| !o.passed)
            .map(|o| o.case.as_str())
            .collect();

        let stale = self
            .cases
            .iter()
            .filter(|case| !failed.contains(case.as_str()))
            .cloned()
            .collect();

        let outcomes = outcomes
            .into_iter()
            .map(|outcome| {
                let status = match (outcome.passed, self.cases.contains(&outcome.case)) {
                    (true, _) => Status::Pass,
                    (false, true) => Status::Known,
                    (false, false) => Status::Regressed,
                };

                (status, outcome)
            })
            .collect();

        Report { outcomes, stale }
    }
}

impl Report {
    pub fn passed(&self) -> bool {
        self.stale.is_empty() && self.count(Status::Regressed) == 0
    }

    fn count(&self, status: Status) -> usize {
        self.outcomes.iter().filter(|(s, _)| *s == status).count()
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (status, outcome) in &self.outcomes {
            let label = match status {
                Status::Pass => "pass ",
                Status::Known => "known",
                Status::Regressed => "FAIL ",
            };

            writeln!(f, "{label} {}", outcome.case)?;

            if let (Status::Regressed, Some(detail)) = (status, &outcome.detail) {
                for line in detail.lines().take(DETAIL_LINES) {
                    writeln!(f, "      {line}")?;
                }
            }
        }

        for case in &self.stale {
            writeln!(f, "STALE {case}")?;
        }

        writeln!(
            f,
            "{} passed, {} known failures, {} regressed, {} stale",
            self.count(Status::Pass),
            self.count(Status::Known),
            self.count(Status::Regressed),
            self.stale.len()
        )
    }
}
