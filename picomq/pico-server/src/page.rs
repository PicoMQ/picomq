const BUDGET: usize = 10;

pub fn fetch_count(limit: usize) -> usize {
    limit.saturating_mul(BUDGET).saturating_add(1)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<K, T> {
    pub items: Vec<(K, T)>,
    pub next: Option<K>,
}

impl<K: Clone, T> Page<K, T> {
    pub fn scan(
        entries: impl IntoIterator<Item = (K, T)>,
        limit: usize,
        mut keep: impl FnMut(&K, &T) -> bool,
    ) -> Self {
        let mut entries = entries.into_iter().peekable();
        let mut items = Vec::new();
        let mut last = None;

        for _ in 0..limit.saturating_mul(BUDGET) {
            if items.len() == limit {
                break;
            }
            let Some((key, item)) = entries.next() else {
                return Self { items, next: None };
            };

            last = Some(key.clone());
            if keep(&key, &item) {
                items.push((key, item));
            }
        }

        let next = entries.peek().and(last);
        Self { items, next }
    }

    pub fn truncate(&mut self, len: usize) {
        if len >= self.items.len() {
            return;
        }

        self.items.truncate(len);
        self.next = self.items.last().map(|(key, _)| key.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numbered(count: u32) -> Vec<(u32, u32)> {
        (0..count).map(|n| (n, n)).collect()
    }

    #[test]
    fn a_full_page_resumes_after_its_last_item() {
        let page = Page::scan(numbered(10), 3, |_, _| true);

        assert_eq!(page.items, [(0, 0), (1, 1), (2, 2)]);
        assert_eq!(page.next, Some(2));
    }

    #[test]
    fn the_last_page_has_no_next() {
        assert_eq!(Page::scan(numbered(3), 3, |_, _| true).next, None);
        assert_eq!(Page::scan(numbered(2), 3, |_, _| true).next, None);
        assert_eq!(Page::scan(numbered(0), 3, |_, _| true).next, None);
    }

    #[test]
    fn skipped_entries_spend_the_budget_and_leave_a_short_page() {
        let page = Page::scan(numbered(1_000), 2, |key, _| *key >= 900);

        assert!(page.items.is_empty());
        assert_eq!(page.next, Some(19));
    }

    #[test]
    fn a_filtered_scan_resumes_where_it_stopped() {
        let entries = numbered(100);
        let mut after = None;
        let mut seen = Vec::new();

        loop {
            let rest = entries
                .iter()
                .copied()
                .filter(|(key, _)| after.is_none_or(|after| *key > after));
            let page = Page::scan(rest, 2, |key, _| key % 7 == 0);
            seen.extend(page.items.iter().map(|(key, _)| *key));
            match page.next {
                Some(next) => after = Some(next),
                None => break,
            }
        }

        assert_eq!(seen, (0..100).filter(|n| n % 7 == 0).collect::<Vec<_>>());
    }

    #[test]
    fn the_fetch_count_covers_the_budget_and_one_to_spare() {
        assert_eq!(fetch_count(100), 1_001);
    }

    #[test]
    fn truncating_resumes_after_the_last_kept_item() {
        let mut page = Page::scan(numbered(10), 5, |_, _| true);
        page.truncate(2);

        assert_eq!(page.items, [(0, 0), (1, 1)]);
        assert_eq!(page.next, Some(1));

        let mut whole = Page::scan(numbered(2), 5, |_, _| true);
        whole.truncate(5);
        assert_eq!(whole.next, None);
    }
}
