#[derive(Debug, Clone, Copy, Default)]
struct Slot {
    generation: Option<u64>,
    end: u64,
    pending: u32,
}

#[derive(Debug)]
pub(crate) struct Ring {
    segment_bytes: u64,
    slots: Vec<Slot>,
}

impl Ring {
    pub(crate) fn new(segment_bytes: u64, count: usize) -> Self {
        Self {
            segment_bytes,
            slots: vec![Slot::default(); count],
        }
    }

    pub(crate) fn slot(&self, offset: u64) -> usize {
        (self.generation(offset) % self.slots.len() as u64) as usize
    }

    pub(crate) fn open(&mut self, start: u64) -> bool {
        let generation = self.generation(start);
        let index = self.slot(start);
        let slot = &mut self.slots[index];
        if slot.generation.is_some_and(|held| held != generation) {
            return false;
        }
        slot.generation = Some(generation);
        slot.pending += 1;
        slot.end = slot.end.max(start);
        true
    }

    pub(crate) fn settle(&mut self, start: u64, end: u64) {
        let index = self.slot(start);
        let slot = &mut self.slots[index];
        slot.pending = slot.pending.saturating_sub(1);
        slot.end = slot.end.max(end);
    }

    pub(crate) fn restore(&mut self, index: usize, start: u64, end: u64) -> bool {
        if self.slot(start) != index {
            return false;
        }
        self.slots[index] = Slot {
            generation: Some(self.generation(start)),
            end,
            pending: 0,
        };
        true
    }

    pub(crate) fn reclaimable(&self, watermark: u64, frontier: u64) -> Vec<usize> {
        let current = self.generation(frontier);
        self.slots
            .iter()
            .enumerate()
            .filter(|(_, slot)| {
                slot.generation
                    .is_some_and(|generation| generation < current)
                    && slot.pending == 0
                    && slot.end <= watermark
            })
            .map(|(index, _)| index)
            .collect()
    }

    pub(crate) fn release(&mut self, index: usize) {
        self.slots[index] = Slot::default();
    }

    fn generation(&self, offset: u64) -> u64 {
        offset / self.segment_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_generations_onto_slots() {
        let ring = Ring::new(100, 3);
        assert_eq!(ring.slot(0), 0);
        assert_eq!(ring.slot(99), 0);
        assert_eq!(ring.slot(100), 1);
        assert_eq!(ring.slot(300), 0);
    }

    #[test]
    fn refuses_a_slot_still_holding_an_older_generation() {
        let mut ring = Ring::new(100, 2);
        assert!(ring.open(0));
        ring.settle(0, 50);
        assert!(ring.open(150));
        assert!(!ring.open(200));
        ring.release(0);
        assert!(ring.open(200));
    }

    #[test]
    fn reclaims_only_trimmed_settled_past_generations() {
        let mut ring = Ring::new(100, 3);
        assert!(ring.open(0));
        ring.settle(0, 120);
        assert!(ring.open(120));
        assert_eq!(ring.reclaimable(120, 120), vec![0]);
        assert!(ring.reclaimable(119, 120).is_empty());
        assert!(ring.reclaimable(120, 99).is_empty());
        assert_eq!(ring.reclaimable(200, 200), vec![0]);
        ring.settle(120, 200);
        assert_eq!(ring.reclaimable(200, 200), vec![0, 1]);
    }

    #[test]
    fn restore_rejects_a_misplaced_slot() {
        let mut ring = Ring::new(100, 2);
        assert!(!ring.restore(1, 0, 10));
        assert!(ring.restore(1, 100, 180));
        assert_eq!(ring.reclaimable(180, 200), vec![1]);
    }
}
