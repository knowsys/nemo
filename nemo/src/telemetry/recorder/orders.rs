//! What the recorder gathers about semi-naive variants, compared with their rules'
//! variable orders.

use crate::telemetry::NamedCount;

/// Statistics of semi-naive variants, compared with their rules' variable orders.
///
/// They count how often the delta atom was a leading atom.
/// If it was not, they compare the rows the delta atom read with those of the smallest
/// leading atom.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct VariantOrderStats {
    /// Variants observed.
    pub(super) variants: u64,
    /// Variants whose delta atom was a leading atom.
    delta_led: u64,
    /// Variants whose delta atom read no rows.
    ///
    /// The join stops before reading any other atom, so the order cost nothing.
    delta_empty: u64,
    /// Variants whose delta atom was not a leading atom
    /// and read fewer rows than the smallest leading atom.
    delta_smaller: u64,
    /// Of those, variants whose smallest leading atom read at least four times
    /// as many rows as the delta atom.
    delta_much_smaller: u64,
    /// The rows the smallest leading atoms read in total,
    /// over the variants whose delta atom read rows but was not a leading atom.
    led_rows: u64,
    /// The rows the delta atoms read in total, over the same variants.
    delta_rows: u64,
}

impl VariantOrderStats {
    /// Accounts one variant.
    pub(super) fn observe(&mut self, delta_rows: u64, leading_rows: u64, delta_leads: bool) {
        self.variants += 1;
        if delta_leads {
            self.delta_led += 1;
            return;
        }
        if delta_rows == 0 {
            self.delta_empty += 1;
            return;
        }
        self.led_rows += leading_rows;
        self.delta_rows += delta_rows;
        if delta_rows < leading_rows {
            self.delta_smaller += 1;
            if leading_rows >= delta_rows.saturating_mul(4) {
                self.delta_much_smaller += 1;
            }
        }
    }

    /// Returns the statistics as the snapshot keys them, in order.
    pub(super) fn rows(&self) -> [NamedCount; 7] {
        [
            ("variants", self.variants),
            ("deltaLed", self.delta_led),
            ("deltaEmpty", self.delta_empty),
            ("deltaSmaller", self.delta_smaller),
            ("deltaMuchSmaller", self.delta_much_smaller),
            ("ledRows", self.led_rows),
            ("deltaRows", self.delta_rows),
        ]
    }
}
