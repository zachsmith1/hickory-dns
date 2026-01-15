//! SOA serial computation helpers for replicated zones.
//!
//! Many operators prefer a date-based SOA serial (e.g. `YYYYMMDDNN`), but correctness requires
//! monotonicity even under clock skew, retries, and replays. The hybrid rule implemented here:
//!
//! ```text
//! serial = max(prev_serial + 1, yyyymmddNN)
//! ```
//!
//! where `yyyymmddNN` is `YYYYMMDD01` for the first update of the day.

use time::Date;

/// Compute a "date-based but monotonic" next SOA serial.
///
/// - `prev_serial`: the previously served SOA serial for the zone.
/// - `today`: the writer's notion of the current UTC date for serial formatting.
///
/// This function guarantees `next > prev_serial`.
pub fn next_hybrid_soa_serial(prev_serial: u32, today: Date) -> u32 {
    let base = yyyymmdd_base(today);
    // "01" is the first serial for a given date.
    let date_serial = base.saturating_add(1);

    // Preserve monotonicity even if the date-based value goes backwards (clock skew/replay).
    (prev_serial.saturating_add(1)).max(date_serial)
}

fn yyyymmdd_base(today: Date) -> u32 {
    let year: i32 = today.year();
    let month: u8 = today.month() as u8;
    let day: u8 = today.day();

    // YYYYMMDD00 fits in u32 for reasonable years.
    let y = (year as u32).saturating_mul(10_000);
    let m = (month as u32).saturating_mul(100);
    let d = day as u32;
    (y + m + d).saturating_mul(100)
}

#[cfg(test)]
mod tests {
    use super::next_hybrid_soa_serial;
    use time::{Date, Month};

    #[test]
    fn first_update_of_day_starts_at_yyyymmdd01() {
        let d = Date::from_calendar_date(2026, Month::January, 14).unwrap();
        assert_eq!(next_hybrid_soa_serial(0, d), 2026011401);
    }

    #[test]
    fn multiple_updates_same_day_increment_monotonically() {
        let d = Date::from_calendar_date(2026, Month::January, 14).unwrap();
        let s1 = next_hybrid_soa_serial(2026011401, d);
        let s2 = next_hybrid_soa_serial(s1, d);
        assert_eq!(s1, 2026011402);
        assert_eq!(s2, 2026011403);
    }

    #[test]
    fn clock_skew_backwards_still_increments_prev_plus_one() {
        let jan14 = Date::from_calendar_date(2026, Month::January, 14).unwrap();
        let jan13 = Date::from_calendar_date(2026, Month::January, 13).unwrap();

        let prev = next_hybrid_soa_serial(0, jan14);
        // "today" appears to be earlier than when prev was minted
        let next = next_hybrid_soa_serial(prev, jan13);
        assert_eq!(next, prev + 1);
    }
}

