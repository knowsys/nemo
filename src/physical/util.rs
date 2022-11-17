//! This module collects miscellaneous functionality.

/// Module for utility functions used in tests
#[cfg(test)]
pub mod test_util;
use std::ops::Range;

#[cfg(test)]
pub use test_util::make_gic;
#[cfg(test)]
pub use test_util::make_gict;

/// A macro that generates forwarding macros to dispatch along
/// datatype-tagged enums.
///
/// `$name` is the name of the generated forwarder, and `$variant`
/// takes the enum variants to dispatch to.
///
/// # Example
///
/// ```ignore
/// generate_forwarder!(forward_to_scan; IntervalTrieScan, TrieScanJoin);
/// ```
///
/// will generate a forwarder called `forward_to_scan` that dispatches
/// to variants `Self::IntervalTrieScan` and `Self::TrieScanJoin`,
/// which can be used as follows:
///
/// ```ignore
/// fn up(&mut self) {
///     forward_to_scan!(self, up)
/// }
///
/// /// The following will map the return value of the forwarded
/// /// `returns()` call to the appropriate variant of [`DataValueT`]:
/// fn returns(&mut self) -> DataValueT {
///     forward_to_scan!(self, returns.map_to(DataValueT))
/// }
///
/// /// The return value can be suppressed by adding a semicolon, this
/// /// is useful if the return type would differ between match arms:
/// fn doesnt_return(&mut self) {
///     forward_to_scan!(self, returns;)
/// }
/// ```
#[macro_export]
macro_rules! generate_forwarder {
    ($name:ident; $( $variant:ident ),*) => {
        macro_rules! $name {
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?) => {
                match $$self {
                    $( Self::$variant(value) => value.$$func($$($$($$arg),*)?) ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?;) => {
                match $$self {
                    $( Self::$variant(value) => { value.$$func($$($$($$arg),*)?); } ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?.map_to($$enum:ident) ) => {
                match $$self {
                    $( Self::$variant(value) => value.$$func($$($$($$arg),*)?).map($$enum::$variant) ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?.wrap_with($$wrap:path) ) => {
                match $$self {
                    $( Self::$variant(value) => $$wrap(value.$$func($$($$($$arg),*)?)) ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?.as_variant_of($$enum:ident) ) => {
                match $$self {
                    $( Self::$variant(value) => $$enum::$variant(value.$$func($$($$($$arg),*)?)) ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?.wrap_with($$wrap:path).as_variant_of($$enum:ident) ) => {
                match $$self {
                    $( Self::$variant(value) => $$enum::$variant($$wrap(value.$$func($$($$($$arg),*)?))) ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?.as_variant_of($$enum:ident).wrap_with($$wrap:path) ) => {
                match $$self {
                    $( Self::$variant(value) => $$wrap($$enum::$variant(value.$$func($$($$($$arg),*)?))) ),*
                }
            };
        }
    }
}

/// A specialised version of [`generate_forwarder`] for the possible
/// variants of [`crate::physical::datatypes::data_value::DataValueT`].
#[macro_export]
macro_rules! generate_datatype_forwarder {
    ($name:ident) => {
        $crate::generate_forwarder!($name;
                                    U64,
                                    Float,
                                    Double);
    }
}

/// For a certain interval, find an "exact" covering using the intervals from the given vector
/// Exact means that start and end points of the covering must match the given interval
/// Also, assumes that the vector is sorted by the start points of the intervals
/// TODO: It would be even better if this would return the covering with the minimal intersection...
pub fn cover_interval<'a>(vec: &'a [Range<usize>], target: &Range<usize>) -> Vec<&'a Range<usize>> {
    let mut result = Vec::<&Range<usize>>::new();

    if vec.is_empty() || vec[0].start > target.start {
        return result;
    }

    // Algorithm is inspired by:
    // https://www.geeksforgeeks.org/minimum-number-of-intervals-to-cover-the-target-interval

    // Stores start of current interval
    let mut start = target.start;

    // Stores end of current interval
    let mut end: Option<usize> = None;

    // Stores the interval which is currently considered as part of the overall solution
    let mut promising_interval: Option<&Range<usize>> = None;

    // Iterate over all the intervals
    for interval in vec {
        // Since we want to capture the target interval exactly we cannot use an interval with a smaller start
        if result.is_empty() && interval.start < target.start {
            continue;
        }

        // We don't want intervals that go outisde of the target interval either
        if interval.end > target.end {
            continue;
        }

        // If starting point of current index <= start
        if interval.start > start {
            // As the start point is now not covered, its a good time to add the best interval to result
            if let Some(best_interval) = promising_interval {
                result.push(best_interval);
                promising_interval = None;
            }

            // Update the value of start
            let end = end.expect("Branch at the stop should have happend before");
            start = end - 1; // Note that ranges not not include the end

            // If the target interval is already covered or it is not possible to move then break the loop
            if interval.start > end || end >= target.end {
                break;
            }
        }

        // If the interval increases further to the right then we consider it and update end
        if end.is_none() || interval.end > end.unwrap() {
            promising_interval = Some(interval);
            end = Some(interval.end);
        }
    }

    // If there is still a promising candidate that we didn't add, we do it here
    if let Some(forgotten_interval) = promising_interval {
        result.push(forgotten_interval);
    }

    // If the entire target interval is not reached exactly return empty vector
    if end.is_none() || end.expect("End is not none") != target.end {
        return Vec::new();
    }

    result
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use super::cover_interval;

    #[test]
    fn test_cover() {
        let vec = vec![1..2, 1..4, 2..4, 2..5, 2..11];
        let target = 1..11;

        let correct_result = vec![&vec[1], &vec[4]];
        let answer = cover_interval(&vec, &target);

        assert_eq!(answer, correct_result);

        let target = 1..13;

        let correct_result = Vec::<&Range<usize>>::new();
        let answer = cover_interval(&vec, &target);

        assert_eq!(answer, correct_result);

        let target = 0..11;

        let correct_result = Vec::<&Range<usize>>::new();
        let answer = cover_interval(&vec, &target);

        assert_eq!(answer, correct_result);

        let vec = vec![0..1, 1..2];
        let target = 0..2;

        let correct_result = vec![&vec[0], &vec[1]];
        let answer = cover_interval(&vec, &target);

        assert_eq!(answer, correct_result);

        let vec = vec![1..5, 1..8, 3..7, 3..10, 5..6, 5..12, 5..18, 10..20];
        let target = 1..18;

        let correct_result = vec![&vec[1], &vec[6]];
        let answer = cover_interval(&vec, &target);

        assert_eq!(answer, correct_result);

        let target = 1..20;

        let correct_result = vec![&vec[1], &vec[6], &vec[7]];
        let answer = cover_interval(&vec, &target);

        assert_eq!(answer, correct_result);

        let vec = vec![1..2, 1..6, 2..3, 3..4, 6..7, 7..8, 8..9];
        let target = 1..8;

        let correct_result = vec![&vec[1], &vec[4], &vec[5]];
        let answer = cover_interval(&vec, &target);

        assert_eq!(answer, correct_result);

        let vec = vec![1..2, 1..6, 2..3, 3..8, 6..7, 7..8, 8..9];
        let target = 1..9;

        let correct_result = vec![&vec[1], &vec[3], &vec[6]];
        let answer = cover_interval(&vec, &target);

        assert_eq!(answer, correct_result);

        let vec = vec![1..2, 1..6, 1..12, 2..3, 3..8, 6..7, 7..8, 8..9];
        let target = 1..9;

        let correct_result = vec![&vec[1], &vec[4], &vec[7]];
        let answer = cover_interval(&vec, &target);

        assert_eq!(answer, correct_result);
    }
}
