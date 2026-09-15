use super::*;
use ClaimClass::{Hint, History, Recent, Retry};

#[test]
fn two_rotations_and_repeated_reads_preserve_slots() {
    let mut policy = ClaimPolicy::new();
    for preferred in [Hint, Recent, Retry, Hint, History, Recent, Retry, Hint].repeat(2) {
        assert_eq!(policy.order()[0], preferred);
        assert_eq!(policy.order()[0], preferred);
        policy.on_claimed();
    }
    assert_eq!(policy.order()[0], Hint);
}

#[test]
fn preferred_and_borrow_orders_are_complete_and_unique() {
    for expected in [
        [Hint, Recent, History, Retry],
        [Recent, Hint, History, Retry],
        [Retry, Hint, Recent, History],
        [History, Hint, Recent, Retry],
    ] {
        let order = order_for(expected[0]);
        assert_eq!(order, expected);
        assert_eq!(
            order
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            4
        );
    }
}
