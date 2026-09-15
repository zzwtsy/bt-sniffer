use super::*;
use Source::{Announce, Dht};

fn address(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

#[test]
fn ready_dht_follows_two_hint_entries_without_early_polling() {
    for (hints, expected) in [
        (vec![], vec![(4, Dht), (5, Dht)]),
        (vec![1], vec![(1, Announce), (4, Dht), (5, Dht)]),
        (
            vec![1, 2],
            vec![(1, Announce), (2, Announce), (4, Dht), (5, Dht)],
        ),
        (
            vec![1, 2, 3],
            vec![
                (1, Announce),
                (2, Announce),
                (4, Dht),
                (5, Dht),
                (3, Announce),
            ],
        ),
    ] {
        let priority_slots = hints.len().min(2);
        let mut selector = CandidateSelector::new(hints.into_iter().map(address));
        let mut dht = VecDeque::from([address(4), address(5)]);
        let mut polls = 0;
        for (n, (port, source)) in expected.into_iter().enumerate() {
            let candidate = selector
                .next_ready(|| {
                    polls += 1;
                    dht.pop_front()
                })
                .unwrap();
            assert_eq!(
                (candidate.address, candidate.source),
                (address(port), source)
            );
            assert_eq!(polls, (n + 1).saturating_sub(priority_slots));
            assert_eq!(selector.selected_count(), 0, "取出不等于登记");
        }
        assert!(selector.next_ready(|| dht.pop_front()).is_none());
    }
}

#[test]
fn duplicate_hint_occupies_priority_slot_and_cross_source_keeps_first_selection() {
    let mut selector = CandidateSelector::new([address(1), address(1), address(2)]);
    let mut dht = VecDeque::from([address(3), address(1)]);
    let mut polls = 0;
    let mut accepted = Vec::new();
    for (port, source, fresh, count, reads) in [
        (1, Announce, true, 1, 0),
        (1, Announce, false, 1, 0),
        (3, Dht, true, 2, 1),
        (1, Dht, false, 2, 2),
        (2, Announce, true, 3, 3),
    ] {
        let candidate = selector
            .next_ready(|| {
                polls += 1;
                dht.pop_front()
            })
            .unwrap();
        assert_eq!(
            (candidate.address, candidate.source),
            (address(port), source)
        );
        assert_eq!(selector.select_once(candidate.address), fresh);
        if fresh {
            accepted.push((candidate.address, candidate.source));
        }
        assert_eq!(selector.selected_count(), count);
        assert_eq!(polls, reads);
    }
    assert_eq!(
        accepted,
        vec![
            (address(1), Announce),
            (address(3), Dht),
            (address(2), Announce)
        ]
    );
}

#[test]
fn empty_dht_uses_remaining_hint_and_later_arrival_is_not_exhaustion() {
    let mut selector = CandidateSelector::new([address(1), address(2), address(3)]);
    for port in [1, 2] {
        assert_eq!(
            selector
                .next_ready(|| panic!("优先提示期间不得读取 DHT"))
                .unwrap()
                .address,
            address(port)
        );
    }
    let candidate = selector.next_ready(|| None).unwrap();
    assert_eq!(
        (candidate.address, candidate.source),
        (address(3), Announce)
    );
    assert!(selector.next_ready(|| None).is_none());
    let candidate = selector.next_ready(|| Some(address(4))).unwrap();
    assert_eq!((candidate.address, candidate.source), (address(4), Dht));
}

#[test]
fn selection_is_per_round_and_uses_full_socket_address() {
    let mut selector = CandidateSelector::new([]);
    for addr in [address(1), address(2), "[::1]:1".parse().unwrap()] {
        assert!(selector.select_once(addr));
        assert!(!selector.select_once(addr));
    }
    assert_eq!(selector.selected_count(), 3);
    let mut next_round = CandidateSelector::new([]);
    assert_eq!(next_round.selected_count(), 0);
    assert!(next_round.select_once(address(1)));
}
