//! Routing table 行为测试。

use super::*;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

/// 构造一个与全零本地 ID 在指定最高不同位上有距离的 Node ID。
fn id_with_distance_bit(index: usize, suffix: u8) -> NodeId {
    assert!(index < 160);
    let mut bytes = [0_u8; 20];
    let byte_index = 19 - index / 8;
    bytes[byte_index] |= 1 << (index % 8);
    bytes[19] |= suffix;
    NodeId(bytes)
}

fn ipv4(last_octet: u8) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(192, 0, 2, last_octet),
        6881,
    ))
}

/// 用固定输入验证 XOR 距离的每个字节，避免排序依据被错误实现掩盖。
#[test]
fn xor_distance_is_calculated_byte_by_byte() {
    assert_eq!(
        xor_distance(&[0b1010_0000; 20], &[0b1100_0000; 20]),
        [0b0110_0000; 20]
    );
}

/// 新表只有一个覆盖完整空间的 bucket；二分后两个范围不能重叠也不能留下空洞。
#[test]
fn bucket_prefix_split_covers_both_halves() {
    let root = BucketPrefix::root();
    let (left, right) = root.split();
    assert!(left.contains(NodeId([0; 20])));
    assert!(!left.contains(NodeId([0x80; 20])));
    assert!(right.contains(NodeId([0x80; 20])));
    assert!(!right.contains(NodeId([0; 20])));
}

/// 验证响应可以新增或更新联系人，并刷新最近响应时间。
#[test]
fn verified_response_inserts_and_updates_a_contact() {
    let now = Instant::now();
    let id = id_with_distance_bit(159, 1);
    let mut table = RoutingTable::new(NodeId([0; 20]), AddressFamily::Ipv4, now);
    assert_eq!(
        table.observe_response(id, ipv4(1), now),
        InsertOutcome::Inserted
    );
    assert_eq!(table.len(), 1);
    let later = now + Duration::from_secs(1);
    assert_eq!(
        table.observe_response(id, ipv4(2), later),
        InsertOutcome::Updated
    );
    assert_eq!(table.len(), 1);
    assert_eq!(table.closest_good(&id.0, 1, later)[0].address, ipv4(2));
}

/// 本机身份和另一地址族不能作为当前路由表的候选邻居。
#[test]
fn self_and_wrong_address_family_are_ignored() {
    let now = Instant::now();
    let local = NodeId([0; 20]);
    let remote = id_with_distance_bit(159, 1);
    let mut table = RoutingTable::new(local, AddressFamily::Ipv4, now);
    assert_eq!(
        table.observe_response(local, ipv4(1), now),
        InsertOutcome::IgnoredSelf
    );
    assert_eq!(
        table.observe_response(
            remote,
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6881, 0, 0)),
            now
        ),
        InsertOutcome::IgnoredWrongAddressFamily
    );
    assert!(table.is_empty());
}

/// 陌生查询者须先验证；只读节点不作为可服务的路由联系人接纳。
#[test]
fn unknown_query_sender_requires_verification_and_read_only_is_ignored() {
    let now = Instant::now();
    let id = id_with_distance_bit(159, 1);
    let mut table = RoutingTable::new(NodeId([0; 20]), AddressFamily::Ipv4, now);
    assert_eq!(
        table.observe_query(id, ipv4(1), false, now),
        QueryObservation::VerificationRequired {
            id,
            address: ipv4(1)
        }
    );
    assert_eq!(
        table.observe_query(id, ipv4(1), true, now),
        QueryObservation::IgnoredReadOnly
    );
    assert!(table.is_empty());
}

/// 已验证节点最近主动查询过我们时，即使旧响应已过期，也仍然属于 good。
#[test]
fn recent_query_keeps_a_previously_verified_node_good() {
    let start = Instant::now();
    let id = id_with_distance_bit(159, 1);
    let mut table = RoutingTable::new(NodeId([0; 20]), AddressFamily::Ipv4, start);
    table.observe_response(id, ipv4(1), start);
    let later = start + GOOD_FOR + Duration::from_secs(1);
    assert!(table.closest_good(&id.0, 1, later).is_empty());
    assert_eq!(
        table.observe_query(id, ipv4(1), false, later),
        QueryObservation::Updated
    );
    assert_eq!(table.closest_good(&id.0, 1, later).len(), 1);
}

/// 满 bucket 只有在覆盖本地 ID 时才二分；远端一半仍然维持八节点上限。
#[test]
fn full_local_bucket_splits_but_remote_bucket_does_not() {
    let now = Instant::now();
    let mut table = RoutingTable::new(NodeId([0; 20]), AddressFamily::Ipv4, now);
    for suffix in 1..=BUCKET_SIZE as u8 {
        assert_eq!(
            table.observe_response(NodeId([0x80 | suffix; 20]), ipv4(suffix), now),
            InsertOutcome::Inserted
        );
    }
    assert_eq!(table.bucket_count(), 1);
    assert_eq!(
        table.observe_response(NodeId([0x90; 20]), ipv4(9), now),
        InsertOutcome::RejectedFull
    );
    assert_eq!(table.bucket_count(), 2);
    assert_eq!(table.bucket_len(1), Some(BUCKET_SIZE));
}

/// 多个 questionable 节点必须按最近活动时间从旧到新交给 dispatcher 探测。
#[test]
fn questionable_nodes_are_returned_in_probe_order() {
    let start = Instant::now();
    let mut table = RoutingTable::new(NodeId([0; 20]), AddressFamily::Ipv4, start);
    for suffix in 1..=BUCKET_SIZE as u8 {
        table.observe_response(
            NodeId([0x80 | suffix; 20]),
            ipv4(suffix),
            start + Duration::from_secs(suffix.into()),
        );
    }
    let later = start + GOOD_FOR + Duration::from_secs(20);
    match table.observe_response(NodeId([0x90; 20]), ipv4(9), later) {
        InsertOutcome::ProbeRequired {
            incumbents,
            candidate,
        } => {
            assert_eq!(incumbents.len(), BUCKET_SIZE);
            assert_eq!(incumbents[0].address, ipv4(1));
            assert_eq!(candidate.id, NodeId([0x90; 20]));
        }
        other => panic!("期望先探测旧节点，实际得到 {other:?}"),
    }
}

/// 坏节点可以被替换，后续成功响应清除失败记录。
#[test]
fn bad_node_is_replaced_and_success_resets_failures() {
    let now = Instant::now();
    let failed = NodeId([0x81; 20]);
    let mut table = RoutingTable::new(NodeId([0; 20]), AddressFamily::Ipv4, now);
    for suffix in 1..=BUCKET_SIZE as u8 {
        table.observe_response(NodeId([0x80 | suffix; 20]), ipv4(suffix), now);
    }
    assert_eq!(table.record_failure(failed, now), Some(NodeStatus::Good));
    table.observe_response(failed, ipv4(1), now + Duration::from_secs(1));
    assert_eq!(
        table.closest_good(&failed.0, 1, now + Duration::from_secs(1))[0].consecutive_failures(),
        0
    );
    table.record_failure(failed, now);
    table.record_failure(failed, now);
    match table.observe_response(NodeId([0x90; 20]), ipv4(9), now) {
        InsertOutcome::ReplacedBad { removed } => assert_eq!(removed.id, failed),
        other => panic!("期望替换 bad 节点，实际得到 {other:?}"),
    }
}

/// questionable 节点可用于恢复 routing table，但 bad 节点必须排除。
#[test]
fn closest_usable_prefers_good_and_excludes_bad() {
    let start = Instant::now();
    let good = NodeId([0x10; 20]);
    let questionable = NodeId([0x20; 20]);
    let bad = NodeId([0x30; 20]);
    let mut table = RoutingTable::new(NodeId([0; 20]), AddressFamily::Ipv4, start);
    table.observe_response(questionable, ipv4(1), start);
    table.observe_response(bad, ipv4(2), start);
    table.record_failure(bad, start);
    table.record_failure(bad, start);
    let later = start + GOOD_FOR + Duration::from_secs(1);
    table.observe_response(good, ipv4(3), later);
    let contacts = table.closest_usable(NodeId([0; 20]), 8, later);
    assert_eq!(
        contacts.iter().map(|node| node.id).collect::<Vec<_>>(),
        vec![good, questionable]
    );
}

/// 到期前不应刷新；完成刷新后，应从完成时刻重新计算十五分钟。
#[test]
fn refresh_deadline_and_random_target_follow_bucket_state() {
    let start = Instant::now();
    let mut table = RoutingTable::new(NodeId([0; 20]), AddressFamily::Ipv4, start);
    assert_eq!(
        table.next_refresh_deadline(DEFAULT_BUCKET_REFRESH_AFTER),
        None
    );
    table.observe_response(NodeId([0x80; 20]), ipv4(1), start);
    let deadline = start + DEFAULT_BUCKET_REFRESH_AFTER;
    assert_eq!(
        table.next_refresh_deadline(DEFAULT_BUCKET_REFRESH_AFTER),
        Some(deadline)
    );
    assert!(
        table
            .stale_refresh_target(
                deadline - Duration::from_secs(1),
                DEFAULT_BUCKET_REFRESH_AFTER,
                [0xff; 20]
            )
            .is_none()
    );
    let target = table
        .stale_refresh_target(deadline, DEFAULT_BUCKET_REFRESH_AFTER, [0x55; 20])
        .unwrap();
    assert!(
        table.buckets[table.bucket_index(target)]
            .range
            .contains(target)
    );
    table.mark_refreshed(target, deadline);
    assert_eq!(
        table.next_refresh_deadline(DEFAULT_BUCKET_REFRESH_AFTER),
        Some(deadline + DEFAULT_BUCKET_REFRESH_AFTER)
    );
}

/// 分裂后的每个 bucket 都应生成自己范围内的目标，并分别记录刷新时间。
#[test]
fn split_buckets_are_refreshed_independently() {
    let start = Instant::now();
    let mut table = RoutingTable::new(NodeId([0; 20]), AddressFamily::Ipv4, start);
    for suffix in 1..=BUCKET_SIZE as u8 {
        table.observe_response(NodeId([0x80 | suffix; 20]), ipv4(suffix), start);
    }
    table.observe_response(NodeId([0x90; 20]), ipv4(9), start);
    assert_eq!(table.bucket_count(), 2);

    let due = start + DEFAULT_BUCKET_REFRESH_AFTER;
    let left_target = table
        .stale_refresh_target(due, DEFAULT_BUCKET_REFRESH_AFTER, [0xff; 20])
        .unwrap();
    assert_eq!(left_target.0[0] & 0x80, 0);
    table.mark_refreshed(left_target, due);

    let right_target = table
        .stale_refresh_target(due, DEFAULT_BUCKET_REFRESH_AFTER, [0; 20])
        .unwrap();
    assert_ne!(right_target.0[0] & 0x80, 0);
}

/// 按 XOR 距离返回最近的好节点，过滤已判定为坏的联系人。
#[test]
fn closest_good_sorts_by_xor_distance_and_skips_bad_nodes() {
    let now = Instant::now();
    let nearest = id_with_distance_bit(0, 0);
    let middle = id_with_distance_bit(80, 0);
    let farthest = id_with_distance_bit(159, 0);
    let mut table = RoutingTable::new(NodeId([0; 20]), AddressFamily::Ipv4, now);
    table.observe_response(farthest, ipv4(1), now);
    table.observe_response(nearest, ipv4(2), now);
    table.observe_response(middle, ipv4(3), now);
    table.record_failure(middle, now);
    table.record_failure(middle, now);
    assert_eq!(
        table
            .closest_good(&[0; 20], 8, now)
            .iter()
            .map(|node| node.id)
            .collect::<Vec<_>>(),
        vec![nearest, farthest]
    );
}
