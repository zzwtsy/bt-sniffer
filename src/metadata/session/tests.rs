//! 分片状态独立测试：只有已请求的数据可以进入缓冲区，重复片不能增加完成计数。
use super::*;

/// 乱序和短末片合法；完全相同的重复片不算第二份进度。
#[test]
fn pieces_accept_out_of_order_and_identical_duplicates() {
    let mut pieces = Pieces::new(BLOCK_SIZE + 3);
    pieces
        .deadlines
        .fill(Some(Instant::now() + Duration::from_secs(10)));
    pieces.data(1, BLOCK_SIZE + 3, b"end").unwrap();
    pieces.data(1, BLOCK_SIZE + 3, b"end").unwrap();
    assert_eq!(pieces.received, 1);
    pieces
        .data(0, BLOCK_SIZE + 3, &vec![9; BLOCK_SIZE])
        .unwrap();
    assert_eq!(pieces.received, 2);
    assert_eq!(&pieces.bytes[BLOCK_SIZE..], b"end");
    assert!(pieces.data(1, BLOCK_SIZE + 3, b"bad").is_err());
}

/// 恰好 16 KiB 的最后一片仍然是整片；未请求、越界、尺寸错误不得更新状态。
#[test]
fn invalid_or_unsolicited_pieces_do_not_advance() {
    let mut pieces = Pieces::new(BLOCK_SIZE);
    assert!(pieces.data(0, BLOCK_SIZE, &vec![0; BLOCK_SIZE]).is_err());
    pieces.deadlines[0] = Some(Instant::now());
    assert!(pieces.data(1, BLOCK_SIZE, b"").is_err());
    assert!(
        pieces
            .data(0, BLOCK_SIZE + 1, &vec![0; BLOCK_SIZE])
            .is_err()
    );
    assert!(pieces.data(0, BLOCK_SIZE, &[0; 8]).is_err());
    assert_eq!(pieces.received, 0);
    pieces.data(0, BLOCK_SIZE, &vec![0; BLOCK_SIZE]).unwrap();
    assert_eq!(pieces.received, 1);
}
