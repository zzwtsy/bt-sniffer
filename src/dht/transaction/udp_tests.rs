//! UDP transport 与 transaction manager 的组合测试。

use super::TransactionManager;
use crate::dht::krpc::KrpcMessage;
use crate::dht::krpc::MessageType;
use crate::dht::krpc::NodeId;
use crate::dht::krpc::QueryArgs;
use crate::dht::krpc::QueryMethod;
use crate::dht::krpc::ResponseArgs;
use crate::dht::udp::UdpTransport;
use crate::dht::udp::UdpTransportConfig;
use std::time::{Duration, Instant};

/// 模拟一次完整 ping，确认请求、响应和 transaction 匹配能够串联工作。
#[tokio::test]
async fn ping_response_completes_registered_transaction() {
    let client = UdpTransport::bind("127.0.0.1:0", UdpTransportConfig::default())
        .await
        .expect("客户端应该能够绑定");
    let server = UdpTransport::bind("127.0.0.1:0", UdpTransportConfig::default())
        .await
        .expect("服务端应该能够绑定");
    let started_at = Instant::now();
    let mut transactions = TransactionManager::with_initial_counter(Duration::from_secs(3), 8, 42);

    // 先注册再发送；如果发送失败，调用方应使用 cancel 撤销这条记录。
    let transaction_id = transactions
        .register(server.local_addr().unwrap(), QueryMethod::Ping, started_at)
        .expect("ping transaction 应该能够注册");
    let query = KrpcMessage {
        t: transaction_id.to_byte_buf(),
        y: MessageType::Query,
        q: Some(QueryMethod::Ping),
        a: Some(QueryArgs {
            id: NodeId([1; 20]),
            target: None,
            info_hash: None,
            port: None,
            token: None,
            implied_port: None,
            want: Vec::new(),
        }),
        r: None,
        e: None,
        ro: None,
    };
    client
        .send_to(server.local_addr().unwrap(), &query)
        .await
        .expect("ping 查询应该能够发送");

    // 服务端必须原样返回查询中的 transaction ID。
    let received_query = server.recv().await.expect("服务端应该收到 ping");
    let response = KrpcMessage {
        t: received_query.message.t,
        y: MessageType::Response,
        q: None,
        a: None,
        r: Some(ResponseArgs {
            id: NodeId([2; 20]),
            token: None,
            nodes: None,
            nodes6: None,
            values: None,
            samples: None,
            interval: None,
            num: None,
        }),
        e: None,
        ro: None,
    };
    server
        .send_to(received_query.source, &response)
        .await
        .expect("ping 响应应该能够发送");

    let received_response = client.recv().await.expect("客户端应该收到响应");
    let completed = transactions
        .complete(
            &received_response.message.t,
            received_response.source,
            started_at + Duration::from_millis(1),
        )
        .expect("合法响应应该完成 transaction");

    assert_eq!(completed.transaction.method, QueryMethod::Ping);
    assert!(transactions.is_empty());
}
