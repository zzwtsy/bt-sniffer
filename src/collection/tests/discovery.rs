//! 发现入口与双栈查询到 metadata 入库。
use super::*;
/// 两种入口共用同一采集闭环，区别只在 hash 从采样产生还是从磁盘恢复。
#[derive(Clone, Copy, PartialEq, Eq)]
enum DiscoverySource {
    Sampling,
    BackloggedSampling,
    History,
}

async fn sample_or_history(family: AddressFamily, source: DiscoverySource) {
    sample_or_history_observed(family, source, Default::default()).await;
}
async fn sample_or_history_observed(
    family: AddressFamily,
    source: DiscoverySource,
    observer: crate::observation::Observer,
) {
    // 准备：使用独立目录和本机节点，模拟 peer 返回固定的原始 info 字节。
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle,
        address: _,
    } = fixture_observed(dir.path(), family, observer.clone()).await;
    let store = session.test_store();
    if source == DiscoverySource::History {
        store
            .save_hashes(&[hash(), hash()], now().unwrap())
            .await
            .unwrap();
    }
    if source == DiscoverySource::BackloggedSampling {
        let old: Vec<_> = (10..74).map(|n| SwarmKey([n; 20])).collect();
        store.save_hashes(&old, 0).await.unwrap();
    }
    let (peer, tcp_task) = tcp(family).await;
    let server = udp(family).await;
    let remote = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        loop {
            let request = server.recv().await.unwrap();
            let method = request.message.q.as_ref().unwrap();
            if *method == QueryMethod::GetPeers && source != DiscoverySource::BackloggedSampling {
                assert_eq!(request.message.a.as_ref().unwrap().info_hash, Some(hash()));
                assert!(request.message.a.as_ref().unwrap().target.is_none());
            }
            let wanted = request.message.a.as_ref().and_then(|a| a.info_hash) == Some(hash());
            let message = response(
                request.message.t,
                family,
                (*method == QueryMethod::GetPeers && wanted).then_some(peer),
                method.clone(),
            );
            server.send_to(request.source, &message).await.unwrap();
        }
    });
    handle
        .ping(RemoteNode {
            address: remote,
            expected_id: Some(NodeId([8; 20])),
        })
        .await
        .unwrap();
    let mut fetch_config = config(dir.path());
    if source == DiscoverySource::BackloggedSampling {
        fetch_config.max_active = 10000;
        fetch_config.sample_backpressure = SampleBackpressure::Freshness;
    }
    session.start_fetch(fetch_config).await.unwrap();
    if source != DiscoverySource::History {
        session
            .start_sampling(
                0,
                crate::dht::dispatcher::SamplerConfig {
                    address_policy: AddressPolicy::LocalUnicast,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    // 断言：观察数据库和任务状态，不能只凭网络请求结束认定完成。
    await_metadata(&store).await;
    // 收尾：先确认模拟 peer 完成，再关闭会话，最后停止持续服务的 UDP 任务。
    tcp_task.await.unwrap();
    session.shutdown().await.unwrap();
    server_task.abort();
    let _ = server_task.await;
    // 重启只保留一份结果，不重新领取成功任务。
    let Fixture {
        mut session,
        handle: _,
        address: _,
    } = fixture(dir.path(), family).await;
    session.start_fetch(config(dir.path())).await.unwrap();
    if source != DiscoverySource::BackloggedSampling {
        assert_eq!(session.test_store().active_jobs().await.unwrap(), 0);
    }
    assert_eq!(
        session
            .test_store()
            .metadata(hash())
            .await
            .unwrap()
            .unwrap(),
        INFO
    );
    session.shutdown().await.unwrap();
}
/// 存在首次发现时间为 0 的历史积压时，主动采样仍能发现并提交新的 metadata。
#[tokio::test]
async fn historical_backlog_keeps_active_discovery_and_new_completion() {
    sample_or_history(AddressFamily::Ipv4, DiscoverySource::BackloggedSampling).await;
}
/// 通过本机 IPv4 完成主动采样、查找 peer、下载校验和入库，重启不重复领取成功任务。
#[tokio::test]
async fn sample_to_metadata_v4() {
    sample_or_history(AddressFamily::Ipv4, DiscoverySource::Sampling).await;
}
/// 通过本机 IPv6 消费历史 hash，完成下载与入库，验证历史入口不依赖主动采样。
#[tokio::test]
async fn history_to_metadata_v6() {
    sample_or_history(AddressFamily::Ipv6, DiscoverySource::History).await;
}
/// 合法宣布可直接触发下载；非法 token 不能创建任务，即使宣布者不在路由表中。
#[tokio::test]
async fn valid_announce_downloads_without_routing_and_invalid_token_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let observer = crate::observation::Observer::new("announce".into());
    let Fixture {
        mut session,
        handle: _,
        address,
    } = fixture_observed(dir.path(), AddressFamily::Ipv4, observer.clone()).await;
    let store = session.test_store();
    session.start_fetch(config(dir.path())).await.unwrap();
    let (peer, tcp_task) = tcp(AddressFamily::Ipv4).await;
    let sender = udp(AddressFamily::Ipv4).await;
    sender
        .send_to(
            address,
            &announce_query(Some(Token(ByteBuf::from(b"invalid".to_vec()))), peer.port()),
        )
        .await
        .unwrap();
    assert_eq!(sender.recv().await.unwrap().message.y, MessageType::Error);
    assert_eq!(store.active_jobs().await.unwrap(), 0);
    let events = observer.page(0, 100, &Default::default()).events;
    assert!(
        events
            .iter()
            .any(|e| e["step"] == "announce" && e["result"] == "invalid_token")
    );
    assert!(
        !events
            .iter()
            .any(|e| e["step"] == "announce_save" && e["result"] == "applied")
    );
    sender
        .send_to(address, &announce_query(None, 0))
        .await
        .unwrap();
    let token = sender
        .recv()
        .await
        .unwrap()
        .message
        .r
        .unwrap()
        .token
        .unwrap();
    for _ in 0..3 {
        sender
            .send_to(address, &announce_query(Some(token.clone()), peer.port()))
            .await
            .unwrap();
        sender.recv().await.unwrap();
    }
    await_metadata(&store).await;
    tcp_task.await.unwrap();
    session.shutdown().await.unwrap();
    let events = observer
        .page(
            0,
            100,
            &crate::observation::Filter {
                hash: Some(crate::observation::hex(&hash().0)),
                ..Default::default()
            },
        )
        .events;
    assert!(events.iter().any(|e| e["step"] == "announce_save"
        && e["result"] == "applied"
        && e["context"]["observation_id"].is_string()));
}

/// 从尚未验证的候选继续迭代查询，取得可用 peer 后完成真实 metadata 下载。
#[tokio::test]
async fn iterative_lookup_follows_untrusted_candidate_then_downloads() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle,
        address: _,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let store = session.test_store();
    store.save_hashes(&[hash()], now().unwrap()).await.unwrap();
    let (peer, peer_task) = tcp(AddressFamily::Ipv4).await;
    let a = udp(AddressFamily::Ipv4).await;
    let a_addr = a.local_addr().unwrap();
    let b = udp(AddressFamily::Ipv4).await;
    let b_addr = b.local_addr().unwrap();
    let b_id = NodeId(hash().0);
    let (seen, seen_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let b_task = tokio::spawn(async move {
        let request = b.recv().await.unwrap();
        assert_eq!(request.message.q, Some(QueryMethod::GetPeers));
        seen.send(()).unwrap();
        release_rx.await.unwrap();
        let mut reply = response(
            request.message.t,
            AddressFamily::Ipv4,
            Some(peer),
            QueryMethod::GetPeers,
        );
        reply.r.as_mut().unwrap().id = b_id;
        b.send_to(request.source, &reply).await.unwrap();
    });
    let a_task = tokio::spawn(async move {
        loop {
            let request = a.recv().await.unwrap();
            let mut reply = response(
                request.message.t,
                AddressFamily::Ipv4,
                None,
                QueryMethod::GetPeers,
            );
            if request.message.q == Some(QueryMethod::GetPeers) {
                let SocketAddr::V4(address) = b_addr else {
                    unreachable!()
                };
                reply.r.as_mut().unwrap().nodes =
                    Some(CompactNodesV4(vec![crate::dht::krpc::CompactNodeV4 {
                        id: b_id,
                        address,
                    }]));
            }
            a.send_to(request.source, &reply).await.unwrap();
        }
    });
    handle
        .ping(RemoteNode {
            address: a_addr,
            expected_id: Some(NodeId([8; 20])),
        })
        .await
        .unwrap();
    session.start_fetch(config(dir.path())).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), seen_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !handle
            .fetch_seeds(hash())
            .await
            .unwrap()
            .iter()
            .any(|n| n.id == b_id)
    );
    release.send(()).unwrap();
    await_metadata(&store).await;
    assert!(
        handle
            .fetch_seeds(hash())
            .await
            .unwrap()
            .iter()
            .any(|n| n.id == b_id)
    );
    peer_task.await.unwrap();
    b_task.await.unwrap();
    session.shutdown().await.unwrap();
    a_task.abort();
    let _ = a_task.await;
}

/// get_peers 响应的身份错误或缺少 token 时，不能将其作为有效查找结果。
#[tokio::test]
async fn get_peers_rejects_wrong_identity_and_missing_token() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        session,
        handle,
        address: _,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let server = udp(AddressFamily::Ipv4).await;
    let address = server.local_addr().unwrap();
    for wrong_id in [true, false] {
        let h = handle.clone();
        let query = tokio::spawn(async move {
            h.get_peers(
                RemoteNode {
                    address,
                    expected_id: Some(NodeId([8; 20])),
                },
                hash(),
            )
            .await
        });
        let request = server.recv().await.unwrap();
        let mut reply = response(
            request.message.t,
            AddressFamily::Ipv4,
            None,
            QueryMethod::GetPeers,
        );
        if wrong_id {
            reply.r.as_mut().unwrap().id = NodeId([9; 20]);
        } else {
            reply.r.as_mut().unwrap().token = None;
        }
        server.send_to(request.source, &reply).await.unwrap();
        let error = query.await.unwrap().unwrap_err();
        if wrong_id {
            assert!(matches!(
                error,
                crate::dht::dispatcher::QueryError::UnexpectedNodeId { .. }
            ));
        } else {
            assert!(matches!(
                error,
                crate::dht::dispatcher::QueryError::InvalidResponse(_)
            ));
        }
        assert!(handle.fetch_seeds(hash()).await.unwrap().is_empty());
    }
    session.shutdown().await.unwrap();
}

/// 一条成功近邻返回七个协议失败近邻和一个存活备用，查找必须补位。
#[tokio::test]
async fn lookup_promotes_reserve_on_both_families() {
    for family in [AddressFamily::Ipv4, AddressFamily::Ipv6] {
        let dir = tempfile::tempdir().unwrap();
        let Fixture {
            session, handle, ..
        } = fixture(dir.path(), family).await;
        let mut servers = Vec::new();
        let mut contacts = Vec::new();
        for id in 1..=9 {
            let server = udp(family).await;
            contacts.push((NodeId([id; 20]), server.local_addr().unwrap()));
            servers.push(server);
        }
        let peer: SocketAddr = if family == AddressFamily::Ipv4 {
            "127.0.0.1:6881"
        } else {
            "[::1]:6881"
        }
        .parse()
        .unwrap();
        let mut tasks = Vec::new();
        for (index, server) in servers.into_iter().enumerate() {
            let contacts = contacts.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    let request = server.recv().await.unwrap();
                    let method = request.message.q.unwrap();
                    let mut message = response(request.message.t, family, None, method.clone());
                    let args = message.r.as_mut().unwrap();
                    args.id = contacts[index].0;
                    if method == QueryMethod::GetPeers {
                        if index == 0 {
                            args.nodes = (family == AddressFamily::Ipv4).then(|| {
                                CompactNodesV4(
                                    contacts
                                        .iter()
                                        .skip(1)
                                        .map(|(id, a)| crate::dht::krpc::CompactNodeV4 {
                                            id: *id,
                                            address: match a {
                                                SocketAddr::V4(a) => *a,
                                                _ => unreachable!(),
                                            },
                                        })
                                        .collect(),
                                )
                            });
                            args.nodes6 = (family == AddressFamily::Ipv6).then(|| {
                                CompactNodesV6(
                                    contacts
                                        .iter()
                                        .skip(1)
                                        .map(|(id, a)| crate::dht::krpc::CompactNodeV6 {
                                            id: *id,
                                            address: match a {
                                                SocketAddr::V6(a) => *a,
                                                _ => unreachable!(),
                                            },
                                        })
                                        .collect(),
                                )
                            });
                        } else if index == 8 {
                            args.values = Some(vec![match peer {
                                SocketAddr::V4(a) => CompactPeerAddress::V4(a),
                                SocketAddr::V6(a) => CompactPeerAddress::V6(a),
                            }]);
                        } else {
                            args.token = None;
                        }
                    }
                    server.send_to(request.source, &message).await.unwrap();
                }
            }));
        }
        let target = SwarmKey([0; 20]);
        // 直接查询作为正向对照，但不污染随后查找使用的初始路由。
        handle
            .ping(RemoteNode {
                address: contacts[0].1,
                expected_id: Some(contacts[0].0),
            })
            .await
            .unwrap();
        let result = lookup::lookup(
            std::slice::from_ref(&handle),
            target,
            Arc::new(lookup::LookupPacer::default()),
        )
        .await;
        assert_eq!(result.peers, vec![peer]);
        let direct = handle
            .get_peers(
                RemoteNode {
                    address: contacts[8].1,
                    expected_id: Some(contacts[8].0),
                },
                target,
            )
            .await
            .unwrap();
        assert_eq!(direct.peers, result.peers);
        assert_eq!(handle.status().await.unwrap().pending, 0);
        session.shutdown().await.unwrap();
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

/// 同一确定性工作负载开关观测得到相同持久结果，并验证协议到提交的关联。
#[tokio::test]
async fn observed_sampling_preserves_result_and_links_all_stages() {
    let disabled = tokio::time::Instant::now();
    sample_or_history(AddressFamily::Ipv4, DiscoverySource::Sampling).await;
    let baseline = disabled.elapsed();
    let observer = crate::observation::Observer::new("pipeline".into());
    let enabled = tokio::time::Instant::now();
    sample_or_history_observed(
        AddressFamily::Ipv4,
        DiscoverySource::Sampling,
        observer.clone(),
    )
    .await;
    let elapsed = enabled.elapsed();
    let mut events = Vec::new();
    let mut after = 0;
    loop {
        let page = observer.page(after, 100, &crate::observation::Filter::default());
        let next = page.next.parse().unwrap();
        events.extend(page.events);
        if next == after {
            break;
        }
        after = next;
    }
    for kind in [
        "sampling",
        "discovery",
        "job",
        "lookup",
        "rpc",
        "peer",
        "piece",
        "validation",
        "commit",
    ] {
        assert!(
            events.iter().any(|e| e["kind"] == kind),
            "缺少 {kind}: {events:?}"
        );
    }
    let committed = events
        .iter()
        .find(|e| e["kind"] == "commit" && e["step"] == "metadata" && e["result"] == "applied")
        .expect("真实事务提交事件");
    assert_eq!(
        committed["context"]["swarm_key"],
        crate::observation::hex(&hash().0)
    );
    assert_eq!(committed["context"]["generation"], 1);
    assert!(
        events
            .iter()
            .any(|e| e["kind"] == "piece" && e["context"]["peer_attempt_id"].is_string())
    );
    assert!(
        events
            .iter()
            .any(|e| e["kind"] == "rpc" && e["context"]["rpc_id"].is_string())
    );
    println!(
        "本机观测比较 baseline_ms={} enabled_ms={} retained_events={} retained_bytes={}",
        baseline.as_millis(),
        elapsed.as_millis(),
        observer.window().retained,
        observer.window().bytes
    );
}

/// 固定 v2/hybrid 字节通过双栈 DHT 发现和真实 TCP 扩展协议提交，再读取完整身份目录。
#[tokio::test]
async fn v2_and_hybrid_dual_stack_collection() {
    for family in [AddressFamily::Ipv4, AddressFamily::Ipv6] {
        for info in [
            include_bytes!("../fixtures/v2.info").as_slice(),
            include_bytes!("../fixtures/hybrid.info"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let Fixture {
                mut session,
                handle,
                ..
            } = fixture(dir.path(), family).await;
            let store = session.test_store();
            let full: [u8; 32] = sha2::Sha256::digest(info).into();
            let key = SwarmKey(full[..20].try_into().unwrap());
            let (peer, tcp_task) = tcp_info(family, info.to_vec()).await;
            let server = udp(family).await;
            let remote = server.local_addr().unwrap();
            let task = tokio::spawn(async move {
                loop {
                    let request = server.recv().await.unwrap();
                    let method = request.message.q.unwrap();
                    let mut message = response(
                        request.message.t,
                        family,
                        (method == QueryMethod::GetPeers).then_some(peer),
                        method,
                    );
                    if let Some(samples) = message.r.as_mut().and_then(|r| r.samples.as_mut()) {
                        samples.0 = vec![key];
                    }
                    server.send_to(request.source, &message).await.unwrap();
                }
            });
            handle
                .ping(RemoteNode {
                    address: remote,
                    expected_id: Some(NodeId([8; 20])),
                })
                .await
                .unwrap();
            session.start_fetch(config(dir.path())).await.unwrap();
            session
                .start_sampling(
                    0,
                    crate::dht::dispatcher::SamplerConfig {
                        address_policy: AddressPolicy::LocalUnicast,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(8), async {
                loop {
                    if store.fetch_stats().await.unwrap().succeeded > 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            tcp_task.await.unwrap();
            let permit = store.read_permit.clone().acquire_owned().await.unwrap();
            let detail = store
                .torrent_detail(
                    crate::info_hash::TorrentIdentity::V2(crate::info_hash::InfoHashV2(full)),
                    permit,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            let detail = serde_json::to_value(detail).unwrap();
            assert_eq!(detail["semantic_status"], "valid");
            assert_eq!(detail["file_count"], 2);
            assert_eq!(detail["piece_layers"], "not_fetched");
            assert!(
                detail["verification"]
                    .as_array()
                    .unwrap()
                    .contains(&serde_json::json!("v2_prefix"))
            );
            session.shutdown().await.unwrap();
            task.abort();
            let _ = task.await;
        }
    }
}
