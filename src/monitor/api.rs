//! HTTP DTO、分页与共享历史 SSE；错误响应不解析底层错误文字。
use super::*;
use crate::observation::{Filter, Kind, hex, parse_hash};
use axum::{
    Router,
    extract::Request,
    extract::{Path, Query as Params, State as Extract},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use serde::Deserialize;
use std::convert::Infallible;

pub(super) fn router(state: Arc<State>) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/snapshot", get(snapshot))
        .route("/api/v1/dht/nodes", get(nodes))
        .route("/api/v1/dht/nodes/{id}/routing", get(routing))
        .route("/api/v1/discoveries", get(discoveries))
        .route("/api/v1/discoveries/{id}", get(discovery))
        .route("/api/v1/hashes", get(hashes))
        .route("/api/v1/hashes/{hash}", get(hash))
        .route("/api/v1/hashes/{hash}/attempts", get(attempts))
        .route("/api/v1/jobs", get(jobs))
        .route("/api/v1/metadata", get(metadata))
        .route("/api/v1/events", get(events))
        .route("/api/v1/stream", get(stream))
        .fallback(|| async { error(StatusCode::NOT_FOUND, "not_found") })
        .layer(middleware::from_fn_with_state(state.clone(), limit))
        .with_state(state)
}
fn error(status: StatusCode, code: &str) -> Response {
    let message = match status {
        StatusCode::BAD_REQUEST => "参数或游标格式无效",
        StatusCode::NOT_FOUND => "对象不存在或历史已不可用",
        StatusCode::METHOD_NOT_ALLOWED => "监控接口只支持 GET",
        StatusCode::TOO_MANY_REQUESTS => "监控资源额度已耗尽，请稍后重试",
        StatusCode::GATEWAY_TIMEOUT => "监控查询等待或执行超时",
        _ => "监控数据源或服务暂不可用",
    };
    (
        status,
        axum::Json(json!({"error":{"code":code,"message":message}})),
    )
        .into_response()
}
fn response(value: Value) -> Response {
    let body = value.to_string();
    if body.len() > 1024 * 1024 {
        return error(StatusCode::SERVICE_UNAVAILABLE, "response_capacity");
    }
    (
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (axum::http::header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}
fn read_response(result: Result<Value, ReadError>) -> Response {
    match result {
        Ok(v) => response(v),
        Err(ReadError::Invalid) => error(StatusCode::BAD_REQUEST, "invalid_query"),
        Err(ReadError::Missing) => error(StatusCode::NOT_FOUND, "not_found"),
        Err(ReadError::Cancelled) => error(StatusCode::GATEWAY_TIMEOUT, "query_timeout"),
        Err(ReadError::Busy) => error(StatusCode::TOO_MANY_REQUESTS, "database_capacity"),
        Err(ReadError::Unavailable) => error(StatusCode::SERVICE_UNAVAILABLE, "source_unavailable"),
    }
}
async fn limit(Extract(state): Extract<Arc<State>>, request: Request, next: Next) -> Response {
    if state.stop.is_cancelled() {
        return error(StatusCode::SERVICE_UNAVAILABLE, "shutting_down");
    }
    if request.method() != axum::http::Method::GET {
        return error(StatusCode::METHOD_NOT_ALLOWED, "read_only");
    }
    {
        let mut rate = state.rate.lock().expect("请求速率锁");
        let now = Instant::now();
        rate.1 = (rate.1 + now.duration_since(rate.0).as_secs_f64() * 10.0).min(10.0);
        rate.0 = now;
        if rate.1 < 1.0 {
            return error(StatusCode::TOO_MANY_REQUESTS, "rate_limit");
        }
        rate.1 -= 1.0;
    }
    let Ok(_permit) = state.requests.clone().try_acquire_owned() else {
        return error(StatusCode::TOO_MANY_REQUESTS, "request_capacity");
    };
    let response = tokio::select! {biased;_=state.stop.cancelled()=>error(StatusCode::SERVICE_UNAVAILABLE,"shutting_down"),result=tokio::time::timeout(Duration::from_secs(2),next.run(request))=>result.unwrap_or_else(|_|error(StatusCode::GATEWAY_TIMEOUT,"request_timeout"))};
    if [StatusCode::BAD_REQUEST, StatusCode::UNPROCESSABLE_ENTITY].contains(&response.status()) {
        return error(StatusCode::BAD_REQUEST, "invalid_query");
    }
    response
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageQuery {
    after: Option<String>,
    limit: Option<usize>,
    state: Option<String>,
    hash: Option<String>,
    object: Option<String>,
    kind: Option<Kind>,
}
impl PageQuery {
    fn size(&self) -> Result<usize, ReadError> {
        let n = self.limit.unwrap_or(50);
        if (1..=100).contains(&n) {
            Ok(n)
        } else {
            Err(ReadError::Invalid)
        }
    }
    fn sequence(&self) -> Result<u64, ReadError> {
        self.after
            .as_ref()
            .map_or(Ok(0), |s| s.parse().map_err(|_| ReadError::Invalid))
    }
}
async fn health(Extract(s): Extract<Arc<State>>) -> Response {
    response(
        json!({"phase":if s.stop.is_cancelled(){"shutting_down"}else{"running"},"sources":s.cached_summary(),"collector":s.observer.get_state("collector")}),
    )
}
async fn snapshot(Extract(s): Extract<Arc<State>>) -> Response {
    response(s.snapshot())
}
async fn nodes(Extract(s): Extract<Arc<State>>) -> Response {
    response(s.cached_summary()["nodes"].clone())
}
async fn routing(
    Extract(s): Extract<Arc<State>>,
    Path(id): Path<usize>,
    Params(q): Params<PageQuery>,
) -> Response {
    let Ok(limit) = q.size() else {
        return read_response(Err(ReadError::Invalid));
    };
    let Ok(after) = q.sequence() else {
        return read_response(Err(ReadError::Invalid));
    };
    let cache = s.cache.lock().expect("监控缓存锁");
    let Some(node) = cache["nodes"].as_array().and_then(|a| a.get(id)) else {
        return error(StatusCode::NOT_FOUND, "node_not_found");
    };
    let Some(routing) = node.get("routing").filter(|v| !v.is_null()) else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "routing_unavailable");
    };
    let contacts = routing["contacts"].as_array().cloned().unwrap_or_default();
    let start = (after as usize).min(contacts.len());
    let end = (start + limit).min(contacts.len());
    response(
        json!({"observed_at_ms":routing["observed_at_ms"],"buckets":routing["buckets"],"items":contacts[start..end],"next":if end<contacts.len(){Some(end.to_string())}else{None}}),
    )
}
async fn hashes(Extract(s): Extract<Arc<State>>, Params(q): Params<PageQuery>) -> Response {
    let Ok(limit) = q.size() else {
        return read_response(Err(ReadError::Invalid));
    };
    read_response(
        s.query(Query::Hashes {
            after: q.after,
            limit,
        })
        .await,
    )
}
async fn jobs(Extract(s): Extract<Arc<State>>, Params(q): Params<PageQuery>) -> Response {
    let Ok(limit) = q.size() else {
        return read_response(Err(ReadError::Invalid));
    };
    read_response(
        s.query(Query::Jobs {
            after: q.after,
            limit,
            state: q.state,
        })
        .await,
    )
}
async fn metadata(Extract(s): Extract<Arc<State>>, Params(q): Params<PageQuery>) -> Response {
    let Ok(limit) = q.size() else {
        return read_response(Err(ReadError::Invalid));
    };
    read_response(
        s.query(Query::Metadata {
            after: q.after,
            limit,
        })
        .await,
    )
}
async fn hash(Extract(s): Extract<Arc<State>>, Path(hash): Path<String>) -> Response {
    let Some(hash) = parse_hash(&hash) else {
        return read_response(Err(ReadError::Invalid));
    };
    read_response(s.query(Query::Hash(hash)).await)
}
fn history(s: &State, q: PageQuery) -> Response {
    let (Ok(after), Ok(limit)) = (q.sequence(), q.size()) else {
        return read_response(Err(ReadError::Invalid));
    };
    let hash = match q.hash {
        Some(h) => match parse_hash(&h) {
            Some(h) => Some(hex(&h)),
            None => return read_response(Err(ReadError::Invalid)),
        },
        None => None,
    };
    if q.object.as_ref().is_some_and(|s| s.len() > 128) {
        return read_response(Err(ReadError::Invalid));
    }
    response(
        serde_json::to_value(s.observer.page(
            after,
            limit,
            &Filter {
                hash,
                object: q.object,
                kind: q.kind,
            },
        ))
        .expect("事件页可序列化"),
    )
}
async fn events(Extract(s): Extract<Arc<State>>, Params(q): Params<PageQuery>) -> Response {
    history(&s, q)
}
async fn discoveries(Extract(s): Extract<Arc<State>>, Params(q): Params<PageQuery>) -> Response {
    let (Ok(after), Ok(limit)) = (q.sequence(), q.size()) else {
        return read_response(Err(ReadError::Invalid));
    };
    response(s.observer.discoveries(after, limit))
}
async fn discovery(
    Extract(s): Extract<Arc<State>>,
    Path(id): Path<String>,
    Params(mut q): Params<PageQuery>,
) -> Response {
    if id.is_empty() || id.len() > 128 {
        return read_response(Err(ReadError::Invalid));
    }
    if !s.observer.has_discovery(&id) {
        return error(StatusCode::NOT_FOUND, "history_unavailable");
    }
    q.object = Some(id);
    history(&s, q)
}
async fn attempts(
    Extract(s): Extract<Arc<State>>,
    Path(hash): Path<String>,
    Params(q): Params<PageQuery>,
) -> Response {
    let Some(hash) = parse_hash(&hash) else {
        return read_response(Err(ReadError::Invalid));
    };
    let (Ok(after), Ok(limit)) = (q.sequence(), q.size()) else {
        return read_response(Err(ReadError::Invalid));
    };
    response(s.observer.attempts(&hex(&hash), after, limit))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamQuery {
    after: Option<String>,
}
async fn stream(
    Extract(s): Extract<Arc<State>>,
    headers: HeaderMap,
    Params(q): Params<StreamQuery>,
) -> Response {
    let Ok(permit) = s.streams.clone().try_acquire_owned() else {
        return error(StatusCode::TOO_MANY_REQUESTS, "stream_capacity");
    };
    let window = s.observer.window();
    let cursor = match headers.get("last-event-id") {
        Some(v) => match v.to_str() {
            Ok(v) => Some(v.to_owned()),
            Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_cursor"),
        },
        None => q.after,
    };
    let (run, sequence) = match cursor {
        Some(c) => match c.rsplit_once(':') {
            Some((run, n)) if !run.is_empty() && run.len() <= 128 => match n.parse::<u64>() {
                Ok(n) => (run.to_owned(), n),
                Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_cursor"),
            },
            _ => return error(StatusCode::BAD_REQUEST, "invalid_cursor"),
        },
        None => (window.run_id.clone(), window.latest.parse().unwrap()),
    };
    let changed = s.observer.subscribe();
    let output = futures_util::stream::unfold(
        (
            s,
            permit,
            changed,
            run,
            sequence,
            true,
            false,
            Instant::now() + Duration::from_secs(1),
        ),
        |(s, permit, mut changed, run, mut after, hello, done, mut next_snapshot)| async move {
            if done {
                return None;
            }
            if hello {
                let event = Event::default()
                    .event("hello")
                    .data(serde_json::to_string(&s.observer.window()).unwrap());
                return Some((
                    Ok::<_, Infallible>(event),
                    (s, permit, changed, run, after, false, false, next_snapshot),
                ));
            }
            loop {
                let w = s.observer.window();
                let oldest = w.oldest.parse::<u64>().unwrap();
                let latest = w.latest.parse::<u64>().unwrap();
                if run != w.run_id || after.saturating_add(1) < oldest || after > latest {
                    let event = Event::default()
                        .event("reset")
                        .data(serde_json::to_string(&w).unwrap());
                    return Some((
                        Ok(event),
                        (s, permit, changed, run, after, false, true, next_snapshot),
                    ));
                }
                if !s.finish.is_cancelled() && Instant::now() >= next_snapshot {
                    next_snapshot = Instant::now() + Duration::from_secs(1);
                    let body = s.snapshot().to_string();
                    let (event, done) = if body.len() > 1024 * 1024 {
                        (
                            Event::default()
                                .event("reset")
                                .data("{\"reason\":\"response_capacity\"}"),
                            true,
                        )
                    } else {
                        (Event::default().event("snapshot").data(body), false)
                    };
                    return Some((
                        Ok(event),
                        (s, permit, changed, run, after, false, done, next_snapshot),
                    ));
                }
                if after < latest {
                    let page = s.observer.page(after, 100, &Filter::default());
                    // 数据库线程可在 window 与 page 之间生产事件并触发淘汰。
                    if after.saturating_add(1) < page.window.oldest.parse::<u64>().unwrap() {
                        let event = Event::default()
                            .event("reset")
                            .data(serde_json::to_string(&page.window).unwrap());
                        return Some((
                            Ok(event),
                            (s, permit, changed, run, after, false, true, next_snapshot),
                        ));
                    }
                    after = page.next.parse().unwrap();
                    let event = Event::default()
                        .event("events")
                        .id(format!("{run}:{after}"))
                        .data(serde_json::to_string(&page).unwrap());
                    return Some((
                        Ok(event),
                        (s, permit, changed, run, after, false, false, next_snapshot),
                    ));
                }
                if s.finish.is_cancelled() {
                    return None;
                }
                tokio::select! {
                    _=s.finish.cancelled()=>{},
                    result=changed.changed()=>{if result.is_err(){return None;}},
                    _=tokio::time::sleep_until(next_snapshot)=>{}
                }
            }
        },
    );
    Sse::new(output)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}
