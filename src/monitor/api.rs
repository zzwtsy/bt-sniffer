//! 最小监控 HTTP/SSE 接口；错误响应不解析底层错误文字。
use super::*;
use crate::info_hash::{InfoHashV1, InfoHashV2, TorrentIdentity};
use crate::observation::Filter;
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
        .route("/api/v1/stream", get(stream))
        .route("/api/v1/torrents", get(torrents))
        .route("/api/v1/torrents/{hash}", get(torrent))
        .route("/api/v1/torrents/{hash}/files", get(torrent_files))
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

fn read_error(error_value: ReadError) -> Response {
    match error_value {
        ReadError::Invalid => error(StatusCode::BAD_REQUEST, "invalid_query"),
        ReadError::Missing => error(StatusCode::NOT_FOUND, "not_found"),
        ReadError::Busy => error(StatusCode::TOO_MANY_REQUESTS, "database_capacity"),
        ReadError::Cancelled => error(StatusCode::GATEWAY_TIMEOUT, "query_timeout"),
        ReadError::Unavailable => error(StatusCode::SERVICE_UNAVAILABLE, "data_unavailable"),
    }
}

fn json_response(value: impl Serialize) -> Response {
    match serde_json::to_value(value) {
        Ok(value) => response(value),
        Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "data_unavailable"),
    }
}

fn parse_hash(value: &str) -> Result<TorrentIdentity, ReadError> {
    if !matches!(value.len(), 40 | 64) || !value.is_ascii() {
        return Err(ReadError::Invalid);
    }
    let bytes = value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let text = std::str::from_utf8(pair).map_err(|_| ReadError::Invalid)?;
            u8::from_str_radix(text, 16).map_err(|_| ReadError::Invalid)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if bytes.len() == 20 {
        Ok(TorrentIdentity::V1(InfoHashV1(
            bytes.try_into().map_err(|_| ReadError::Invalid)?,
        )))
    } else {
        Ok(TorrentIdentity::V2(InfoHashV2(
            bytes.try_into().map_err(|_| ReadError::Invalid)?,
        )))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TorrentsQuery {
    q: Option<String>,
    page: Option<usize>,
    limit: Option<usize>,
}

async fn torrents(
    Extract(state): Extract<Arc<State>>,
    Params(query): Params<TorrentsQuery>,
) -> Response {
    let permit = match state.database_permit() {
        Ok(permit) => permit,
        Err(error_value) => return read_error(error_value),
    };
    let cancel = state.stop.child_token();
    let _guard = cancel.clone().drop_guard();
    match state
        .store
        .catalog_page(
            query.q,
            query.page.unwrap_or(1),
            query.limit.unwrap_or(50),
            permit,
            cancel,
        )
        .await
    {
        Ok(page) => json_response(page),
        Err(error_value) => read_error(error_value),
    }
}

async fn torrent(Extract(state): Extract<Arc<State>>, Path(hash): Path<String>) -> Response {
    let hash = match parse_hash(&hash) {
        Ok(hash) => hash,
        Err(error_value) => return read_error(error_value),
    };
    let permit = match state.database_permit() {
        Ok(permit) => permit,
        Err(error_value) => return read_error(error_value),
    };
    let cancel = state.stop.child_token();
    let _guard = cancel.clone().drop_guard();
    match state.store.torrent_detail(hash, permit, cancel).await {
        Ok(detail) => json_response(detail),
        Err(error_value) => read_error(error_value),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilesQuery {
    after: Option<String>,
    limit: Option<usize>,
}

async fn torrent_files(
    Extract(state): Extract<Arc<State>>,
    Path(hash): Path<String>,
    Params(query): Params<FilesQuery>,
) -> Response {
    let hash = match parse_hash(&hash) {
        Ok(hash) => hash,
        Err(error_value) => return read_error(error_value),
    };
    let permit = match state.database_permit() {
        Ok(permit) => permit,
        Err(error_value) => return read_error(error_value),
    };
    let cancel = state.stop.child_token();
    let _guard = cancel.clone().drop_guard();
    match state
        .store
        .torrent_files(
            hash,
            query.after,
            query.limit.unwrap_or(100),
            permit,
            cancel,
        )
        .await
    {
        Ok(page) => json_response(page),
        Err(error_value) => read_error(error_value),
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
async fn health(Extract(s): Extract<Arc<State>>) -> Response {
    response(
        json!({"phase":if s.stop.is_cancelled(){"shutting_down"}else{"running"},"sources":s.cached_summary(),"collector":s.observer.get_state("collector")}),
    )
}
async fn snapshot(Extract(s): Extract<Arc<State>>) -> Response {
    response(s.snapshot())
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
        None => (
            window.run_id.clone(),
            window
                .latest
                .parse()
                .expect("Observer 序号由 u64 编码为十进制字符串"),
        ),
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
                let event = Event::default().event("hello").data(
                    serde_json::to_string(&s.observer.window())
                        .expect("监控窗口字段均可序列化为 JSON"),
                );
                return Some((
                    Ok::<_, Infallible>(event),
                    (s, permit, changed, run, after, false, false, next_snapshot),
                ));
            }
            loop {
                let w = s.observer.window();
                let oldest = w
                    .oldest
                    .parse::<u64>()
                    .expect("Observer 最早序号由 u64 编码为十进制字符串");
                let latest = w
                    .latest
                    .parse::<u64>()
                    .expect("Observer 最新序号由 u64 编码为十进制字符串");
                if run != w.run_id || after.saturating_add(1) < oldest || after > latest {
                    let event = Event::default()
                        .event("reset")
                        .data(serde_json::to_string(&w).expect("监控窗口字段均可序列化为 JSON"));
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
                    if after.saturating_add(1)
                        < page
                            .window
                            .oldest
                            .parse::<u64>()
                            .expect("Observer 最早序号由 u64 编码为十进制字符串")
                    {
                        let event = Event::default().event("reset").data(
                            serde_json::to_string(&page.window)
                                .expect("监控窗口字段均可序列化为 JSON"),
                        );
                        return Some((
                            Ok(event),
                            (s, permit, changed, run, after, false, true, next_snapshot),
                        ));
                    }
                    after = page
                        .next
                        .parse()
                        .expect("Observer 分页游标由 u64 编码为十进制字符串");
                    let event = Event::default()
                        .event("events")
                        .id(format!("{run}:{after}"))
                        .data(
                            serde_json::to_string(&page).expect("监控事件页字段均可序列化为 JSON"),
                        );
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
