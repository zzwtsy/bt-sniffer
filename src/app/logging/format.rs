//! 复用标准格式器，在文本和 JSON 对象上附加进程运行标识。
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, format::Writer};

/// 两种固定输出格式；不对应用户配置或可扩展后端。
pub(super) enum RunFormat {
    Text { id: String },
    Json { id: String },
}
impl<S, N> FormatEvent<S, N> for RunFormat
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let mut line = String::new();
        let buffer = Writer::new(&mut line);
        match self {
            Self::Text { id } => {
                tracing_subscriber::fmt::format()
                    .with_ansi(writer.has_ansi_escapes())
                    .format_event(ctx, buffer, event)?;
                writeln!(writer, "{} run_id={id}", line.trim_end())
            }
            Self::Json { id } => {
                tracing_subscriber::fmt::format()
                    .json()
                    .format_event(ctx, buffer, event)?;
                // 标准 formatter 负责字段、转义和 span；对象序列化避免手工拼接 JSON。
                let mut record: serde_json::Value =
                    serde_json::from_str(&line).map_err(|_| std::fmt::Error)?;
                let object = record.as_object_mut().ok_or(std::fmt::Error)?;
                object.insert("run_id".into(), serde_json::Value::String(id.clone()));
                let encoded = serde_json::to_string(&record).map_err(|_| std::fmt::Error)?;
                writeln!(writer, "{encoded}")
            }
        }
    }
}
