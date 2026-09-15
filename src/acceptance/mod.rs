//! 手动验收报告：身份和状态受类型约束，扩展统计保留 JSON。
//! 显式完成只尝试发布一次；Drop 只为未完成报告尽力保存，不能产生通过结论。
mod identity;
#[cfg(test)]
mod tests;

use serde::Serialize;
use serde_json::{Value, json};
use std::{
    io,
    path::{Path, PathBuf},
};

/// 调用者明确区分未完成、失败与环境阻塞，不用布尔组合表达结果。
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FinishOutcome {
    Passed,
    Failed,
    Aborted,
    EnvironmentBlocked,
}

#[derive(PartialEq, Eq)]
enum Phase {
    Preparing,
    Running,
    Attempted,
}

/// 公共字段只能由报告生命周期修改；各验收的业务统计不强制共享 schema。
#[derive(Serialize)]
struct ReportData {
    #[serde(flatten)]
    identity: identity::RunIdentity,
    report_version: u32,
    kind: String,
    started_unix_ms: u128,
    status: FinishOutcome,
    profile: &'static str,
    config: Value,
    verification: Vec<String>,
    statistics: Value,
    families: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_errors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ended_unix_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completion_confirmation: Option<&'static str>,
}

pub(crate) struct Report {
    data: ReportData,
    path: PathBuf,
    start: std::time::Instant,
    phase: Phase,
}

fn unix_millis() -> io::Result<u128> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_millis())
}

impl Report {
    /// 身份或时钟获取失败直接返回；尚未创建可被误认为通过的报告。
    pub(crate) fn new(kind: &str, directory: &Path, config: Value) -> io::Result<Self> {
        Self::with_identity(kind, directory, config, identity::collect())
    }

    fn with_identity(
        kind: &str,
        directory: &Path,
        config: Value,
        identity: io::Result<identity::RunIdentity>,
    ) -> io::Result<Self> {
        let identity = identity?;
        let timestamp = unix_millis()?;
        Ok(Self {
            data: ReportData {
                identity,
                report_version: 2,
                kind: kind.into(),
                started_unix_ms: timestamp,
                status: FinishOutcome::EnvironmentBlocked,
                profile: if cfg!(debug_assertions) {
                    "debug"
                } else {
                    "release"
                },
                config,
                verification: Vec::new(),
                statistics: json!({}),
                families: json!({}),
                run_errors: None,
                duration_seconds: None,
                ended_unix_ms: None,
                completion_confirmation: None,
            },
            path: directory.join(format!("{kind}-{timestamp}.json")),
            start: std::time::Instant::now(),
            phase: Phase::Preparing,
        })
    }

    fn ensure_mutable(&self) -> io::Result<()> {
        if self.phase == Phase::Attempted {
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "报告已经尝试保存",
            ))
        } else {
            Ok(())
        }
    }

    /// 开始执行后异常退出保守记失败；不重置已完成报告。
    pub(crate) fn running(&mut self) -> io::Result<()> {
        self.ensure_mutable()?;
        self.phase = Phase::Running;
        self.data.status = FinishOutcome::Failed;
        Ok(())
    }
    pub(crate) fn set_statistics(&mut self, value: Value) -> io::Result<()> {
        self.ensure_mutable()?;
        self.data.statistics = value;
        Ok(())
    }
    pub(crate) fn set_families(&mut self, value: Value) -> io::Result<()> {
        self.ensure_mutable()?;
        self.data.families = value;
        Ok(())
    }
    pub(crate) fn set_verification(&mut self, value: Vec<String>) -> io::Result<()> {
        self.ensure_mutable()?;
        self.data.verification = value;
        Ok(())
    }
    pub(crate) fn set_run_errors(&mut self, value: Option<Vec<String>>) -> io::Result<()> {
        self.ensure_mutable()?;
        self.data.run_errors = value;
        Ok(())
    }
    /// 返回独立的只读序列化快照，修改此值不会修改报告。
    pub(crate) fn to_value(&self) -> serde_json::Result<Value> {
        serde_json::to_value(&self.data)
    }
    /// 写入前标记已尝试；即使失败也不在 Drop 重试或允许改写结论。
    pub(crate) fn finish(&mut self, outcome: FinishOutcome) -> io::Result<()> {
        self.ensure_mutable()?;
        self.phase = Phase::Attempted;
        self.data.status = outcome;
        self.save()
    }
    fn save(&mut self) -> io::Result<()> {
        self.data.duration_seconds = Some(self.start.elapsed().as_secs_f64());
        self.data.ended_unix_ms = Some(unix_millis()?);
        let parent = self
            .path
            .parent()
            .ok_or_else(|| io::Error::other("报告缺少目录"))?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        serde_json::to_writer_pretty(temporary.as_file_mut(), &self.data)?;
        use io::Write;
        temporary.write_all(b"\n")?;
        temporary.flush()?;
        temporary
            .persist_noclobber(&self.path)
            .map_err(|error| error.error)?;
        eprintln!("验收报告已保存：{}", self.path.display());
        Ok(())
    }
}
impl Drop for Report {
    fn drop(&mut self) {
        self.data.status = match self.phase {
            Phase::Attempted => return,
            Phase::Preparing => FinishOutcome::EnvironmentBlocked,
            Phase::Running => FinishOutcome::Failed,
        };
        self.data.completion_confirmation = Some("drop_fallback");
        if let Err(error) = self.save() {
            eprintln!("验收报告保存失败 {}：{error}", self.path.display());
        }
    }
}
