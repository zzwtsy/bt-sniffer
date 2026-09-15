//! 手动验收报告：正常路径显式 finish 确认保存，Drop 仅为异常退出尽力记录。
//! 临时文件完整写入后排他发布，不能覆盖已有证据；不承诺强杀或断电持久性。
mod identity;
#[cfg(test)]
mod tests;

use serde_json::{Value, json};
use std::{
    io,
    path::{Path, PathBuf},
};

/// 验收调用者持有状态与输出路径；finish 的错误必须影响调用者的验收结果。
pub(crate) struct Report {
    pub(crate) value: Value,
    path: PathBuf,
    start: std::time::Instant,
    /// 已尝试显式保存后不在 Drop 重试，防止错误路径发布相反的成功结论。
    attempted: bool,
}

impl Report {
    /// 收集运行身份并保守记为环境未验证；读取源码或身份失败时不能继续验收。
    pub(crate) fn new(kind: &str, directory: &Path, config: Value) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("UTC 时钟")
            .as_millis();
        let mut value = identity::collect().expect("收集验收身份");
        value["report_version"] = json!(2);
        value["kind"] = json!(kind);
        value["started_unix_ms"] = json!(timestamp);
        value["status"] = json!("environment_blocked");
        value["profile"] = json!(if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        });
        value["config"] = config;
        value["verification"] = json!([]);
        value["statistics"] = json!({});
        value["families"] = json!({});
        Self {
            value,
            path: directory.join(format!("{kind}-{timestamp}.json")),
            start: std::time::Instant::now(),
            attempted: false,
        }
    }

    /// 进入业务验证后默认失败，异常退出不能因尚未判定而冒充通过。
    pub(crate) fn running(&mut self) {
        self.value["status"] = json!("failed");
    }

    /// 判定并保存一次；提前停止记 aborted，保存失败返回 Err，重复完成也返回 Err。
    pub(crate) fn finish(&mut self, completed: bool, passed: bool) -> io::Result<()> {
        if self.attempted {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "报告已经尝试保存",
            ));
        }
        self.attempted = true;
        self.value["status"] = json!(if !completed {
            "aborted"
        } else if passed {
            "passed"
        } else {
            "failed"
        });
        self.save()
    }

    /// 写入完整临时文件后无覆盖发布；失败不留下可被误读的最终半文件。
    fn save(&mut self) -> io::Result<()> {
        self.value["duration_seconds"] = json!(self.start.elapsed().as_secs_f64());
        self.value["ended_unix_ms"] = json!(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(io::Error::other)?
                .as_millis()
        );
        let parent = self
            .path
            .parent()
            .ok_or_else(|| io::Error::other("报告缺少目录"))?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        serde_json::to_writer_pretty(temporary.as_file_mut(), &self.value)?;
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
        if self.attempted {
            return;
        }
        // 只有显式 finish 可以发布 passed；异常析构的尽力记录不能代替完成确认。
        if self.value["status"] == "passed" {
            self.value["status"] = json!("failed");
        }
        self.value["completion_confirmation"] = json!("drop_fallback");
        if let Err(error) = self.save() {
            eprintln!("验收报告保存失败 {}：{error}", self.path.display());
        }
    }
}
