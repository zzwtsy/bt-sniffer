//! 命令行只解析配置，不创建目录、不解析 DNS，也不打开网络端口。
//!
//! Cli 交给 app 组装资源；地址策略同时约束引导、DHT 自动查询和 metadata 连接。
use crate::address::AddressPolicy;
use crate::dht::routing::AddressFamily;
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf};

pub(crate) const DEFAULT_BOOTSTRAP: [&str; 3] = [
    "dht.libtorrent.org:25401",
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
];

#[derive(Debug, Parser)]
#[command(version, about = "持久化的双栈 BitTorrent DHT 服务节点")]
pub(crate) struct Cli {
    /// 开启只读观测 HTTP；仅允许 loopback，省略时不创建监控资源。
    #[arg(long, value_parser = monitor_address)]
    pub(crate) monitor_listen: Option<SocketAddr>,
    /// 状态目录；默认使用操作系统的本地应用数据目录。
    #[arg(long)]
    pub(crate) state_dir: Option<PathBuf>,
    // 实例名与地址族共同定位数据库身份，不隔离日志，也不绕过状态目录独占锁。
    #[arg(long, default_value = "main", value_parser = instance_name)]
    pub(crate) instance: String,
    /// IPv4 UDP 监听地址（默认 0.0.0.0:6881）。
    #[arg(long, conflicts_with = "ipv6_only", value_parser = listen_v4)]
    pub(crate) listen_v4: Option<SocketAddr>,
    /// IPv6 UDP 监听地址（默认 `[::]:6881`）。
    #[arg(long, conflicts_with = "ipv4_only", value_parser = listen_v6)]
    pub(crate) listen_v6: Option<SocketAddr>,
    #[arg(long, conflicts_with = "ipv6_only")]
    pub(crate) ipv4_only: bool,
    #[arg(long)]
    pub(crate) ipv6_only: bool,
    /// 可重复指定 HOST:PORT；指定后替换内置引导列表。
    #[arg(long, conflicts_with = "no_bootstrap", value_parser = bootstrap_address)]
    pub(crate) bootstrap: Vec<String>,
    /// 禁止公共引导，仍会恢复并验证磁盘中的联系人。
    #[arg(long)]
    pub(crate) no_bootstrap: bool,
    /// 开启主动 BEP 51 采样；默认只提供 DHT 服务。
    #[arg(long)]
    pub(crate) sample: bool,
    /// 消费历史 hash 和合法宣布，自动获取原始 metadata；与 --sample 组合开启主动闭环。
    #[arg(long)]
    pub(crate) fetch: bool,
    /// --sample --fetch 组合的主动采样背压；capacity 保留原满载暂停策略。
    #[arg(long, value_enum, default_value = "freshness")]
    pub(crate) sample_backpressure: crate::collection::SampleBackpressure,
    // 同时持有任务领取的 worker 数上限，不是每个地址族各自的额度。
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=64), requires = "fetch")]
    pub(crate) fetch_concurrency: u16,
    // 活跃任务接纳上限，不限制数据库中的全部历史任务数。
    #[arg(long, default_value_t = 10000, value_parser = clap::value_parser!(u32).range(1..=1000000), requires = "fetch")]
    pub(crate) fetch_max_active_jobs: u32,
    /// 数据库及 WAL 的软容量预算，预留 64 MiB 清理空间。
    #[arg(long, default_value_t = 10737418240u64, value_parser = clap::value_parser!(u64).range(134217728..), requires = "fetch")]
    pub(crate) state_max_bytes: u64,
    /// 全部地址族共享的主动 DHT 查询速率；允许一秒额度突发。
    #[arg(long, default_value_t=20, value_parser=clap::value_parser!(u32).range(10..))]
    pub(crate) dht_query_rate: u32,
    /// 普通入站 UDP 数据报处理速率。
    #[arg(long, default_value_t=200, value_parser=clap::value_parser!(u32).range(1..))]
    pub(crate) dht_inbound_rate: u32,
    /// DHT UDP payload 字节/秒；查询占 1/8，回复占 7/8。
    #[arg(long, default_value_t=262144, value_parser=clap::value_parser!(u32).range(32768..))]
    pub(crate) dht_upload_bytes_per_sec: u32,
    /// 允许局域网单播和 loopback；此选项不会自动禁用公共引导。
    #[arg(long)]
    pub(crate) allow_local: bool,
}
impl Cli {
    /// 生成双栈共用的流量额度配置；这里只复制值，不创建限流器。
    pub(crate) fn traffic(&self) -> crate::dht::traffic::Config {
        crate::dht::traffic::Config {
            queries: self.dht_query_rate,
            inbound: self.dht_inbound_rate,
            upload: self.dht_upload_bytes_per_sec,
        }
    }
    /// 只计算目录路径；目录创建和独占锁由 Storage 负责。
    /// 日志目录独立固定为进程工作目录下的 logs，不由此路径决定。
    pub(crate) fn directory(&self) -> Result<PathBuf, String> {
        self.state_dir
            .clone()
            .or_else(|| {
                directories::BaseDirs::new().map(|dirs| dirs.data_local_dir().join("bt-sniffer"))
            })
            .ok_or_else(|| "无法确定数据目录，请指定 --state-dir".into())
    }
    /// 本地模式仍只接受单播地址，不等于关闭地址校验。
    pub(crate) fn policy(&self) -> AddressPolicy {
        if self.allow_local {
            AddressPolicy::LocalUnicast
        } else {
            AddressPolicy::PublicOnly
        }
    }
    /// 显式列表替换默认列表；禁用引导不影响历史联系人恢复。
    pub(crate) fn seeds(&self) -> Vec<String> {
        if self.no_bootstrap {
            Vec::new()
        } else if self.bootstrap.is_empty() {
            DEFAULT_BOOTSTRAP.iter().map(|s| (*s).into()).collect()
        } else {
            self.bootstrap.clone()
        }
    }
}
/// 按 UTF-8 字节数限制身份键的实例名部分，不按汉字或字符个数计数。
fn instance_name(value: &str) -> Result<String, String> {
    if value.is_empty() || value.len() > 128 {
        Err("实例名必须为 1～128 字节".into())
    } else {
        Ok(value.into())
    }
}
fn listen_v4(value: &str) -> Result<SocketAddr, String> {
    listen(value, AddressFamily::Ipv4)
}
fn listen_v6(value: &str) -> Result<SocketAddr, String> {
    listen(value, AddressFamily::Ipv6)
}
fn listen(value: &str, family: AddressFamily) -> Result<SocketAddr, String> {
    let address: SocketAddr = value.parse().map_err(|e| format!("无效监听地址：{e}"))?;
    if !family.accepts(address) {
        Err("监听地址族不匹配".into())
    } else {
        Ok(address)
    }
}
fn bootstrap_address(value: &str) -> Result<String, String> {
    let (host, port) = value.rsplit_once(':').ok_or("引导地址必须包含端口")?;
    let port: u16 = port.parse().map_err(|_| "无效引导端口")?;
    if port == 0 || host.is_empty() || host.chars().any(char::is_whitespace) {
        return Err("引导地址和非零端口不能为空".into());
    }
    if (host.contains(':') || host.starts_with('[')) && value.parse::<SocketAddr>().is_err() {
        return Err("IPv6 引导地址必须使用 [地址]:端口".into());
    }
    Ok(value.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    /// 默认配置只启用服务；显式引导列表替换默认列表，禁用引导返回空列表。
    #[test]
    fn defaults_and_overrides() {
        // 默认双栈服务，但不会自动开始采样。
        let cli = Cli::try_parse_from(["bt-sniffer"]).unwrap();
        assert!(!cli.sample && !cli.ipv4_only && !cli.ipv6_only);
        assert_eq!(cli.seeds(), DEFAULT_BOOTSTRAP);
        let cli = Cli::try_parse_from(["bt-sniffer", "--bootstrap", "localhost:1234"]).unwrap();
        assert_eq!(cli.seeds(), ["localhost:1234"]);
        assert!(
            Cli::try_parse_from(["bt-sniffer", "--no-bootstrap"])
                .unwrap()
                .seeds()
                .is_empty()
        );
    }
    /// 采集必须显式开启，相关并发和容量参数在解析阶段检查上下界。
    #[test]
    fn fetch_options_are_opt_in_and_bounded() {
        let cli = Cli::try_parse_from(["bt-sniffer"]).unwrap();
        assert!(!cli.fetch);
        assert!(Cli::try_parse_from(["bt-sniffer", "--fetch-concurrency", "2"]).is_err());
        assert!(
            Cli::try_parse_from(["bt-sniffer", "--fetch", "--fetch-concurrency", "0"]).is_err()
        );
        assert!(Cli::try_parse_from(["bt-sniffer", "--fetch", "--state-max-bytes", "1"]).is_err());
        let cli = Cli::try_parse_from(["bt-sniffer", "--sample", "--fetch"]).unwrap();
        assert!(cli.sample && cli.fetch);
    }
    /// 相互冲突的地址族和非法引导地址必须在创建运行资源之前拒绝。
    #[test]
    fn conflicting_and_invalid_arguments() {
        // 参数阶段发现错误，避免已经打开数据库后才发现配置矛盾。
        for args in [
            vec!["--ipv4-only", "--ipv6-only"],
            vec!["--listen-v4", "[::]:1"],
            vec!["--ipv4-only", "--listen-v6", "[::]:1"],
            vec!["--bootstrap", "::1:1"],
            vec!["--bootstrap", "host:0"],
            vec!["--no-bootstrap", "--bootstrap", "host:1"],
        ] {
            assert!(Cli::try_parse_from(std::iter::once("bt-sniffer").chain(args)).is_err());
        }
    }
}

#[cfg(test)]
#[test]
fn dht_budgets_are_enabled_without_fetch_and_reject_invalid_limits() {
    let cli = Cli::try_parse_from(["bt-sniffer"]).unwrap();
    assert_eq!(
        (
            cli.dht_query_rate,
            cli.dht_inbound_rate,
            cli.dht_upload_bytes_per_sec
        ),
        (20, 200, 262144)
    );
    assert!(cli.traffic().validate().is_ok());
    for (flag, value) in [
        ("--dht-query-rate", "9"),
        ("--dht-inbound-rate", "0"),
        ("--dht-upload-bytes-per-sec", "32767"),
    ] {
        assert!(Cli::try_parse_from(["bt-sniffer", flag, value]).is_err());
    }
    assert!(
        Cli::try_parse_from([
            "bt-sniffer",
            "--dht-query-rate",
            "10",
            "--dht-upload-bytes-per-sec",
            "32768"
        ])
        .is_ok()
    );
}

#[test]
fn sampling_backpressure_cli_is_validated_without_changing_resource_defaults() {
    use crate::collection::SampleBackpressure;
    use clap::Parser;
    let cli = Cli::try_parse_from(["bt-sniffer", "--sample", "--fetch"]).unwrap();
    assert_eq!(cli.sample_backpressure, SampleBackpressure::Freshness);
    assert_eq!(cli.fetch_concurrency, 4);
    assert_eq!(
        Cli::try_parse_from(["bt-sniffer", "--sample-backpressure", "capacity"])
            .unwrap()
            .sample_backpressure,
        SampleBackpressure::Capacity
    );
    assert!(Cli::try_parse_from(["bt-sniffer", "--sample-backpressure", "other"]).is_err());
}

fn monitor_address(value: &str) -> Result<SocketAddr, String> {
    let address: SocketAddr = value.parse().map_err(|_| "监控监听地址无效".to_string())?;
    if !address.ip().is_loopback() {
        return Err("监控只能监听 loopback".into());
    }
    Ok(address)
}
