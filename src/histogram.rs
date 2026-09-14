//! DHT 与采集共用的固定桶计算；不依赖业务切片。
use std::time::Duration;
// 桶上界单位均为毫秒；最后一个有效桶之后另留溢出桶。
const NETWORK_BOUNDS: &[u64] = &[
    1, 5, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 60000, 180000,
];

/// 分位数是桶上界；溢出用 exceeds_ms 表达，绝不把哨兵当成实际毫秒数。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub(crate) struct Quantile {
    pub(crate) upper_bound_ms: Option<u64>,
    pub(crate) exceeds_ms: Option<u64>,
}
const DIAGNOSTIC_BOUNDS: &[u64] = &[
    1, 5, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 60000, 180000, 300000, 600000, 1800000,
    3600000, 7200000, 21600000, 43200000, 64800000, 86400000,
];
/// 桶集合在实例构造时确定，记录和读取不能选用不同边界。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Buckets {
    #[default]
    Network,
    Diagnostic,
}
impl Buckets {
    fn bounds(self) -> &'static [u64] {
        match self {
            Self::Network => NETWORK_BOUNDS,
            Self::Diagnostic => DIAGNOSTIC_BOUNDS,
        }
    }
}
/// 固定桶及溢出桶；不保存任何单条样本。

#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct Histogram {
    buckets: [u64; 23],
    #[serde(skip)]
    kind: Buckets,
}
impl Histogram {
    pub(crate) fn new(kind: Buckets) -> Self {
        Self {
            kind,
            ..Default::default()
        }
    }
    pub(crate) fn merge(&mut self, other: &Self) {
        assert_eq!(self.kind, other.kind, "只能合并同一组固定桶");
        for (bucket, count) in self.buckets.iter_mut().zip(other.buckets) {
            *bucket = bucket.saturating_add(count);
        }
    }

    /// 毫秒向下截断后归入第一个不小于样本的桶。
    pub(crate) fn record(&mut self, duration: Duration) {
        let bounds = self.kind.bounds();
        let ms = duration.as_millis();
        let index = bounds
            .iter()
            .position(|bound| ms <= u128::from(*bound))
            .unwrap_or(bounds.len());
        self.buckets[index] = self.buckets[index].saturating_add(1);
    }
    /// percent 由调用者限制在 1..=100，采用向上取整的样本位次返回近似上界。
    /// 空样本两个字段均为 None；超过最大桶只设置 exceeds_ms，不返回伪造的精确值。
    pub(crate) fn quantile(&self, percent: u64) -> Quantile {
        let bounds = self.kind.bounds();
        let count = self.count();
        if count == 0 {
            return Quantile {
                upper_bound_ms: None,
                exceeds_ms: None,
            };
        }
        let rank = (u128::from(count) * u128::from(percent)).div_ceil(100);
        let mut seen = 0u128;
        for (index, count) in self.buckets.iter().enumerate() {
            seen += u128::from(*count);
            if seen >= rank {
                return Quantile {
                    upper_bound_ms: bounds.get(index).copied(),
                    exceeds_ms: (index >= bounds.len()).then(|| *bounds.last().unwrap()),
                };
            }
        }
        unreachable!("非空直方图的分位落在某个桶中")
    }
    pub(crate) fn count(&self) -> u64 {
        self.buckets.iter().fold(0u64, |a, b| a.saturating_add(*b))
    }
    pub(crate) fn overflow(&self) -> u64 {
        self.buckets[self.kind.bounds().len()]
    }
}
/// 诊断桶的数量与总耗时；合并计数和桶后再读取分位数。
#[derive(Debug, Clone)]
pub(crate) struct Distribution {
    histogram: Histogram,
    pub(crate) count: u64,
    pub(crate) sum_ms: u64,
}
impl Default for Distribution {
    fn default() -> Self {
        Self {
            histogram: Histogram::new(Buckets::Diagnostic),
            count: 0,
            sum_ms: 0,
        }
    }
}
impl Distribution {
    pub(crate) fn new(buckets: Buckets) -> Self {
        Self {
            histogram: Histogram::new(buckets),
            count: 0,
            sum_ms: 0,
        }
    }
    pub(crate) fn record(&mut self, duration: Duration) {
        let ms = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        self.histogram.record(Duration::from_millis(ms));
        self.count += 1;
        self.sum_ms = self.sum_ms.saturating_add(ms);
    }
    pub(crate) fn merge(&mut self, other: &Self) {
        self.histogram.merge(&other.histogram);
        self.count = self.count.saturating_add(other.count);
        self.sum_ms = self.sum_ms.saturating_add(other.sum_ms);
    }
    pub(crate) fn quantile(&self, percent: u64) -> Option<u64> {
        self.histogram.quantile(percent).upper_bound_ms
    }
    pub(crate) fn exceeds(&self, percent: u64) -> Option<u64> {
        self.histogram.quantile(percent).exceeds_ms
    }
    pub(crate) fn overflow(&self) -> u64 {
        self.histogram.overflow()
    }
}
