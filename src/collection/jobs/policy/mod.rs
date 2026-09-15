//! 同步领取策略：集中八槽轮转与空类别借用；不读取数据库。
//! scheduler 在一次 run_inner 内持有唯一实例，Store 只消费类别顺序。
use super::ClaimClass;

const ROTATION: [ClaimClass; 8] = [
    ClaimClass::Hint,
    ClaimClass::Recent,
    ClaimClass::Retry,
    ClaimClass::Hint,
    ClaimClass::History,
    ClaimClass::Recent,
    ClaimClass::Retry,
    ClaimClass::Hint,
];
const BORROW_ORDER: [ClaimClass; 4] = [
    ClaimClass::Hint,
    ClaimClass::Recent,
    ClaimClass::History,
    ClaimClass::Retry,
];

/// 进程内轮转游标；重启从首槽开始，不持久化。
pub(in crate::collection) struct ClaimPolicy {
    turn: usize,
}

impl ClaimPolicy {
    pub(in crate::collection) fn new() -> Self {
        Self { turn: 0 }
    }

    /// 只读取本槽的完整查询顺序，不因空队列、错误或取消等待推进。
    pub(in crate::collection) fn order(&self) -> [ClaimClass; 4] {
        order_for(ROTATION[self.turn])
    }

    /// 仅在正常收到成功领取后调用；借用成功也只推进一槽。
    pub(in crate::collection) fn on_claimed(&mut self) {
        self.turn = (self.turn + 1) % ROTATION.len();
    }
}

/// 首选只查询一次，其余类别按固定借用顺序补齐。
pub(super) fn order_for(preferred: ClaimClass) -> [ClaimClass; 4] {
    let mut order = [preferred; 4];
    let mut next = 1;
    for class in BORROW_ORDER {
        if class != preferred {
            order[next] = class;
            next += 1;
        }
    }
    order
}

#[cfg(test)]
mod tests;
