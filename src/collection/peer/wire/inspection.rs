//! 对已拒绝的扩展握手补充键序证据；不接纳、不重编码，也不保留原始键。
use bendy::decoding::{Decoder, Object};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InspectionStatus {
    Complete,
    Malformed,
    DepthLimit,
    SizeLimit,
}
/// 两个布尔值表示已观察到的事实；检查不完整时，false 不保证不存在该问题。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KeyInspection {
    pub(crate) unsorted_keys: bool,
    pub(crate) duplicate_keys: bool,
    pub(crate) inspection_status: InspectionStatus,
}
/// 输入受原握手上限与独立 4096 字节预算共同限制；递归及每字典键集合均受输入约束。
pub(super) fn inspect(input: &[u8], header_limit: usize, depth: usize) -> KeyInspection {
    let mut result = KeyInspection {
        unsorted_keys: false,
        duplicate_keys: false,
        inspection_status: InspectionStatus::Complete,
    };
    if input.len() > header_limit.min(4096) {
        result.inspection_status = InspectionStatus::SizeLimit;
        return result;
    }
    let mut remaining = input;
    let parsed = read_value(&mut remaining, 0, depth.min(64), &mut result);
    result.inspection_status = match parsed {
        Err(status) => status,
        Ok(()) if remaining.is_empty() => InspectionStatus::Complete,
        Ok(()) => InspectionStatus::Malformed,
    };
    result
}
/// 消费一个值，检查中断时保留此前观察到的证据；不修改输入字节。
fn read_value(
    input: &mut &[u8],
    depth: usize,
    limit: usize,
    result: &mut KeyInspection,
) -> Result<(), InspectionStatus> {
    match input.first() {
        Some(b'd' | b'l') => {
            if depth >= limit {
                return Err(InspectionStatus::DepthLimit);
            }
            let dictionary = input[0] == b'd';
            *input = &input[1..];
            // 每个字典独立比较原始字节；不同字典的同名键不是重复。
            let mut seen = BTreeSet::new();
            let mut previous: Option<&[u8]> = None;
            while input.first() != Some(&b'e') {
                if dictionary {
                    let key = read_atom(input)?.ok_or(InspectionStatus::Malformed)?;
                    if previous.is_some_and(|old| old > key) {
                        result.unsorted_keys = true;
                    }
                    if !seen.insert(key) {
                        result.duplicate_keys = true;
                    }
                    previous = Some(key);
                }
                read_value(input, depth + 1, limit, result)?;
            }
            *input = &input[1..];
            Ok(())
        }
        _ => {
            read_atom(input)?;
            Ok(())
        }
    }
}
/// 仅借用原子内容；由 Bendy 校验长度和整数语法，不自行实现宽松原子解析器。
fn read_atom<'a>(input: &mut &'a [u8]) -> Result<Option<&'a [u8]>, InspectionStatus> {
    if !matches!(input.first(), Some(b'i' | b'0'..=b'9')) {
        return Err(InspectionStatus::Malformed);
    }
    let mut decoder = Decoder::new(input);
    let (length, bytes) = match decoder
        .next_object()
        .map_err(|_| InspectionStatus::Malformed)?
    {
        Some(Object::Bytes(bytes)) => {
            let digits = bytes.len().checked_ilog10().unwrap_or(0) as usize + 1;
            (digits + 1 + bytes.len(), Some(bytes))
        }
        Some(Object::Integer(number)) => (number.len() + 2, None),
        _ => return Err(InspectionStatus::Malformed),
    };
    *input = input.get(length..).ok_or(InspectionStatus::Malformed)?;
    Ok(bytes)
}
