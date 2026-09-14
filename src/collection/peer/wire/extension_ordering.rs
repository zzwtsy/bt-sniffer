//! 仅规范化扩展握手的字典顺序；不用于 KRPC、metadata 消息头或原始 info。
//! 原子由 Bendy 校验并借用原始编码；输入最多 4096 字节、深度最多 64，所有字典拒绝重复键。
use bendy::decoding::{Decoder, Object};
use std::collections::BTreeMap;
/// 失败仅阻止兼容接纳；调用者返回原始严格解析错误。
#[derive(Debug)]
pub(super) struct InvalidOrdering;

/// 原子借用输入，只为容器分配节点；总节点数受输入字节上限约束。
enum Value<'a> {
    Atom(&'a [u8]),
    List(Vec<Value<'a>>),
    Dict(BTreeMap<&'a [u8], Entry<'a>>),
}
struct Entry<'a> {
    encoded_key: &'a [u8],
    value: Value<'a>,
}

/// 输出长度与输入相同，只改变完整的字典条目顺序。载荷字节串不被递归解析。
pub(super) fn normalize(
    input: &[u8],
    header_limit: usize,
    depth: usize,
) -> Result<Vec<u8>, InvalidOrdering> {
    if input.len() > header_limit.min(4096) || input.first() != Some(&b'd') {
        return Err(InvalidOrdering);
    }
    let mut remaining = input;
    let value = read_value(&mut remaining, 0, depth.min(64))?;
    if !remaining.is_empty() {
        return Err(InvalidOrdering);
    }
    let mut output = Vec::with_capacity(input.len());
    value.write(&mut output);
    Ok(output)
}

fn read_value<'a>(
    input: &mut &'a [u8],
    depth: usize,
    limit: usize,
) -> Result<Value<'a>, InvalidOrdering> {
    match input.first() {
        Some(b'd' | b'l') => {
            if depth >= limit {
                return Err(InvalidOrdering);
            }
            let dictionary = input[0] == b'd';
            *input = &input[1..];
            let value = if dictionary {
                let mut entries = BTreeMap::new();
                while input.first() != Some(&b'e') {
                    let (encoded_key, key) = read_atom(input)?;
                    let key = key.ok_or(InvalidOrdering)?;
                    if entries.contains_key(key) {
                        return Err(InvalidOrdering);
                    }
                    let value = read_value(input, depth + 1, limit)?;
                    entries.insert(key, Entry { encoded_key, value });
                }
                Value::Dict(entries)
            } else {
                let mut values = Vec::new();
                while input.first() != Some(&b'e') {
                    values.push(read_value(input, depth + 1, limit)?);
                }
                Value::List(values)
            };
            *input = &input[1..];
            Ok(value)
        }
        _ => Ok(Value::Atom(read_atom(input)?.0)),
    }
}

/// 只让 bendy 读取一个原子，避免它在容器层提前拒绝乱序键。
/// 返回原始编码和可选的字符串内容；数字、长度前导零等仍由库严格校验。
fn read_atom<'a>(input: &mut &'a [u8]) -> Result<(&'a [u8], Option<&'a [u8]>), InvalidOrdering> {
    if !matches!(input.first(), Some(b'i' | b'0'..=b'9')) {
        return Err(InvalidOrdering);
    }
    let mut decoder = Decoder::new(input);
    let (length, bytes) = match decoder.next_object().map_err(|_| InvalidOrdering)? {
        Some(Object::Bytes(bytes)) => {
            // bendy 已拒绝非规范长度，十进制头长度可由内容长度精确还原。
            let digits = bytes.len().checked_ilog10().unwrap_or(0) as usize + 1;
            (digits + 1 + bytes.len(), Some(bytes))
        }
        Some(Object::Integer(number)) => (number.len() + 2, None),
        _ => return Err(InvalidOrdering),
    };
    let (encoded, rest) = input.split_at(length);
    *input = rest;
    Ok((encoded, bytes))
}

impl Value<'_> {
    fn write(&self, output: &mut Vec<u8>) {
        match self {
            Self::Atom(encoded) => output.extend_from_slice(encoded),
            Self::List(values) => {
                output.push(b'l');
                for value in values {
                    value.write(output);
                }
                output.push(b'e');
            }
            Self::Dict(entries) => {
                output.push(b'd');
                for entry in entries.values() {
                    output.extend_from_slice(entry.encoded_key);
                    entry.value.write(output);
                }
                output.push(b'e');
            }
        }
    }
}
