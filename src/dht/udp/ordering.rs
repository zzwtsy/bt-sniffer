//! 仅为 KRPC 接纳未排序字典；不用于 metadata，也不改变原子值的编码。
//!
//! bendy 0.6 不提供宽松键顺序选项。本适配层只组织容器：原子语法交给
//! bendy，字典按原始键字节排序，重复键（包括未知扩展内部）一律拒绝。
//! 输入先经过 UDP 大小检查；树的节点数受输入长度约束，容器深度最多 64。
use bendy::decoding::{Decoder, Object};
use bendy::serde::Error;
use std::collections::BTreeMap;

pub(super) const MAX_DEPTH: usize = 64;

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
pub(super) fn normalize(input: &[u8]) -> Result<Vec<u8>, Error> {
    let mut remaining = input;
    let value = read_value(&mut remaining, 0)?;
    if !remaining.is_empty() {
        return Err(Error::TrailingBytes);
    }
    let mut output = Vec::with_capacity(input.len());
    value.write(&mut output);
    Ok(output)
}

fn invalid(message: &str) -> Error {
    Error::CustomDecode(message.to_owned())
}

fn read_value<'a>(input: &mut &'a [u8], depth: usize) -> Result<Value<'a>, Error> {
    match input.first() {
        Some(b'd' | b'l') => {
            if depth >= MAX_DEPTH {
                return Err(invalid("KRPC nesting depth exceeds 64"));
            }
            let dictionary = input[0] == b'd';
            *input = &input[1..];
            let value = if dictionary {
                let mut entries = BTreeMap::new();
                while input.first() != Some(&b'e') {
                    let (encoded_key, key) = read_atom(input)?;
                    let key = key.ok_or_else(|| invalid("KRPC dictionary key must be bytes"))?;
                    if entries.contains_key(key) {
                        return Err(invalid("duplicate KRPC dictionary key"));
                    }
                    let value = read_value(input, depth + 1)?;
                    entries.insert(key, Entry { encoded_key, value });
                }
                Value::Dict(entries)
            } else {
                let mut values = Vec::new();
                while input.first() != Some(&b'e') {
                    values.push(read_value(input, depth + 1)?);
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
fn read_atom<'a>(input: &mut &'a [u8]) -> Result<(&'a [u8], Option<&'a [u8]>), Error> {
    if !matches!(input.first(), Some(b'i' | b'0'..=b'9')) {
        return Err(invalid("expected KRPC integer or byte string"));
    }
    let mut decoder = Decoder::new(input);
    let (length, bytes) = match decoder.next_object()? {
        Some(Object::Bytes(bytes)) => {
            // bendy 已拒绝非规范长度，十进制头长度可由内容长度精确还原。
            let digits = bytes.len().checked_ilog10().unwrap_or(0) as usize + 1;
            (digits + 1 + bytes.len(), Some(bytes))
        }
        Some(Object::Integer(number)) => (number.len() + 2, None),
        _ => return Err(invalid("expected KRPC atom")),
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
