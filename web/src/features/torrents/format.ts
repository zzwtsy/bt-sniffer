export function bytes(value: string | null) {
  if (value == null)
    return "—";
  let amount: bigint;
  try {
    amount = BigInt(value);
  } catch {
    return "—";
  }
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
  let unit = 0;
  let divisor = 1n;
  while (unit < units.length - 1 && amount >= divisor * 1024n) {
    divisor *= 1024n;
    unit++;
  }
  if (unit === 0)
    return `${amount} B`;
  const whole = amount / divisor;
  const decimal = (amount % divisor) * 10n / divisor;
  return `${whole}.${decimal} ${units[unit]}`;
}

export function fetchedAt(value: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(new Date(value));
}

const relative = new Intl.RelativeTimeFormat("zh-CN", { numeric: "auto" });

/** 相对时间用于列表扫描；超过 30 天回退到绝对日期。 */
export function relativeTime(value: number, now: number = Date.now()) {
  const seconds = Math.trunc((value - now) / 1000);
  const absSeconds = Math.abs(seconds);
  if (absSeconds < 10)
    return "刚刚";
  if (absSeconds < 60)
    return relative.format(seconds, "second");
  const minutes = Math.trunc(seconds / 60);
  if (Math.abs(minutes) < 60)
    return relative.format(minutes, "minute");
  const hours = Math.trunc(seconds / 3600);
  if (Math.abs(hours) < 24)
    return relative.format(hours, "hour");
  const days = Math.trunc(seconds / 86400);
  if (Math.abs(days) < 30)
    return relative.format(days, "day");
  return new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium" }).format(new Date(value));
}

export function shortHash(value: string) {
  return `${value.slice(0, 8)}…${value.slice(-6)}`;
}

/** magnet URN 前缀：v2 完整身份按 BEP 52 使用 multihash sha2-256 前缀。 */
const URN_V1 = "btih:";
const URN_V2 = "btmh:1220";

/** 复制用的完整磁力链接；有已解析名称时附带 dn 参数，方便客户端直接显示名称。 */
export function magnetLink(hash: string, name?: string | null) {
  const base = `magnet:?xt=urn:${hash.length === 64 ? URN_V2 : URN_V1}${hash}`;
  return name == null || name === "" ? base : `${base}&dn=${encodeURIComponent(name)}`;
}
