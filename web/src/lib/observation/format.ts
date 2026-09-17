import { number } from "./contracts";

const labels: Record<string, string> = {
  pending: "等待领取",
  running: "运行中",
  retry_wait: "等待重试",
  dormant: "暂缓",
  succeeded: "已提交",
  started: "开始",
  waiting: "等待",
  ready: "就绪",
  applied: "已应用（Applied）",
  stale: "旧领取未应用",
  failed: "失败",
  cancelled: "已取消",
  completed: "已完成",
  downloaded: "下载完成",
  received: "已收到",
  accepted: "有效接收",
  duplicate: "重复接收",
  invalid: "无效",
  sent: "已发送",
  new: "新发现",
  reobserved: "重复发现",
  sampling: "主动采样",
  announce: "announce",
  dht: "DHT",
  first: "First · 首次",
  repeat: "Repeat · 再次",
  lifecycle: "生命周期",
  bootstrap: "启动引导",
  routing: "路由",
  rpc: "RPC",
  discovery: "发现",
  admission: "接纳",
  job: "领取",
  lookup: "peer 查找",
  peer: "peer 协议",
  piece: "分片",
  validation: "校验",
  commit: "提交",
  retry: "重试",
  backpressure: "背压",
  execution: "本轮执行",
  claim: "领取",
  connect: "TCP 连接",
  handshake: "标准握手",
  extension: "扩展协商",
  transfer: "分片传输",
  metadata: "metadata",
  queue: "队列",
  queue_wait: "排队原因",
  pacing: "查询节奏",
  tcp_permit: "同 IP 许可",
  hash_saved: "hash 保存",
  batch_segment: "批次分段",
  receive: "分片接收",
  request: "请求",
  candidate: "候选",
  response: "响应",
  timeout: "超时",
  local_wait: "本地等待",
  no_route: "无路由",
  hash_mismatch: "hash 不匹配",
  reclaimed: "已回收",
  validated: "校验通过",
  selected: "已选择",
  capacity: "容量限制",
  good: "良好",
  questionable: "待确认",
  bad: "失效",
  ipv4: "IPv4",
  ipv6: "IPv6",
};
export function label(value: unknown): string {
  return typeof value === "string" ? (labels[value] ?? value) : "未知";
}
export function count(value: unknown): string {
  const n = number(value);
  return n === undefined ? "—" : n.toLocaleString("zh-CN");
}
export function bytes(value: unknown): string {
  const n = number(value);
  if (n === undefined)
    return "未知";
  if (n < 1024)
    return `${n} B`;
  if (n < 1024 ** 2)
    return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / 1024 ** 2).toFixed(1)} MiB`;
}
const fullTime = new Intl.DateTimeFormat("zh-CN", {
  year: "numeric",
  month: "numeric",
  day: "numeric",
  hour: "numeric",
  minute: "numeric",
  second: "numeric",
  hour12: false,
  timeZoneName: "short",
});
const axisTime = new Intl.DateTimeFormat("zh-CN", {
  hour: "2-digit",
  minute: "2-digit",
});
export function timeTick(value: unknown): string {
  const n = Number(value);
  return Number.isFinite(n) && Math.abs(n) <= 8.64e15 ? axisTime.format(n) : "Invalid Date";
}
export function time(value: unknown): string {
  const n = number(value);
  return n === undefined
    ? "未知"
    : Math.abs(n) <= 8.64e15 ? fullTime.format(n) : "Invalid Date";
}
export function duration(value: unknown): string {
  const n = number(value);
  return n === undefined
    ? "未知"
    : n < 1000
      ? `${n.toFixed(0)} ms`
      : `${(n / 1000).toFixed(2)} s`;
}
export function shortHash(hash: string): string {
  return hash.length > 20 ? `${hash.slice(0, 8)}…${hash.slice(-8)}` : hash;
}
