# DHT 协议与流量

负责节点发现、KRPC 查询、采样与合法 peer 提示，不决定 metadata 任务优先级。源码入口：[dispatcher](../../src/dht/dispatcher/mod.rs)、[KRPC](../../src/dht/krpc/mod.rs)、[transaction](../../src/dht/transaction/mod.rs)、[UDP](../../src/dht/udp/mod.rs)、[traffic](../../src/dht/traffic/mod.rs)。验证入口：`dht::transaction::tests`、`dht::udp::tests`、`dht::traffic::tests`、`dht::dispatcher::sampler::tests`。

## 收到包后信任什么

主动查询在实际发送前注册 transaction，等待额度时不占用 transaction。返回时匹配 transaction ID、来源 socket 地址；已知目标身份时还匹配预期节点 ID，引导未知身份场景由后续验证建立身份；错误或不匹配响应不能被当成成功来更新查询结果。KRPC 边界检查消息类型、字段类型和长度，compact 节点及 peer 地址要满足地址族与地址策略，announce 需通过 token 校验。

UDP 字典乱序兼容位于 `udp/ordering.rs`，仅处理协议允许兼容的编码问题，不绕过重复键、长度、响应身份或结构校验。peer 扩展握手的兼容是另一条边界，不能把这两处策略用于原始 info 字节的完整字典校验。

路由表按地址族独立持有；恢复联系人只是候选信息，不等于本次已经验证可达。`persistence` 保存实例身份、联系人和采样冷却；同一实例名与地址族恢复身份。bootstrap 的 DNS 来源和监听降级规则见[运行](../operations/running.md)。

## 采样与交付

采样通过 dispatcher 的 sampler 接口启动，使用节点与 IP 冷却预约控制重复请求；durable 预约与结果保存需要明确确认，进程恢复不能简单把上次“已发出”解释为完成。采样批次经有界 ingest 保存 hash，背压会影响继续请求的时机。合法 announce 使用尽力非阻塞提示入口，队列满可能丢弃提示，不作为可靠任务日志。

单节点默认并行 3 个采样/回退请求，发送间隔 1 秒，每轮最多 64 个 RPC；候选容量 1024，输出批次及在途预留共用 64 个槽位，节点 ID 与 IP 冷却表各最多 10000 项。最低采样间隔 60 秒，失败退避从 60 秒至 900 秒，不能覆盖远端要求的更长冷却。来源见 [SamplerConfig](../../src/dht/dispatcher/sampler/api.rs)。

采样开关不等于下载开关，DHT 不读取采集重试策略。采样与 fetch 共同启用时，由应用选择相应背压策略，业务接纳限制见[采集](collection.md)。

## 配额与排队

所有节点共用会话 `Budget`。主动查询额度分为 collector、control、sampling、verification：collector 获得总额的整数一半，sampling 与 verification 各为整数十分之一，余量给 control。允许一秒额度突发，不应把短窗口瞬时速率直接当违规。

`traffic` 内部由 `api` 固定类别与统计结构，`limiter` 适配 Tokio 时钟和 GCRA，`state` 独占组合配额与 IP 状态，`report` 在锁外格式化固定大小快照。`Budget` 始终用同一个同步锁覆盖组合探测与统一扣减，不跨 `await` 持锁。

UDP upload 计算 payload 字节，1/8 给主动查询、其余给回复；普通入站与响应预留分别控制。组合配额在短同步锁中先探测再提交，避免目标 IP 受限时无谓消耗其他额度；锁不能跨 await。待发、在途 transaction、IP 跟踪表都有容量与时间限制。修改容量时连同取消、超时和占用统计一起测试，不改成无限队列。

可配置默认值只在[参数表](../operations/running.md#参数)维护。排队、实际发送、响应验证是不同事件，统计口径见[事件参考](log-events.md)。共享预算使双栈不会各自突破配置总量，代价是地址族和请求类别相互竞争；调整分配应以类别等待和实际成功率为证据，而非单看包数。
