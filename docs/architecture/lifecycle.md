# 任务生命周期与关闭

本页定义跨组件的所有者、故障传播和完成确认。源码入口：[main](../../src/main.rs)、[Session](../../src/app/session/mod.rs)、[故障分类](../../src/app/session/faults.rs)、[采集收尾](../../src/collection/lifecycle.rs)、[Storage](../../src/storage/mod.rs)。验证入口：`app::tests::graceful_shutdown_releases_state_directory`、`collection::tests::recovery::graceful_shutdown_cancels_tcp_and_keeps_task_recoverable`。

## 谁拥有资源

| 所有者 | 持有资源 | 完成依据 |
| --- | --- | --- |
| main | 日志 guard、runtime | app 返回、runtime 收尾、最终日志后释放 guard |
| app | 引导流程与 Session | 信号或故障后回收引导并 await 会话关闭 |
| Session | JoinSet、节点、采集协调器、Storage | 消费每个任务结果，再关闭数据库 |
| dispatcher | socket、transaction、路由与采样状态 | 退出结果携带网络结果、持久化结果与快照 |
| 采集协调器 | worker 与发现入口 | 停产、取消查询和 TCP、回收结果并安排恢复 |
| Storage | 数据库线程与目录锁 | 关闭连接及释放锁后发送 finished |
| 检查/诊断脚本 | 自己创建的子进程 | wait / poll 确认退出；规则见各工具说明 |

启动顺序是参数解析、日志初始化、runtime、应用资源组装。help/version 在资源初始化前返回。Session 打开数据库后添加节点，随后按参数启动 fetch 和 sampling；部分初始化失败不会撤销已提交的数据库更新。

Session 内部按资源启动、任务监督和有序关闭分文件，但仍由同一个 Session 值持有任务集合、节点、Monitor 和 Storage；拆分不会产生第二个监督者或关闭入口。

## 故障传播

Session 通过 task ID 关联任务角色，区分运行期提前返回和关闭期预期结束。数据库写入故障暂停自动采集，基础 DHT 可继续工作；需要关闭并重新打开会话恢复，不自动重启采集发包。数据库线程退出、控制任务异常或 panic 会升级为会话故障，不能被“保存快照失败”覆盖最初原因。

等待数据库结果被取消，不会撤销已入队操作。结果通道关闭可能意味着结果未收到，不能据此判断事务是否提交；恢复依赖数据库状态与 generation，见[存储](../domains/storage.md)和[采集](../domains/collection.md)。

## 关闭顺序

Session 所有关闭阶段共用 30 秒期限：

1. 停止快照生产，通知采集协调器取消并回收它；此时 dispatcher 仍存活，供协调器关闭发现入口与查询。
2. 向所有地址族停止采样，再请求关闭节点。
3. 消费剩余节点和采样任务结果，汇总网络及持久化错误。
4. 保存路由快照，重试尚未确认的采样分段；重复保存依靠幂等写入。
5. 读取最终采集统计并输出流量，关闭数据库，确认连接和锁释放。

超时返回包含当前阶段的错误，未确认数据可能未落盘。Session 的 Drop 取消并 abort 任务，不能替代显式 `await shutdown` 的保存保证。main 对 runtime 使用 1 秒 shutdown timeout；系统 DNS 阻塞操作不能仅靠 future 取消确认结束。日志 guard 的有限等待同样不提供持久落盘承诺。

数据库排队或等待结果期间，关闭通知不撤销已经接纳的命令，也不保证全部网络工作立即停止。期限内恢复时，正常关闭必须等待事务结果与线程确认；期限耗尽只报告阶段和已有故障，不能声称数据库已关闭。

`collection::tests::recovery::slow_storage_shutdown_preserves_accepted_commands_and_completion_boundary` 启动真实本机节点和采集协调器，在实际完成命令或诊断快照命令到达数据库线程时用同步屏障阻塞，并单独覆盖命令队列满。期限内恢复须回收协调器、取消未完成 TCP worker 并保存延期或成功状态；期限耗尽须报告“回收采集协调器”阶段，不能宣称数据库已关闭。测试解除阻塞后确认 socket 关闭、等待数据库关闭观察者，再重新打开目录复核 generation、attempts、metadata 和成功事务的提示清理；超时遗留 running 任务由下次启动恢复。

采用共同期限可以限制总退出时间，代价是前一阶段耗尽预算后，后续阶段无法保证完成。若实际出现可复现的收尾超时，应先定位阶段和阻塞所有者，再评估预算或流程；不要通过强杀后声明“正常关闭”掩盖问题。

## 只读监控

app 在启动阶段绑定 loopback，显式启用时绑定失败返回启动错误。Session 拥有 Monitor、刷新任务和 HTTP 连接集合；监控任务提前退出只停止监控，数据库线程故障仍走原故障路径。

开始收尾时停止接受请求、停止刷新并取消监控查询，保留已有 SSE 读取业务收尾事件。数据库结束后沿同一共同截止时间结束 SSE 并回收连接，最终推送最多占剩余期限中的 1 秒；到期中止后确认任务退出。服务任务被中止不会丢失连接集合所有权。预算及接口语义见[观测接口](../domains/monitoring.md)。
