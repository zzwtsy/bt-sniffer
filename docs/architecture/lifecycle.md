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

采用共同期限可以限制总退出时间，代价是前一阶段耗尽预算后，后续阶段无法保证完成。若实际出现可复现的收尾超时，应先定位阶段和阻塞所有者，再评估预算或流程；不要通过强杀后声明“正常关闭”掩盖问题。
