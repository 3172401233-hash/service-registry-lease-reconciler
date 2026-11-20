# service-registry-lease-reconciler

协调服务注册租约、僵尸实例和多区域状态收敛。

> **定位说明**：本协调器只**消费**注册中心产生的注册事件（注册 / 心跳 / 注销 / 快照），
> 在旁路计算权威状态与区域可见性，**不替换真实注册中心**，也不负责接收实例流量、
> 持久化或主动探测实例健康。

## 运行环境

- Node.js 24.16.0（`engines: >=24.16.0`）
- 无第三方依赖

```bash
node --test   # 运行全部测试
```

## 解决的问题

- 服务实例崩溃后，注册中心仍会在租约窗口内保留旧记录，调用方把请求发给已停止的端点；
- 多区域复制恢复时，旧租约 / 旧快照可能覆盖新状态（僵尸实例复活）；
- 分区恢复后不能无条件按"最后一条消息"覆盖，较旧快照不能复活已注销实例。

## 事件模型

所有状态都由调用方传入的**逻辑时间**计算，协调器自身不读时钟。

| 事件 | 字段 |
| --- | --- |
| `register` | `region, instanceId, version, time, ttl, endpoint` |
| `heartbeat` | `region, instanceId, version, time, ttl` |
| `deregister` | `region, instanceId, version, time` |
| `snapshot` | `region, snapshotTime, entries[]`（分区恢复后的状态回放） |

```js
import { LeaseReconciler } from './src/reconciler.js';

const reconciler = new LeaseReconciler({
  regions: ['us-east', 'eu-west'],
  replicationDelay: 10,      // 数字、{"us-east->eu-west": 10} 或 (from, to) => ms
  tombstoneRetention: 100,   // 必须大于最大复制延迟
  defaultTtl: 30,
});

reconciler.apply({ type: 'register', region: 'us-east', instanceId: 'svc-1',
                   version: 1, time: 0, ttl: 30, endpoint: { host: '10.0.0.1', port: 8080 } });

const view = reconciler.reconcile('eu-west', /* now */ 12);
// view.endpoints       该区域可见的活跃端点
// view.tombstones      tombstone 及保留截止时间 retainUntil
// view.conflicts       冲突实例（已从 endpoints 中排除）
// view.safe            是否存在复制在途事件导致读取不安全
// view.inFlightEvents  在途事件明细
```

## 状态机

每个实例按 `version` 维度维护明确状态机：

```
ABSENT       --register(v)-------------> ACTIVE        （无 tombstone 或 v 高于 tombstone 版本）
ACTIVE       --heartbeat---------------> ACTIVE        （续租，延长 leaseExpiresAt）
ACTIVE       --deregister--------------> DEREGISTERED  （产生 tombstone）
ACTIVE       --now > leaseExpiresAt----> EXPIRED       （派生状态，按读取时的逻辑时间计算）
EXPIRED      --register(v' >= v)-------> ACTIVE        （重新注册）
EXPIRED      --deregister--------------> DEREGISTERED
DEREGISTERED --register(v' > v)--------> ACTIVE        （换版本复活；同版本拒绝）
```

其余迁移一律拒绝并返回原因，例如：过期后的心跳（必须重新注册）、
注销后的心跳、版本不匹配的续租、同版本重复注册。

## 收敛与冲突规则

- **Last-writer-wins**：事件按 `(version, time)` 排序，严格更旧的事件视为 stale 拒绝；
- **并发冲突**：`(version, time)` 相同但内容分歧的事件记为冲突，冲突实例从可见端点中排除；
  平局时 `deregister` 优先（更安全），同时保留冲突记录；
- **tombstone**：注销产生 tombstone，保留 `tombstoneRetention` 时长；保留期内同版本或
  更旧版本的注册 / 快照条目一律被拒绝，防止旧快照复活已注销实例；
- **快照回放**：逐条目按 `(version, lastEventTime)` 与本地比较，只合并更新的条目，
  被 tombstone 覆盖的条目跳过，绝不无条件覆盖；
- **区域投影**：每个区域的可见状态只由"已复制到该区域"的事件折叠得出，分区期间
  远端仍看到旧状态，恢复后自然收敛；
- **查询安全性**：若存在逻辑时间不晚于读取时间、但尚未复制到读取区域的在途事件，
  `reconcile()` 返回 `safe: false` 并列出在途事件，调用方应稍后重试而非使用过期数据。

## 测试

`node --test` 覆盖：正常续租、主动注销、崩溃过期、区域分区、快照回放、乱序事件，
以及并发冲突与注销平局。
