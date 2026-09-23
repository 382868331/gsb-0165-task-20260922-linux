# msi_kernel — 消息驱动的 MSI 目录一致性内核（3 核 / 8 地址，带容量上限）

一个**纯离线、纯离散事件**的目录式缓存一致性消息内核可复用模块。不含真实
线程、不含真实网络：load/store 请求与网络报文投递全部是显式函数调用，
调用方决定报文投递顺序，因此同一份核心可以被确定性地驱动、重放与穷举。

本版在无容量模型上增加三类**容量上限**：目录每地址等待队列、每核在途请求、
网络通道待发报文。网络通道满时发送不丢消息——报文进入 blocked 暂存区、
事务保持未完成，任何投递腾出槽位后按 FIFO 自动补发；目录/核容量满时请求
被明确拒绝且不留半更新，释放后可用相同参数重试。

## 运行环境

- **Python 3.14.7**（仅使用标准库；3.10+ 的语法也兼容，但以 3.14.7 为准）。
- **无需安装任何第三方依赖**，不需要账号 / 密钥 / 网络 / 硬件 / 数据库 /
  中间件 / 云 API / Docker。
- Linux 原生运行。

## 准确命令

```bash
# 必要测试（在仓库根目录执行）
python -m unittest discover -s tests -v

# 固定输入演示（约 0.1 秒内完成，上限约定为约 8 秒）
python demo.py
```

## 仓库结构

```
msi_kernel/
  __init__.py    公开 API 汇总
  errors.py      错误类型（含可定位字段；CapacityExceededError 容量拒绝）
  model.py       协议内核：状态、请求准入、容量上限、消息处理、排队与完成
  explore.py     消息重排枚举、不变量检查（含容量不变量）、有界搜索
tests/
  reference.py   独立参考模型（不导入、不调用被测核心来产生期望值）
  test_model.py        协议行为测试
  test_validation.py   输入校验 / 错误语义 / 批量原子性测试
  test_capacity.py     三类容量、网络背压、批量容量原子性、2 核 2 地址差分测试
  test_reference.py    与独立参考模型逐完成操作对照
  test_explore.py      枚举、预算耗尽 vs 无解 测试
demo.py          固定输入演示（失效链 + 重排枚举 + 异常边界 + 容量背压）
```

## 规模与输入上限（接口边界）

| 项目 | 上限 / 取值 |
|---|---|
| 核数 `NUM_CORES` | 固定 3（核编号 0,1,2） |
| 地址数 `NUM_ADDRS` | 固定 8（地址编号 0..7） |
| 目录节点编号 `DIR` | 3（仅用于消息的 src/dst） |
| 数据值 | 任意 Python `int`（任意精度，无溢出语义） |
| 事务 id / 消息 id | 正整数；不显式给事务 id 时内核自动分配 |
| 容量 `dir_queue_cap` | 目录每地址等待队列长度，默认 4096；`None`=无限；允许 `0` |
| 容量 `core_request_cap` | 每核在途请求数，默认 4096；`None`=无限；允许 `0` |
| 容量 `network_cap` | 全局待发报文数，默认 4096；`None`=无限；允许 `0` |
| 穷举预算 | `explore` / `find_schedule` 的 `budget` 参数（默认 100000 个已访问状态） |

容量均为构造参数：`CacheCoherenceKernel(memory=..., capacity=CapacityConfig(...))`，
也可直接传 `dir_queue_cap=` / `core_request_cap=` / `network_cap=` 关键字。
容量必须是非负整数或 `None`（负数 / `bool` / `float` / 字符串均拒绝）。
三个缺省值都远大于固定规模可能产生的数量，因此**缺省配置的可观测行为与
无容量旧模型完全一致**，原有合法调用无需改动。短演示用 2 核 2 地址（只
使用核 0/1、地址 0/1）的场景核对容量行为，固定 3 核 8 地址的契约不变。

## 快速上手

```python
from msi_kernel import CacheCoherenceKernel, Op, MsgType, Message, DIR, drain

k = CacheCoherenceKernel(memory={0: 100})
k.request(0, 0, Op.LOAD)                       # I -> S，立即完成，读值 100
k.request(1, 0, Op.LOAD)                       # 目录 S，直接加入共享者
k.request(1, 0, Op.STORE, value=200)           # S -> M：先失效核0
for m in k.pending():                          # 显式投递消息（无自动网络）
    rep = k.deliver(m.msg_id)
    for reply in rep.emitted:                  # 对端应答也是待发消息
        k.deliver(reply.msg_id)
print(k.completed()[-1])                       # store 完成事件，值 200

k.request(2, 0, Op.LOAD)                       # 读 M：先 FETCH owner 回写
drain(k)                                       # 按 id 升序排空待发消息
print(k.completed()[-1].value)                 # 200
```

## 公开接口

### 请求输入

- `kernel.request(core, addr, op, *, value=None, txn_id=None) -> RequestReport`
- `kernel.request_batch(iterable_of_Request) -> list[RequestReport]`
  **原子**：整批先校验（含批内事务 id 去重），任一非法则内核零变更。
- 同地址请求 FIFO 排队（`RequestReport.queued=True`）；不同地址事务独立，
  可并存、可交错。
- 无需消息即可满足的请求（I/S 命中、I 上写、唯一共享者升级）在调用内同步
  完成，完成事件放在 `RequestReport.completions`。

### 网络投递（显式输入）

- `kernel.deliver(msg_id: int)`：投递内核待发队列中的报文；
- `kernel.deliver(Message(None, txn_id, mtype, src, dst, addr, value=None))`：
  显式注入一条网络报文（`msg_id` 必须为 `None`），用于表达乱序 / 重复 /
  陈旧报文等外部输入。注入报文的事务 id 必须是内核已注册的事务。

返回 `DeliveryReport`：

| status | 含义 |
|---|---|
| `applied` | 正常生效 |
| `duplicate` | 同一 `msg_id` 已投递过：幂等，无任何副作用 |
| `stale` | 报文不属于该地址当前活动事务（如陈旧 ACK/FLUSH）：**不推进新事务** |

投递一个从不存在的 `msg_id` 抛 `InvalidMessageError`（带 `msg_id`；若该 id
的消息仍在 blocked 暂存区，错误信息会额外提示需先释放通道槽位）。

### 容量上限与背压

三类容量由构造时的 `CapacityConfig`（或关键字）给定，默认值等价于无容量：

| 容量 | 占用槽位的对象 | 满时行为 |
|---|---|---|
| `dir_queue_cap` | 该地址 FIFO **等待队列**里的请求（活动事务不占） | 抛 `CapacityExceededError`，请求零变更、不消耗事务 id，释放后可同参数重试 |
| `core_request_cap` | 该核的**在途请求**＝活动事务 + 各地址等待队列项 | 同上 |
| `network_cap` | 待发队列中的报文（全局） | **不抛错**：报文进 `blocked` FIFO 暂存，事务保持未完成；任何 `deliver` 腾出槽位后自动补发 |

要点：

- **同步完成的请求不占在途/队列槽位**：I/S 命中、目录 I 上写、唯一共享者
  升级在调用内完成，因此即使 `core_request_cap=0`、地址等待队列满（地址空闲
  时）这些请求仍被接纳；只有需要发消息（成为活动事务）或进入等待队列的请求
  才占槽位。
- **网络背压不丢消息**：`_send` 在通道满时把消息（已分配 `msg_id`）放入
  `kernel.blocked()`，事务的等待集合等状态原样保留。投递一条报文腾出槽位后，
  blocked 队首按 FIFO 自动补入待发队列；`DeliveryReport.blocked` /
  `.released` 分别报告“本次新背压的消息”和“本次自动补发的旧消息”。也可用
  `kernel.retry_blocked()` 显式触发补发（通道仍满时为空操作）。
- blocked 消息尚未进入待发队列，不能直接 `deliver`（会抛 `InvalidMessageError`）；
  它们会在补发后以正常 `msg_id` 投递。
- **`network_cap=0` 边界**：同步请求照常完成；一旦事务必须发消息，报文全部
  blocked 而待发队列为空，没有任何报文能腾出槽位，此时 `drain` 抛
  `InfeasibleError`（明确的不可行，不是预算耗尽），blocked 报文仍完整保留。
- **批量容量原子性**：`request_batch` 先在内核深拷贝上顺序预演整批，任一请求
  触及目录/核容量上限都只污染副本，真实内核零变更（连同批内已可同步完成的
  请求也一起回滚），错误带该请求的 `request_seq`。
- 用量观测：`kernel.capacity_view()` 返回 `CapacityView`（每地址队列长、每核
  在途数、待发数、blocked 数、剩余槽位 `*_spare()`）；`snapshot()` 也含
  `blocked_messages` 与 `capacity`。


### 观测

- `kernel.snapshot() -> Snapshot`：每核缓存、每地址目录项（状态 / 共享者 /
  owner / 活动事务 / 等待 ACK 集合）、内存、待发消息、每地址 FIFO 队列、
  完成事件。所有视图为只读拷贝。
- `kernel.pending()` / `kernel.completed()`。
- `Message` / `Request` / `Completion` 等数据结构均为 frozen dataclass。

### 消息类型（有限协议子集）

`FETCH`（目录→M owner，要求回写并降级）、`FLUSH`（owner→目录，带数据）、
`INV`（目录→共享者，要求失效）、`INV_ACK`（共享者→目录）。

### 枚举与搜索（`msi_kernel.explore`）

- `explore(factory, scenario, *, budget=100000, all_paths=False)`：穷举
  “当前待发消息任选其一投递”的全部重排，**逐状态检查不变量**，收集每条
  完整调度的完成序列与每个 load 在所有调度下的读值集合。
  `all_paths=True` 时额外保留全部原始消息 id 路径（不做等价剪枝）。
- `check_invariants(kernel)`：结构不变量（见下）。
- `find_schedule(factory, scenario, *, goal=None, want_complete=True, budget)`：
  有界搜索一条满足目标的投递序列。
- `drain(kernel, *, budget=100000)`：按 id 升序确定性排空。
- `apply_path(kernel, path)`、`descriptor(kernel)`（与消息 id 无关的状态指纹）。

**预算耗尽与证明无解严格分开**：`BudgetExhaustedError` 只表示没搜完；
在预算内穷尽整个可达空间仍找不到目标才抛 `InfeasibleError`。

### 结构不变量（每次投递之间都检查）

1. 每个地址：三个核缓存中 **M 至多一个**；
2. 任一缓存为 M 时，其它缓存**不得为 S**；
3. 目录 M：owner 存在，非 owner 缓存必须为 I（FETCH 已送达而 FLUSH 在途时，
   owner 可为 S，这是协议内过渡态）；
4. 目录 S：无 M 缓存、owner 为空，每个持 S 的核都登记在共享者集合中
   （INV 已在核端生效、ACK 在途时，目录共享者集合可暂时多出核）；
5. 目录 I：全部缓存为 I，owner 为空。
6. 容量：待发报文数 ≤ `network_cap`；每地址等待队列长 ≤ `dir_queue_cap`；
   每核在途请求数 ≤ `core_request_cap`。
7. blocked 非空时，待发队列必须恰好满载（否则 blocked 消息早该自动补发）。

## 错误语义

- 所有错误继承 `KernelError`：`InvalidRequestError` / `InvalidMessageError`
  带 `field`，批量错误还带 `request_seq`，可定位到具体参数与批次位置。
- `CapacityExceededError`（继承 `KernelError`）表示**参数合法但此刻容量不足**：
  带 `field`（`dir_queue` / `core_inflight`）、`limit`、`used`，批量时带
  `request_seq`。它是可恢复的暂时拒绝——不分配/不消耗事务 id、不留半更新，
  待事务完成释放槽位后用相同参数重试即可。网络通道满**不**抛此错，而是走
  blocked 背压。
- 整数参数（core / addr / value / txn_id / msg_id 等）**拒绝 `bool`**
  （`True/False` 不会被当作 1/0）；容量参数同样拒绝 `bool`。
- 数值输入拒绝 `NaN` / 正负 `Infinity`，普通 `float` 也拒绝（消息明确要求
  整数数据）；报错信息区分 NaN、Infinity、普通 float。
- load 不得带 value；store 必须带 int value；事务 id 必须为正整数且全生命
  周期唯一。
- 失败不留未说明的部分变更：批量请求先在副本上整体预演（含容量）后在真实
  内核生效；报文处理的有效性判定先于任何状态修改。

## 数值与误差口径

本模块**不做任何浮点运算**：缓存数据、内存值、消息携带值全部是整数，状态机
是精确离散转移。因此不存在数值误差/容差问题，也**不使用任何宽容差**；所谓
“误差口径”即为**逐位精确相等**。NaN/Infinity 作为非法输入被拒绝。

## 独立参考模型如何验证

`tests/reference.py` **不导入 `msi_kernel`**，只对完成事件
`(addr, op, value)` 序列运行一个平凡的“每地址最后写入”抽象机：同地址操作
严格按准入 FIFO 完成，故某 load 的允许读值唯一等于该地址在它之前最后一次
store 的写入值（此前无 store 则为初始内存值）。测试再用内核实际跑出的完成
序列与该允许值逐完成操作对照，并在 300 个随机（确定性 LCG 生成）小场景的
全部消息重排上复核。内核快照的自洽检查只作为补充断言，不用核心给自己算
期望值。

## 设计取舍

- **固定 3 核 8 地址**：规模是题面契约的一部分，不做任意规模泛化。
- **消息顺序完全交给调用方**：内核不自动投递、不丢消息、不模拟延迟，便于
  穷举重排与复现；`drain` 只是提供一个确定性便利驱动。
- **FETCH 采用“回写后共享”**：owner 收到 FETCH 降级为 S 并回 FLUSH，数据
  写入目录内存后再分发给请求者；不实现缓存间直接传数（没有 Data/DATA 消息）。
- **同地址严格串行（FIFO）**，不同地址无锁独立；事务 id 与消息 id 全程唯一，
  幂等以 `msg_id` 身份判定，陈旧性以“报文事务 id 是否等于该地址当前活动
  事务 + 是否在等待集合中”判定。
- **注入式网络输入**：除投递内核产生的待发报文外，允许调用方显式构造
  `Message` 注入，使“陈旧 ACK / 重复报文”这类异常网络情形可直接表达和测试。
- **三类容量默认等价于无容量**：默认上限 4096 远大于固定规模，旧调用不变；
  显式调小即可得到紧容量行为。网络满采用“保留 + 背压 + 自动 FIFO 补发”，
  目录/核满采用“明确拒绝 + 可重试”，两者语义不同并在文档与异常类型上区分。
- **blocked 是状态的一部分**：结构指纹 `descriptor` 同时包含待发与 blocked
  多重集，紧容量下“消息在 pending 还是 blocked”是不同可达状态，不会被去重
  错误合并；枚举/搜索的“完成终态”要求二者皆空。
- 状态枚举按结构指纹去重（与消息 id 无关），避免等价重排的组合爆炸；
  `all_paths=True` 可在小规模下保留全部原始路径。

## 已知限制

- 只实现题面契约的 **MSI 有限子集**：无 MESI/MOESI、无缓存间直传、无替换
  写回（容量无限）、无分支/合并事务、无消息超时与重传协议。**不宣称兼容
  任何工业标准处理器的一致性实现。**
- 显式注入的报文（陈旧/乱序输入）不参与 `explore` 的常规重排枚举，常规
  枚举只投递内核实际产生的报文；异常网络路径由定向单元测试覆盖。
- 穷举复杂度随并发消息数阶乘增长，`budget` 是硬保护；触发预算时结果中
  `exhausted=True`（枚举）或抛 `BudgetExhaustedError`（搜索），不得把它
  当成“无解”。
- 容量是**静态配置**：内核构造后不能在线调整三类上限；需要不同容量请新建
  内核。`network_cap=0` 下需要发消息的事务无法靠自身投递推进（drain 抛
  `InfeasibleError`），这是边界配置的确定行为，不是可恢复的暂时背压。
- blocked 暂存区本身不设额外长度上限（受限于地址/核容量，系统中在途消息数
  有界）；本迭代只对题目指定的目录、核心请求、网络通道三类入口设容量。
- 不提供持久化、远程接口、并发执行；所有操作在单线程内同步完成。
