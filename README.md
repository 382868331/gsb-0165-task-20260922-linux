# msi_kernel — 消息驱动的 MSI 目录一致性内核（3 核 / 8 地址，带容量上限与背压）

一个**纯离线、纯离散事件**的目录式缓存一致性消息内核可复用模块。不含真实
线程、不含真实网络：load/store 请求与网络报文投递全部是显式函数调用，
调用方决定报文投递顺序，因此同一份核心可以被确定性地驱动、重放与穷举。

本版在原协议内核上新增**三类有界容量**：目录每地址等待队列、每核未完成
请求数、每条有向网络通道（DIR↔核）的在途消息数。通道满时内核**不丢消息**：
报文保留在发送端、事务保持未完成，待通道腾出后按 FIFO 自动续传；请求超过
队列 / 每核上限时给明确错误且不留半更新状态。

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
  errors.py      错误类型（含可定位字段、容量超限错误）
  model.py       协议内核：状态、请求准入、容量上限、消息处理、排队与完成
  explore.py     消息重排枚举、不变量检查（含容量约束）、有界搜索
tests/
  reference.py   独立参考模型（不导入、不调用被测核心来产生期望值）
  test_model.py        协议行为测试
  test_validation.py   输入校验 / 错误语义 / 批量原子性测试
  test_reference.py    与独立参考模型逐完成操作对照
  test_explore.py      枚举、预算耗尽 vs 无解 测试
  test_capacity.py     容量上限与背压测试（正常 / 边界 / 失败 / 紧宽一致性）
demo.py          固定输入演示（失效链 + 重排枚举 + 异常边界 + 容量背压）
```

## 规模与输入上限（接口边界）

| 项目 | 上限 / 取值 |
|---|---|
| 核数 `NUM_CORES` | 固定 3（核编号 0,1,2） |
| 地址数 `NUM_ADDRS` | 固定 8（地址编号 0..7） |
| 目录节点编号 `DIR` | 3（仅用于消息的 src/dst） |
| 每地址等待队列 `queue_cap` | 正整数，默认 **8** |
| 每核未完成请求 `core_cap` | 正整数，默认 **8** |
| 每有向通道在途消息 `channel_cap` | 正整数，默认 **8**（DIR→核c 与 核c→DIR 各算一条） |
| 数据值 | 任意 Python `int`（任意精度，无溢出语义） |
| 事务 id / 消息 id | 正整数；不显式给事务 id 时内核自动分配 |
| 穷举预算 | `explore` / `find_schedule` 的 `budget` 参数（默认 100000 个已访问状态） |

容量通过构造参数 `Limits(queue_cap=..., core_cap=..., channel_cap=...)`
配置，三项都必须是正整数（拒绝 bool / 0 / 负数 / 非 int）。短演示不穷举
上限规模；上限是接口边界，不是演示目标。

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

容量上限与背压（可选，不给 `limits` 即使用有限的默认容量）：

```python
from msi_kernel import CacheCoherenceKernel, Limits, CapacityExceededError

# 每条有向网络通道只允许 1 条在途消息；每地址队列 / 每核请求上限各 2。
k = CacheCoherenceKernel(limits=Limits(queue_cap=2, core_cap=2, channel_cap=1))
# 通道满时：新报文保留在发送端（retained_messages），事务保持未完成，
# deliver 一条在途报文腾出通道后，报告的 released 字段给出自动续传的报文。
# 请求超过 queue_cap / core_cap 时抛 CapacityExceededError，状态零变更。
```

## 公开接口

### 请求输入

- `kernel.request(core, addr, op, *, value=None, txn_id=None) -> RequestReport`
- `kernel.request_batch(iterable_of_Request) -> list[RequestReport]`
  **原子**：整批先校验（含批内事务 id 去重），任一非法则内核零变更。
  容量也在生效前整体试接纳：批次接纳到一半才超容量时整批回滚，
  错误带 `request_seq` 定位批中位置。
- 同地址请求 FIFO 排队（`RequestReport.queued=True`）；不同地址事务独立，
  可并存、可交错。
- 无需消息即可满足的请求（I/S 命中、I 上写、唯一共享者升级）在调用内同步
  完成，完成事件放在 `RequestReport.completions`。

### 网络投递（显式输入）与背压

- `kernel.deliver(msg_id: int)`：投递内核**在途**待发队列中的报文；
- `kernel.deliver(Message(None, txn_id, mtype, src, dst, addr, value=None))`：
  显式注入一条网络报文（`msg_id` 必须为 `None`），用于表达乱序 / 重复 /
  陈旧报文等外部输入。注入报文的事务 id 必须是内核已注册的事务。

每条有向通道（DIR→核c、核c→DIR）的在途消息数受 `channel_cap` 限制。
目标通道满时，内核新产生的报文**不丢、也不进通道**，而是带着已分配的
`msg_id` 保留在发送端保留区：它不可投递、不占通道、事务因此保持未完成。
当某条在途报文被 `deliver`（包括被判为 `stale` 的报文，因为它确实到达）
腾出该通道 1 个位置后，内核把保留区中该通道**最老的**报文按 FIFO 自动
续传进入在途通道；同通道严格按 `msg_id` 升序，不会插队。

返回 `DeliveryReport`：

| 字段 / status | 含义 |
|---|---|
| `status=applied` | 正常生效 |
| `status=duplicate` | 同一 `msg_id` 已投递过：幂等，无任何副作用 |
| `status=stale` | 报文不属于该地址当前活动事务（如陈旧 ACK/FLUSH）：**不推进新事务**，但仍腾出所在通道 |
| `emitted` | 本次处理新产生的报文（其中目标通道满的那些同时仍在保留区） |
| `released` | 本次腾出通道后，从保留区自动续传进入在途通道的报文 |

投递一个从不存在的 `msg_id`、或投递一条**仍在保留区**（通道满、尚未进入
在途通道）的 `msg_id`，都抛 `InvalidMessageError`（带 `msg_id`，后者信息
明确说明是背压保留、当前不可投递）。外部注入报文不占任何通道，因而也不
触发续传。

### 观测

- `kernel.snapshot() -> Snapshot`：每核缓存、每地址目录项（状态 / 共享者 /
  owner / 活动事务 / 等待 ACK 集合）、内存、在途待发消息、每地址 FIFO 队列、
  完成事件，以及本次新增的 **`channels`（非空有向通道及其在途消息）**、
  **`retained`（非空发送端保留区）**、**`limits`（当前容量配置）**。
  所有视图为只读拷贝。
- `kernel.pending()`（在途待发）、`kernel.retained_messages()`（背压保留）、
  `kernel.channel_load(src, dst)`（某通道在途数）、
  `kernel.completed()`、`kernel.limits`。
- `Message` / `Request` / `Completion` / `ChannelView` / `RetainedView`
  等数据结构均为 frozen dataclass。

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
6. **容量约束**：每地址等待队列长度 ≤ `queue_cap`；每核“活动事务 + 各地址
   队列中”的未完成请求数 ≤ `core_cap`；每条有向通道在途消息数 ≤
   `channel_cap`；同一 `msg_id` 不得同时出现在保留区与在途通道；有保留消息
   的通道必为满（否则保留消息应已续传，即不会卡死）。

## 错误语义

- 所有错误继承 `KernelError`：`InvalidRequestError` / `InvalidMessageError`
  带 `field`，批量错误还带 `request_seq`，可定位到具体参数与批次位置。
- **`CapacityExceededError`**：请求本身合法但接纳会超过 `queue_cap` 或
  `core_cap`；带 `field`（"queue" / "core_outstanding"）、`used`、`cap`，
  批次中触发还带 `request_seq`。**在任何状态变更之前抛出**，单请求不留部分
  准入；批量接纳在深副本上整体试接纳，触发即丢弃副本，原内核零变更。
- 整数参数（core / addr / value / txn_id / msg_id 等）**拒绝 `bool`**
  （`True/False` 不会被当作 1/0）。
- 数值输入拒绝 `NaN` / 正负 `Infinity`，普通 `float` 也拒绝（消息明确要求
  整数数据）；报错信息区分 NaN、Infinity、普通 float。
- load 不得带 value；store 必须带 int value；事务 id 必须为正整数且全生命
  周期唯一。
- 失败不留未说明的部分变更：批量请求先整体校验后（在副本上）试接纳；
  报文处理的有效性判定先于任何状态修改。

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

**容量不改变读值**另由 `test_capacity.py` 独立核对：在 2 核 2 地址、含跨核
写回 / 升级 / 排队交错的手算小场景上，分别枚举 `channel_cap=1`（紧）与
`channel_cap=8`（等价旧无容量）两种配置的全部消息重排，比较每个 load 的
读值集合，并把紧容量跑出的每条消息路径在宽容量内核上原样重放，确认紧
容量调度是宽容量合法调度的子集、且终态集合一致。

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
- **容量有界 + 发送端背压**：目录等待队列、每核未完成请求、每条有向通道
  在途消息都有正整数上限。请求超队列 / 每核上限直接拒绝（零半更新）；网络
  通道满采用**发送端保留**而非丢消息：报文在 `_send` 时即分配 `msg_id`
  并留底，通道腾出后按通道 FIFO（同通道 `msg_id` 升序）自动续传。容量只
  影响“何时能发”，不影响最终读值——紧容量内核的每条调度都是宽容量（等价
  旧无容量）模型的合法调度，反之亦然于可达终态，由枚举测试逐路径核对。
- 状态枚举按结构指纹去重（与消息 id 无关，并区分“在途 / 背压保留”两类
  消息），避免等价重排的组合爆炸；`all_paths=True` 可在小规模下保留全部
  原始路径。

## 已知限制

- 只实现题面契约的 **MSI 有限子集**：无 MESI/MOESI、无缓存间直传、无缓存
  行容量/替换写回（本次只给目录队列、每核请求、网络通道三类上限加边界，
  缓存本身仍可容纳全部 8 地址）、无分支/合并事务、无消息超时与重传协议。
  **不宣称兼容任何工业标准处理器的一致性实现。**
- 背压是“发送端保留 + 通道腾出自动续传”的确定性模型，不模拟真实延迟、
  仲裁优先级或流控信用协议；续传顺序固定为每通道 FIFO，不做公平性调度。
- 显式注入的报文（陈旧/乱序输入）不占通道、不参与 `explore` 的常规重排
  枚举，常规枚举只投递内核实际产生的报文；异常网络路径由定向单元测试覆盖。
- 穷举复杂度随并发消息数阶乘增长，`budget` 是硬保护；触发预算时结果中
  `exhausted=True`（枚举）或抛 `BudgetExhaustedError`（搜索），不得把它
  当成“无解”。
- 不提供持久化、远程接口、并发执行；所有操作在单线程内同步完成。
