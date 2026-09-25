# msi_kernel — 消息驱动的 MSI 目录一致性内核（3 核 / 8 地址）

一个**纯离线、纯离散事件**的目录式缓存一致性消息内核可复用模块。不含真实
线程、不含真实网络：load/store/evict 请求与网络报文投递全部是显式函数调用，
调用方决定报文投递顺序，因此同一份核心可以被确定性地驱动、重放与穷举。
`evict(core, addr)` 主动逐出让上层可以模拟有限缓存容量下的替换行为。

## 运行环境

- **Python 3.14.7**（仅使用标准库；3.10+ 的语法也兼容，但以 3.14.7 为准）。
- **无需安装任何第三方依赖**，不需要账号 / 密钥 / 网络 / 硬件 / 数据库 /
  中间件 / 云 API / Docker。
- Linux 原生运行。

## 准确命令

```bash
# 必要测试（在仓库根目录执行；.venv 为仓库自带的 Python 3.14.7 环境）
./.venv/bin/python -m unittest discover -s tests -v

# 固定输入演示（实测约 0.1 秒内完成，上限约定为 90 秒）
./.venv/bin/python demo.py
```

## 仓库结构

```
msi_kernel/
  __init__.py    公开 API 汇总
  errors.py      错误类型（含可定位字段）
  model.py       协议内核：状态、请求准入、消息处理、排队与完成
  explore.py     消息重排枚举、不变量检查、有界搜索
tests/
  reference.py   独立参考模型（不导入、不调用被测核心来产生期望值）
  test_model.py        协议行为测试
  test_evict.py        主动逐出：I/S/M 语义、排队交错、陈旧/重复报文、重排枚举
  test_validation.py   输入校验 / 错误语义 / 批量原子性测试
  test_reference.py    与独立参考模型逐完成操作对照
  test_explore.py      枚举、预算耗尽 vs 无解 测试
demo.py          固定输入演示（失效链 + 重排枚举 + 异常边界 + 主动逐出）
```

## 规模与输入上限（接口边界）

| 项目 | 上限 / 取值 |
|---|---|
| 核数 `NUM_CORES` | 固定 3（核编号 0,1,2） |
| 地址数 `NUM_ADDRS` | 固定 8（地址编号 0..7） |
| 目录节点编号 `DIR` | 3（仅用于消息的 src/dst） |
| 数据值 | 任意 Python `int`（任意精度，无溢出语义） |
| 事务 id / 消息 id | 正整数；不显式给事务 id 时内核自动分配 |
| 穷举预算 | `explore` / `find_schedule` 的 `budget` 参数（默认 100000 个已访问状态） |

短演示不穷举上限规模；上限是接口边界，不是演示目标。

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

handle = k.evict(2, 0)                         # 主动逐出核2 的 S 行（模拟容量替换）
print(handle.completions)                      # () —— 通知未到目录，尚未完成
drain(k)                                       # 投递 EVICT_S 后逐出才完成
print(k.completed()[-1])                       # Completion(op=Op.EVICT, ...)
```

### 主动逐出（模拟有限缓存容量）

- `kernel.evict(core, addr, *, txn_id=None) -> RequestReport`，等价于
  `request(core, addr, Op.EVICT)`；`Op.EVICT` 请求不得带 `value`。
- 返回的 `RequestReport` 是**待完成句柄**：S/M 行逐出在通知/写回消息
  被显式 `deliver` 到目录之前，`completions` 为空、事务不出现在完成流中。
- 语义（本地行动作在准入时发生，跨节点动作全部走显式消息）：
  - **I 行**：无操作，调用内同步完成；
  - **S 行**：本地立即丢弃为 I，发 `EVICT_S` 请目录移除共享者，目录处理
    后事务才完成（最后一个共享者逐出后目录转 I）；
  - **M 行**：本地立即丢弃为 I，最新数据随 `EVICT_WB` 在途，目录收到写回、
    更新内存并释放 owner 后事务才完成（写回在途期间目录仍 M/owner=core，
    这是协议内过渡态）。
- 逐出与读写**纳入同一个地址的 FIFO 事务队列**：同地址读写、失效、逐出
  交错时严格按准入顺序串行；不同地址互不影响。逐出完成事件的 `value`
  为交回目录的值（M 行写回值；S/I 行为当前内存值）。
- 逐出的写回与通知都带原事务 id：重复投递幂等（`duplicate`），属于已结束
  事务的旧写回/旧通知被判定 `stale`——**旧逐出不会清掉后来重新获得的
  所有权**，陈旧报文也不会推进当前事务。

## 公开接口

### 请求输入

- `kernel.request(core, addr, op, *, value=None, txn_id=None) -> RequestReport`
  （`op` 为 `Op.LOAD` / `Op.STORE` / `Op.EVICT`）
- `kernel.evict(core, addr, *, txn_id=None) -> RequestReport`（逐出便捷入口）
- `kernel.request_batch(iterable_of_Request) -> list[RequestReport]`
  **原子**：整批先校验（含批内事务 id 去重），任一非法则内核零变更。
- 同地址请求（含逐出）FIFO 排队（`RequestReport.queued=True`）；不同地址
  事务独立，可并存、可交错。
- 无需消息即可满足的请求（I/S 命中、I 上写、唯一共享者升级、I 行逐出）在
  调用内同步完成，完成事件放在 `RequestReport.completions`。

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

投递一个从不存在的 `msg_id` 抛 `InvalidMessageError`（带 `msg_id`）。

### 观测

- `kernel.snapshot() -> Snapshot`：每核缓存、每地址目录项（状态 / 共享者 /
  owner / 活动事务 / 等待 ACK 集合）、内存、待发消息、每地址 FIFO 队列、
  完成事件。所有视图为只读拷贝。
- `kernel.pending()` / `kernel.completed()`。
- `Message` / `Request` / `Completion` 等数据结构均为 frozen dataclass。

### 消息类型（有限协议子集）

`FETCH`（目录→M owner，要求回写并降级）、`FLUSH`（owner→目录，带数据）、
`INV`（目录→共享者，要求失效）、`INV_ACK`（共享者→目录）、
`EVICT_S`（核→目录，S 行逐出通知）、`EVICT_WB`（核→目录，M 行逐出写回，
带数据）。

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
3. 目录 M：owner 存在，非 owner 缓存必须为 I；owner 正常为 M，FETCH 已
   送达而 FLUSH 在途时可为 S，M 逐出写回（EVICT_WB）在途时为 I（owner
   已本地丢弃，所有权在途，目录视图以 `evicting=True` 标识）；
4. 目录 S：无 M 缓存、owner 为空，每个持 S 的核都登记在共享者集合中
   （INV 已在核端生效、ACK 在途，或 S 逐出通知 EVICT_S 在途时，目录共享者
   集合可暂时多出核）；
5. 目录 I：全部缓存为 I，owner 为空。

## 错误语义

- 所有错误继承 `KernelError`：`InvalidRequestError` / `InvalidMessageError`
  带 `field`，批量错误还带 `request_seq`，可定位到具体参数与批次位置。
- 整数参数（core / addr / value / txn_id / msg_id 等）**拒绝 `bool`**
  （`True/False` 不会被当作 1/0）。
- 数值输入拒绝 `NaN` / 正负 `Infinity`，普通 `float` 也拒绝（消息明确要求
  整数数据）；报错信息区分 NaN、Infinity、普通 float。
- load 不得带 value；store 必须带 int value；事务 id 必须为正整数且全生命
  周期唯一。
- 失败不留未说明的部分变更：批量请求先整体校验后生效；报文处理的有效性判定
  先于任何状态修改。

## 数值与误差口径

本模块**不做任何浮点运算**：缓存数据、内存值、消息携带值全部是整数，状态机
是精确离散转移。因此不存在数值误差/容差问题，也**不使用任何宽容差**；所谓
“误差口径”即为**逐位精确相等**。NaN/Infinity 作为非法输入被拒绝。

## 独立参考模型如何验证

`tests/reference.py` **不导入 `msi_kernel`**，只对完成事件
`(addr, op, value)` 序列运行一个平凡的“每地址最后写入”抽象机：同地址操作
严格按准入 FIFO 完成，故某 load 的允许读值唯一等于该地址在它之前最后一次
store 的写入值（此前无 store 则为初始内存值）；evict 完成事件不改变抽象
内存（M 逐出写回的恰是最后存入值，S/I 逐出不写内存），参考机直接跳过。
测试再用内核实际跑出的完成序列与该允许值逐完成操作对照，并在 300 个随机
（确定性 LCG 生成，含 load/store/evict 混合）小场景的全部消息重排上复核，
同时断言每条请求（含逐出）恰好完成一次。内核快照的自洽检查只作为补充
断言，不用核心给自己算期望值。

## 设计取舍

- **固定 3 核 8 地址**：规模是题面契约的一部分，不做任意规模泛化。
- **消息顺序完全交给调用方**：内核不自动投递、不丢消息、不模拟延迟，便于
  穷举重排与复现；`drain` 只是提供一个确定性便利驱动。
- **FETCH 采用“回写后共享”**：owner 收到 FETCH 降级为 S 并回 FLUSH，数据
  写入目录内存后再分发给请求者；不实现缓存间直接传数（没有 Data/DATA 消息）。
- **逐出采用“本地先丢弃、目录后确认”**：evict 准入时核端行立即转 I（本地
  动作），跨节点的共享者移除 / 脏数据写回全部走显式消息；事务以目录处理
  完通知/写回为完成点，因此写回在途期间目录仍登记旧 owner（所有权在途），
  句柄不会提前报告完成。内核本身不建模缓存容量与替换策略——容量上限由
  上层在合适时机调用 `evict` 来表达。
- **同地址严格串行（FIFO）**，不同地址无锁独立；事务 id 与消息 id 全程唯一，
  幂等以 `msg_id` 身份判定，陈旧性以“报文事务 id 是否等于该地址当前活动
  事务 + 事务种类与等待集合是否匹配”判定。
- **注入式网络输入**：除投递内核产生的待发报文外，允许调用方显式构造
  `Message` 注入，使“陈旧 ACK / 重复报文”这类异常网络情形可直接表达和测试。
- 状态枚举按结构指纹去重（与消息 id 无关），避免等价重排的组合爆炸；
  `all_paths=True` 可在小规模下保留全部原始路径。

## 已知限制

- 只实现题面契约的 **MSI 有限子集**：无 MESI/MOESI、无缓存间直传、无分支/
  合并事务、无消息超时与重传协议。**不宣称兼容任何工业标准处理器的一致性
  实现。**
- 内核不建模真实缓存硬件：没有组相联、替换策略或队列容量上限；`evict`
  只是给上层一个表达“容量替换”的显式请求入口，何时逐出完全由调用方决定。
- 显式注入的报文（陈旧/乱序输入）不参与 `explore` 的常规重排枚举，常规
  枚举只投递内核实际产生的报文；异常网络路径由定向单元测试覆盖。
- 穷举复杂度随并发消息数阶乘增长，`budget` 是硬保护；触发预算时结果中
  `exhausted=True`（枚举）或抛 `BudgetExhaustedError`（搜索），不得把它
  当成“无解”。
- 不提供持久化、远程接口、并发执行；所有操作在单线程内同步完成。
