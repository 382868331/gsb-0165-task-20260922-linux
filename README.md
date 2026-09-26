# msi_kernel — 消息驱动的 MSI 目录一致性内核（3 核 / 8 地址）

一个**纯离线、纯离散事件**的目录式缓存一致性消息内核可复用模块。不含真实
线程、不含真实网络：load/store 请求与网络报文投递全部是显式函数调用，
调用方决定报文投递顺序，因此同一份核心可以被确定性地驱动、重放与穷举。

## 运行环境

- **Python 3.14.7**（仅使用标准库；3.10+ 的语法也兼容，但以 3.14.7 为准）。
- **无需安装任何第三方依赖**，不需要账号 / 密钥 / 网络 / 硬件 / 数据库 /
  中间件 / 云 API / Docker。
- Linux 原生运行。

建议使用内置 `venv`：

```bash
python3.14 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip   # 可选；本项目仍不需要第三方依赖
```

## 准确命令

```bash
# 必要测试（在仓库根目录执行）
python -m unittest discover -s tests -v

# 原整行模式固定输入演示（约 0.1 秒内完成，上限约定为约 8 秒）
python demo.py

# 新增固定四扇区模式演示（现场合成交错计算，要求 90 秒内完成）
python demo_iteration.py
```

## 仓库结构

```
msi_kernel/
  __init__.py    公开 API 汇总
  errors.py      错误类型（含可定位字段）
  model.py       协议内核：状态、请求准入、消息处理、排队与完成
  explore.py     消息重排枚举、不变量检查、有界搜索
demo_iteration.py 固定四扇区模式演示：独立扇区写、跨区原子读、乱序 ACK
tests/
  reference.py   独立参考模型（不导入、不调用被测核心来产生期望值）
  test_model.py        协议行为测试
  test_validation.py   输入校验 / 错误语义 / 批量原子性测试
  test_reference.py    与独立参考模型逐完成操作对照
  test_explore.py      枚举、预算耗尽 vs 无解 测试
demo.py          固定输入演示（失效链 + 重排枚举 + 异常边界）
```

## 规模与输入上限（接口边界）

| 项目 | 上限 / 取值 |
|---|---|
| 核数 `NUM_CORES` | 固定 3（核编号 0,1,2） |
| 地址数 `NUM_ADDRS` | 固定 8（地址编号 0..7） |
| 扇区数 `NUM_SECTORS` | 固定 4（扇区编号 0..3；掩码位 0..3） |
| 默认模式 | 原整行模式，等价于四扇区掩码 `15`（`ALL_SECTORS`） |
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
```

### 固定四扇区模式

构造内核时显式传入 `sector_mode=True`；默认仍然是原整行模式。
`sectors` 是非零 4 位掩码：`1/2/4/8` 表示单个扇区，`3` 表示同时请求
扇区 0 和 1。单扇区完成值仍为 `int`；跨多个扇区的 load 返回长度 4 的
`tuple`，未请求位置填 0。

```python
k = CacheCoherenceKernel(
    sector_mode=True,
    sector_values={(0, 0): 100, (0, 1): 200},
)
k.request(0, 0, Op.STORE, value=101, sectors=0b0001)
k.request(1, 0, Op.STORE, value=202, sectors=0b0010)
k.request(2, 0, Op.LOAD, sectors=0b0011)       # 两扇区在同一事务授权
drain(k)
print(k.completed()[-1].value)                # (101, 202, 0, 0)
```

每个地址的每个扇区独立记录 MSI 状态、共享者、脏 owner 和数据版本。
不相交扇区上的事务可以并存推进；与未完成事务掩码重叠的请求进入 FIFO 队列。
跨多个扇区的读只有在全部所需扇区都满足条件时才一次性完成，因此不会观察到
“一个扇区是旧值、另一个扇区是未授权新值”的半更新状态。

## 公开接口

### 请求输入

- `kernel.request(core, addr, op, *, value=None, txn_id=None) -> RequestReport`
- 扇区模式可传 `sectors`：`kernel.request(..., sectors=0b0011)`；默认模式
  只接受全掩码 `ALL_SECTORS=15`。
- `kernel.request_batch(iterable_of_Request) -> list[RequestReport]`
  **原子**：整批先校验（含批内事务 id 去重），任一非法则内核零变更。
- 同地址请求 FIFO 排队（`RequestReport.queued=True`）；不同地址事务独立，
  可并存、可交错。
- 无需消息即可满足的请求（I/S 命中、I 上写、唯一共享者升级）在调用内同步
  完成，完成事件放在 `RequestReport.completions`。

### 网络投递（显式输入）

- `kernel.deliver(msg_id: int)`：投递内核待发队列中的报文；
- `kernel.deliver(Message(None, txn_id, mtype, src, dst, addr, value=None))`：
  显式注入一条网络报文（`msg_id` 必须为 `None`，`sectors` 为扇区掩码），
  用于表达乱序 / 重复 / 陈旧报文等外部输入。注入报文的事务 id 必须是内核
  已注册的事务，且扇区必须仍由该事务等待。

返回 `DeliveryReport`：

| status | 含义 |
|---|---|
| `applied` | 正常生效 |
| `duplicate` | 同一 `msg_id` 已投递过：幂等，无任何副作用 |
| `stale` | 报文不属于该地址当前活动事务（如陈旧 ACK/FLUSH）：**不推进新事务** |

投递一个从不存在的 `msg_id` 抛 `InvalidMessageError`（带 `msg_id`）。

### 观测

- `kernel.snapshot() -> Snapshot`：默认模式提供每核整行缓存、目录、内存；
  扇区模式还提供每个 `CacheView.sectors` 与 `DirectoryView.sectors`，包含
  每扇区状态、共享者、owner、活动事务、等待回写/ACK 和版本。所有视图为
  只读拷贝。
- `kernel.pending()` / `kernel.completed()`。
- `Message` / `Request` / `Completion` 等数据结构均为 frozen dataclass。

### 消息类型（有限协议子集）

`FETCH`（目录→M owner，要求回写并降级）、`FLUSH`（owner→目录，带数据）、
`INV`（目录→共享者，要求失效）、`INV_ACK`（共享者→目录）。扇区模式中所有
消息都携带 `sectors` 掩码；ACK/FLUSH 只能消除对应事务、对应核、对应扇区的
等待。扇区模式的 `FLUSH.value` 是长度 4 的整数 tuple。

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

扇区模式额外对每个 `(addr, sector)` 检查：至多一个 M；M 不与其他核的 S
共存；目录 S/M/I 与缓存、共享者、owner 一致；同一事务绑定的扇区集合在整个
等待期间保持原子。`explore` 在每个可达前缀上调用上述检查。

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

`tests/reference.py` **不导入 `msi_kernel`**。整行模式只对完成事件
`(addr, op, value)` 序列运行“每地址最后写入”抽象机；扇区模式使用独立的
逐 `(addr, sector)` 串行账本，store 只更新其掩码覆盖的位置，跨扇区 load
逐请求扇区核对。测试再用内核实际跑出的完成序列与账本逐完成操作对照，并在
确定性合成交错和有界消息重排上复核。内核快照的自洽检查只作为补充断言，
不用核心给自己算期望值。

## 设计取舍

- **固定 3 核 8 地址**：规模是题面契约的一部分，不做任意规模泛化。
- **消息顺序完全交给调用方**：内核不自动投递、不丢消息、不模拟延迟，便于
  穷举重排与复现；`drain` 只是提供一个确定性便利驱动。
- **FETCH 采用“回写后共享”**：owner 收到 FETCH 降级为 S 并回 FLUSH，数据
  写入目录内存后再分发给请求者；不实现缓存间直接传数（没有 Data/DATA 消息）。
- **同地址严格串行（FIFO）**，不同地址无锁独立；事务 id 与消息 id 全程唯一，
  默认整行模式下同一行严格 FIFO；扇区模式下只有掩码重叠的请求必须串行，
  不相交扇区可以独立推进。幂等以 `msg_id` 身份判定，陈旧性以“报文事务 id、
  核、扇区掩码是否正是当前等待项”判定。
- **四扇区为固定模型而非通用位宽目录**：扇区号固定 0..3；单扇区 store 写
  入一个整数，跨扇区 store 不提供“每扇区不同值”的打包写接口。
- **注入式网络输入**：除投递内核产生的待发报文外，允许调用方显式构造
  `Message` 注入，使“陈旧 ACK / 重复报文”这类异常网络情形可直接表达和测试。
- 状态枚举按结构指纹去重（与消息 id 无关），避免等价重排的组合爆炸；
  `all_paths=True` 可在小规模下保留全部原始路径。

## 已知限制

- 只实现题面契约的 **MSI 有限子集**：无 MESI/MOESI、无缓存间直传、无替换
  写回（容量无限）、无分支/合并事务、无消息超时与重传协议。**不宣称兼容
  任何工业标准处理器的一致性实现。**
- 扇区模式不模拟真实硬件的扇区宽度、部分写使能时序或互联仲裁；它验证的是
  离散消息顺序下的权限、版本和完成原子性。
- 显式注入的报文（陈旧/乱序输入）不参与 `explore` 的常规重排枚举，常规
  枚举只投递内核实际产生的报文；异常网络路径由定向单元测试覆盖。
- 穷举复杂度随并发消息数阶乘增长，`budget` 是硬保护；触发预算时结果中
  `exhausted=True`（枚举）或抛 `BudgetExhaustedError`（搜索），不得把它
  当成“无解”。
- 不提供持久化、远程接口、并发执行；所有操作在单线程内同步完成。
