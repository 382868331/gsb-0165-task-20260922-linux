# 目录式 MSI 缓存一致性消息内核

一个**纯消息驱动、纯步进、可导入**的目录式 MSI 缓存一致性模型库：
3 个核、8 个地址、整数数据；无真实线程、无真实网络、无时钟、无随机数、
无第三方依赖。所有状态迁移都由“发请求 / 显式投递一条消息”两类输入驱动，
消息顺序由调用方完全掌控，可用于协议教学、重排枚举与一致性性质检验。

- Python 版本：**3.14.7**（仅用标准库，3.10+ 语法亦可运行，未在其他版本验证）
- 依赖：**无**。安装 Python 后即可直接运行。
- 离线：核心、测试、演示全程不访问网络/账号/密钥/硬件/数据库/云 API/Docker。

## 目录

```
msi_kernel/
  errors.py    # KernelError、BudgetExhausted 与输入校验（拒 bool、拒 NaN/Inf）
  messages.py  # MsgType、Message（不可变，构造时全字段校验）、make_request
  kernel.py    # CoherenceKernel：核心状态机（缓存、目录、事务、队列、幂等）
  reorder.py   # 小消息重排穷举枚举与允许读值汇总（验收用）
tests/
  oracle_ref.py# 独立参考模型：枚举每核程序序的全部线性化（不调用被测核心）
  test_kernel.py
  test_reorder.py
demo.py        # 固定输入演示：失效链 + 重排枚举 + 实际触发的拒绝边界
```

## 运行

```bash
python -m unittest discover -s tests -v   # 必要测试（约 5 秒）
python demo.py                            # 固定输入演示（约 2 秒，真实计算）
```

两条命令都不需要安装任何额外包。

## 协议子集（本题契约）

目录按地址记录状态 `I/S/M`、共享者集合（S 时）与 owner（M 时）。

| 消息 | 方向 | 含义 |
|---|---|---|
| `GETS` | 核→目录 | load 缺失（缓存 I） |
| `GETX` | 核→目录 | 写缺失（缓存 I），携带写入值 |
| `UPGR` | 核→目录 | S→M 升级，携带写入值 |
| `INV` | 目录→核 | 作废某 S 副本（带每地址递增的 `aid`） |
| `FETCH` | 目录→核 | 要求 M owner 回写并降为 S（服务读） |
| `FETCHX` | 目录→核 | 要求 M owner 回写并作废（服务写） |
| `DATA` / `DATA_M` | 目录→核 | 以 S / M 送达数据 |
| `UPGR_ACK` | 目录→核 | 升级许可（请求者以自己的写入值进入 M） |
| `FLUSH` | 核→目录 | owner 回写数据 |
| `INV_ACK` | 核→目录 | 失效完成确认（带 `aid`） |
| `GRANT_ACK` | 核→目录 | 授权送达确认（同地址队列的跨事务屏障） |

规则：

- **升级必须等待全部失效 ACK**：目录对除请求者外的每个 S 共享者发 `INV`，
  收齐 `INV_ACK` 后才发 `DATA_M`/`UPGR_ACK`。
- **读 M 地址必须先回写**：目录向 owner 发 `FETCH`，owner 回 `FLUSH`
  并降为 S，目录把数据 `DATA` 给请求者，二者共享。
- **写 M 地址**：发 `FETCHX`，owner 回写并作废，请求者以 M 获得数据。
- **同地址请求严格 FIFO 排队；不同地址事务完全独立**。队列用
  `GRANT_ACK` 作跨事务屏障：前一事务的授权送达确认到达前，不启动同地址
  下一事务，避免“数据/失效消息乱序到达”的竞争。
- 排队期间若一个 `UPGR` 请求者被前序写事务失效，目录在该事务轮到它时
  自动按其当前缓存状态改走 `GETX`（owner 回写路径）。这是本子集对
  “排队升级被作废”的固定处理方式。
- 本地 S/M 读命中、M 写命中通过 `local_load`/`local_store` 立即完成，
  不产生消息。

## 接口

```python
from msi_kernel import CoherenceKernel, Message, MsgType, KernelError

k = CoherenceKernel(memory=[0]*8)          # 8 个整数初值，默认全 0
k.request("GETX", xid=1, core=0, addr=0, value=10)   # 返回新产生的消息列表
k.request("GETS", xid=2, core=1, addr=0)            # GETS 不带 value
k.request("UPGR", xid=3, core=1, addr=0, value=20)  # 升级必须带 value
# 或 k.load(xid, core, addr) / k.store(xid, core, addr, value)（按缓存状态选类型）

k.pending_messages()          # 待发消息（显式“网络”队列）
k.deliver(message)            # 投递 Message，或已发出消息日志的下标
k.snapshot()                  # 缓存/目录/待发消息/完成读值/各地址队列（不可变视图）
k.clone()                     # 深拷贝（重排枚举分支用，无并发）
k.check_invariants()          # 至多一个 M、M 时无 S 等；违例 AssertionError
k.is_quiescent()              # 无开放事务且无待发消息
```

`snapshot()` 返回：

- `caches[core][addr]` → `CacheLineView(state, value)`；
- `directory[addr]` → `DirLineView(state, sharers:frozenset, owner, value:mem)`；
- `pending_messages`：待发消息元组；
- `completed`：`CompletedOp(kind, core, addr, xid, value)`，
  其中 LOAD 的 `value` 即**完成读值**，STORE 的 `value` 为 `None`；
- `queued_xids[addr]`：每地址排队中的事务 xid。

重排枚举（独立于生产路径的验收工具）：

```python
from msi_kernel.reorder import RequestSpec, explore
program = [RequestSpec("STORE", 0, 0, 10), RequestSpec("LOAD", 1, 0), ...]
result = explore(program, memory=None, budget=100_000)
# result.visited / terminals / violations / load_values / completion_count / sample_traces
```

程序按核分成“每核有序链”：同核操作保序、跨核发出顺序任意；枚举
“发下一条请求（任意核）/ 投递任意一条待发消息”的全部交错，
每个状态都检查不变量，并汇总每条 LOAD 在所有重排下的允许读值集合。

## 错误语义（固定）

- 所有非法输入抛 `KernelError`（`ValueError` 子类），信息以参数名开头，
  可定位；**不做静默纠正/自动改类型**。
- **整数参数拒绝 `bool`**（`True/False` 不是合法整数）。
- **数值输入拒绝 `NaN`/`+Inf`/`-Inf`**（本模型数据与参数均为整数，
  浮点本身也拒绝；该校验使错误信息明确）。
- 核号 0..2、地址 0..8-1、xid ≥ 0 等超界均拒绝；内存必须恰为 8 个整数。
- 请求类型与缓存状态不符（如非 I 发 `GETS/GETX`、非 S 发 `UPGR`）拒绝；
  同一（核, 地址）有未完成事务时拒绝第二个请求（同一核对不同地址可以
  各有一个开放事务）；xid 在会话内唯一。
- **幂等**：键 `(type,xid,src,dst,addr,value,aid)` 相同的消息重复投递为空操作。
  属于已结束/不存在事务、或 `aid` 对不上的消息是**陈旧/伪造消息**（不是重复），
  抛 `KernelError` 且不推进任何事务。
- **失败原子性**：请求/投递在抛错前不改变任何状态（所有校验先于状态写入）。
- **预算耗尽 ≠ 证明无解**：枚举访问数达到 `budget` 抛 `BudgetExhausted`
  （`RuntimeError` 子类），它既不表示性质成立也不表示发现反例，只表示搜索
  未完成；正常枚举结束时 `ExplorationResult.complete=True` 才是有界穷举结论。

## 输入上限（接口边界）

- 固定 **3 核、8 地址**（常量 `NUM_CORES=3`、`NUM_ADDR=8`，非可配置参数）。
- 数据/写入值为任意 Python `int`（无溢出概念，不是有限位宽整数）。
- 消息、事务数量无固定上限，仅受进程内存限制；`xid` 为非负整数。
- 枚举器的 `budget` 是搜索访问状态数上限（演示/测试用 ≤ 500000，
  7 操作程序约 1.7 万个状态、约 1.5 秒），不是协议本身的规模上限。
- 短演示不穷举上限规模。

## 独立参考与验证口径

- `tests/oracle_ref.py` 是**与被测核心无任何共享代码**的独立参考：它枚举
  3 条每核操作链的全部拓扑序（线性化），用最简单的全局内存重放计算每条
  LOAD 的允许值集合。测试断言“内核在所有消息重排下观察到的读值集合”
  恰好等于该独立参考集合（双向相等），并且每个操作在每个终态恰好完成一次。
- “往返一致”（clone/canonical 去重、重放复现）只作补充检查，不作为期望值来源。
- 缓存级不变量在枚举的每个状态检查：每地址至多一个 M，且 M 不与 S 共存；
  目录级状态按事务阶段精确判定，消息在途的单拍瞬态按协议明确允许。
- **无浮点计算**，因此不存在数值误差/容差问题；所有比较均为整数/状态精确相等。

## 设计取舍

- 目录与核都在同一确定性进程内步进；“网络”就是待发消息列表，延迟与乱序
  完全由调用方选择投递顺序来表达，避免任何真实并发不确定性。
- owner 回写后内存即被更新；`DATA_M` 携带的是请求者自己的写入值（而非内存
  旧值），因此脏 owner 回写旧值与新写者持新值可以正确共存。
- 用 `GRANT_ACK` 作为同地址队列屏障，换取非常简单的无竞争队列推进；代价是
  多一条每事务确认消息。
- 失效用每地址单调 `aid` 编号，使每个 `INV_ACK` 可区分、可幂等判定，
  也让“陈旧 ACK 推进新事务”在结构上不可能。

## 已知限制（不宣称完整行业标准兼容）

- 只实现本题描述的 MSI 子集：无 MESI/MOESI、无共享缓存、无替换策略、
  无驱逐消息、无访存字节掩码、无超时/重传。
- 不实现真正的“排队升级被作废后挂起重试”，而是固定改走 `GETX` 回写路径。
- 同一（核, 地址）同一时刻只允许一个未完成请求；同一核对不同地址的
  请求可以并行开放（无每核总 outstanding 上限，但每核每地址单 miss）。
- 内存按字（一个整数）编址，8 个字；无 Cache 容量/相联度概念。
- 枚举器是教学/验收用的朴素状态空间搜索，只适合小消息程序；它不是模型
  检查器，不做对称性归约或偏序缩减。
- 不保证 3.14.7 以外 Python 版本经过验证。
