# Stage 9.5 影子观察运行手册

> 本手册用于连续 60 个交易日运行 `Shadow Sync & Structural Audit`。
> 它不是实盘交易授权，也不是 Portfolio Margin 杠杆批准。

## 一、当前结论

项目当前状态保持不变：

- `Research PASS`
- `Production design APPROVED`
- `Live leveraged execution NOT YET APPROVED`

Stage 9.5 的目标只有一个：在不提交真实订单的前提下，用真实时间、真实或本地报价、真实或缺失的券商保证金字段，审计 `90% 防御核心 + 10% 现代贝塔卫星` 这套生产候选账本是否能被稳定观察。

任何观察结果都不能自动升级为实盘杠杆批准。即使 60 个交易日全部通过，结论也只能是“允许进入人工评审”，不是“允许自动交易”。

## 二、系统边界

允许：

- 读取本地 Yahoo Adj Close 价格。
- 只读连接券商，读取报价、账户快照和保证金字段。
- 生成 shadow target、shadow orders 和观察报告。
- 通过 FastAPI 和前端展示只读观察状态。

禁止：

- 提交真实订单。
- 撤销真实订单。
- 从 Shadow 一键切到 Live。
- 一键加杠杆。
- 将 `UNAVAILABLE / WARNING / FAIL_CLOSED / STALE / ATTENTION` 渲染成安全状态。
- 把 Portfolio Margin 理论缓冲解释成真实券商安全承诺。

## 三、每日运行流程

### 1. 离线观察模式

用于本地开发、无券商接入、CI 或故障演练。

```bash
python -m live.stage95_shadow_runner --aum 2000000 --no-broker --skip-db
python -m live.stage95_observation_summary --shadow-dir data/shadow --expected-cycles 60
```

预期行为：

- `shadow_sync` 使用本地 clean adjusted-close 数据。
- `margin_monitor` 返回 `UNAVAILABLE`，因为没有真实券商保证金快照。
- 前端必须显示需要关注，不能显示为安全。
- `live_leverage_approved` 必须始终为 `false`。

离线模式只证明脚本链路可运行，不证明券商环境可用。

### 2. 券商只读观察模式

用于真实 Stage 9.5 观察期。必须先确认券商适配器以 `READ_ONLY` 模式创建。

```bash
python -m live.stage95_shadow_runner --aum 2000000 --broker ibkr --skip-db
python -m live.stage95_observation_summary --shadow-dir data/shadow --expected-cycles 60
```

如果需要写入本地数据库，移除 `--skip-db`：

```bash
python -m live.stage95_shadow_runner --aum 2000000 --broker ibkr
```

预期行为：

- `shadow_sync` 尝试读取券商报价和账户快照。
- 若券商报价不可用，允许回退到本地 clean adjusted-close，但必须写入 warning。
- `margin_monitor` 尝试读取 `NetLiquidation / ExcessLiquidity / FullMaintainMarginReq`。
- 保证金字段缺失时状态必须是 `UNAVAILABLE` 或 `PARTIAL`。
- 报告仍然不能批准实盘杠杆。

## 四、输出文件

每日周期输出到 `data/shadow/`：

| 文件 | 用途 |
|------|------|
| `shadow_sync_YYYYMMDD_HHMMSS.json` | 单次影子账本观察 |
| `latest_shadow_sync.json` | 最新影子账本观察 |
| `margin_snapshot_YYYYMMDD_HHMMSS.json` | 单次保证金快照观察 |
| `latest_margin_snapshot.json` | 最新保证金快照观察 |
| `stage95_cycle_YYYYMMDD_HHMMSS.json` | 单次完整 Stage 9.5 周期 |
| `latest_stage95_cycle.json` | 最新完整 Stage 9.5 周期 |
| `stage95_observation_summary_YYYYMMDD_HHMMSS.json` | 观察期汇总 |
| `latest_stage95_observation_summary.json` | 最新观察期汇总 |

`data/shadow/` 是运行产物，不应作为普通研究输入提交。需要复现研究输入时，使用 `data/audit_snapshots/<date>-<source>/`。

## 五、状态解释

### 1. 周期状态

| 状态 | 含义 | 操作 |
|------|------|------|
| `HEALTHY` | 本周期脚本和组件均正常观察 | 记录即可 |
| `ATTENTION` | 有 warning、券商不可用、字段缺失或组件异常 | 当日标记人工关注 |
| `CRITICAL` | 组件异常、fail-closed 或严重错误 | 停止推进，先排查 |
| `MISSING` | 周期报告缺失或无法形成 | 补跑或记录缺失原因 |

### 2. 汇总状态

| 状态 | 含义 | 操作 |
|------|------|------|
| `COLLECTING` | 已有观察，但不足 60 个周期 | 继续观察 |
| `OBSERVED` | 观察周期达到要求，且无当前异常 | 可以进入人工评审 |
| `ATTENTION` | 观察期内存在异常或券商不可用 | 整理异常解释后再评审 |
| `STALE` | 最新周期超过过期阈值，默认 24 小时 | 不能继续评审，先恢复运行 |
| `CRITICAL` | 观察期内存在严重异常 | 不进入实盘讨论 |
| `MISSING` | 没有可用周期 | 从第 1 天重新开始 |

## 六、每日人工检查清单

每天运行后检查：

- `latest_stage95_cycle.json` 是否生成。
- `latest_stage95_observation_summary.json` 是否生成。
- `live_leverage_approved` 是否仍为 `false`。
- `trading_disabled` 是否为 `true`。
- `dry_run` 是否为 `true`。
- `shadow_sync.status` 是否为 `OK` 或带有可解释 warning。
- `margin_snapshot.status` 是否为 `OBSERVED`，或明确显示 `UNAVAILABLE / PARTIAL`。
- `slippage_audit.max_observed_half_spread_bps` 是否异常高。
- `margin_field_coverage` 是否持续覆盖核心字段。
- 前端 Stage 9.5 面板是否清楚显示 warning、stale 和不可用状态。

如果任何一项缺失，不要补写“安全”结论。只记录事实。

## 七、60 个交易日评审门槛

满足以下条件，才允许进入人工评审会议：

- `observed_cycles >= 60`。
- 最新报告未 stale。
- `latest_stage95_observation_summary.live_leverage_approved == false`。
- `critical_cycles == 0`。
- 没有无法解释的 `FAIL_CLOSED`。
- 最近一个周期不是 `MISSING / STALE / CRITICAL`。
- 券商只读模式下，`NetLiquidation / ExcessLiquidity / FullMaintainMarginReq` 核心字段覆盖率达到或接近 100%。
- 任何 `UNAVAILABLE / PARTIAL / ATTENTION` 都有日期、原因、影响范围和处理记录。
- 最大半价差没有系统性超过回测中的 50bp panic 假设；若超过，必须作为单独风险项评审。

这些门槛只允许“进入人工评审”。它们不产生实盘交易授权。

## 八、异常处理规则

### 1. 券商不可达

处理：

- 不推导保证金安全。
- 当日记录为 `UNAVAILABLE`。
- 如果连续不可达，观察期继续累计异常 streak。
- 恢复后检查字段覆盖率是否回到正常。

### 2. 保证金字段缺失

处理：

- 状态必须是 `PARTIAL` 或 `UNAVAILABLE`。
- 不允许用账户净值、现金或买力自行估算安全。
- 记录缺失字段名和券商原始返回。

### 3. 本地价格异常

处理：

- 非正价格、空价格或日期错位必须 fail closed。
- 不允许用 AkShare 美股 `qfq` 价格替代长周期 US ETF clean Adj Close。
- 需要复现时，只使用批准的 `data/audit_snapshots/`。

### 4. 滑点异常

处理：

- 如果半价差接近或超过 50bp，标记当日需要人工关注。
- 如果多日连续异常，暂停讨论实盘执行。
- 不用单日正常半价差推导极端市场一定可成交。

### 5. 前端显示异常

处理：

- 如果 API 返回 warning 但前端没有展示，前端视为阻塞问题。
- 如果前端把缺失数据显示成安全，视为治理红线问题。
- 前端不得出现交易、加杠杆或 Shadow 转 Live 控件。

## 九、人工评审材料

60 个交易日后，评审材料至少包括：

- `latest_stage95_observation_summary.json`。
- 60 个 `stage95_cycle_*.json`。
- 所有 `ATTENTION / CRITICAL / STALE / UNAVAILABLE / PARTIAL` 日期清单。
- 最大和平均半价差 bps。
- 保证金字段覆盖率。
- 券商不可用总次数和最大连续不可用天数。
- 前端截图，包含正常、缺失、不可用和 warning 状态。
- 是否存在任何真实交易方法调用的审计确认。

评审结论只能在人工会议中形成。代码和前端不得自动生成“准许实盘杠杆”的结论。

## 十、下一步

当前最合理的后续任务：

1. 用本手册跑一次本地离线 smoke cycle，确认 `data/shadow/latest_*` 报告和前端显示一致。
2. 按 [IBKR 只读接入清单](D:/codeSpace/BreathofEarth/docs/19-IBKR%E5%8F%AA%E8%AF%BB%E6%8E%A5%E5%85%A5%E6%B8%85%E5%8D%95.md) 准备环境变量和凭证，但不启用订单提交。
3. 连续 60 个交易日执行 Stage 9.5 观察，再进入人工评审。
