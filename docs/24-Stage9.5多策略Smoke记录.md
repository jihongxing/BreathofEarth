# Stage 9.5 多策略 Smoke 记录

> 本记录只证明本地多策略影子审计链路可运行。
> 它不连接真实券商，不提交订单，不构成实盘交易、实盘杠杆或 Shadow 转 Live 授权。

## 2026-06-23

运行目录：

```bash
data/shadow
```

命令：

```bash
python -m live.multi_strategy_shadow_runner --aum 2000000
python -m pytest tests/test_strategy_registry.py tests/test_strategy_audit_pipeline.py tests/test_multi_strategy_shadow_runner.py tests/test_strategy_audit_api.py tests/test_stage95_frontend_smoke.py -q
```

结果：

| 项目 | 结果 |
|------|------|
| Multi-strategy shadow runner | `ATTENTION` |
| Strategy count | `4` |
| Strategy ids | `benchmark_balanced_proxy / classic_permanent_portfolio / fixed_defensive_core / production_90_10` |
| Admission status | `NOT_APPROVED` |
| Margin snapshot status | `UNAVAILABLE` |
| Slippage audit status | `UNAVAILABLE` |
| `readonly` | `true` |
| `trading_disabled` | `true` |
| `human_review_required` | `true` |
| `live_leverage_approved` | `false` |
| API normalized status | `ATTENTION / warning` |
| API stale report | `false` |
| API warning count | `8` |
| Relevant tests | `37 passed` |

解释：

- 本次 smoke 使用本地 skeleton runner，不连接真实券商和 L1/L2 盘口。
- 因为没有真实当前持仓、真实盘口深度、真实保证金快照和完整分层审计证据，四套策略全部保持 `NOT_APPROVED`。
- `margin_snapshot` 和 `slippage_audit` 返回 `UNAVAILABLE` 是预期行为，不能被前端或 API 渲染成安全状态。
- API 归一化后仍强制保留 `readonly=true`、`trading_disabled=true`、`human_review_required=true`、`live_leverage_approved=false`。
- 本次 smoke 生成了 `data/shadow/latest_multi_strategy_shadow.json` 和时间戳报告；这些属于本地运行产物，不作为策略批准证据。

结论：

Stage 9.5 多策略影子审计平台第一版链路可运行：

- registry 能提供可观察策略集合。
- runner 能生成多策略目标权重和只读 shadow report。
- API 能读取 latest report，并在缺失真实执行证据时 fail closed。
- 前端 smoke contract 已锁定只读面板，不允许交易、杠杆或 Shadow 转 Live 控件。

项目判定不变：

```text
Research PASS / Production design APPROVED / Live leveraged execution NOT YET APPROVED
```

下一步：

1. 用本地临时 API 服务加载 `data/shadow/latest_multi_strategy_shadow.json`。
2. 在浏览器中验证 Stage 9.5 多策略面板显示 `ATTENTION / UNAVAILABLE / NOT_APPROVED`。
3. 若前端显示一致，再进入真实券商只读环境的多策略观察准备，但继续禁止订单提交。
