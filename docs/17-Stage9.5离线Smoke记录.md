# Stage 9.5 离线 Smoke 记录

> 本记录只证明本地离线观察链路可运行。
> 它不证明券商环境可用，也不构成实盘交易或实盘杠杆授权。

## 2026-06-23

运行目录：

```bash
output/stage95-smoke
```

命令：

```bash
python -m live.stage95_shadow_runner --aum 2000000 --no-broker --skip-db --output-dir output/stage95-smoke
python -m live.stage95_observation_summary --shadow-dir output/stage95-smoke --output-dir output/stage95-smoke --expected-cycles 60
```

结果：

| 项目 | 结果 |
|------|------|
| Stage 9.5 cycle | `ATTENTION` |
| Shadow sync | `WARNING` |
| Margin snapshot | `UNAVAILABLE` |
| Observation summary | `ATTENTION` |
| Observed cycles | `1 / 60` |
| Broker unavailable cycles | `1` |
| API audit status | `ATTENTION / warning` |
| API summary status | `ATTENTION / warning` |
| `requires_attention` | `true` |
| `live_leverage_approved` | `false` |

解释：

- 离线模式没有真实券商连接，`margin_monitor` 返回 `UNAVAILABLE` 是预期行为。
- 离线模式没有真实持仓快照，`shadow_sync` 只能生成目标名义本金，不生成 shadow orders。
- 只读 API 正确把缺失券商保证金数据归一化为需要人工关注。
- 本次 smoke 不写数据库，不连接券商，不提交订单。

下一步：

1. 用临时 `XIRANG_SHADOW_AUDIT_DIR` 启动本地 API。
2. 在浏览器中验证 Stage 9.5 面板显示 `需人工关注` 和 `实盘杠杆: 未批准`。
3. 清理或保留 `output/stage95-smoke` 作为本地 QA 产物，不提交到仓库。
