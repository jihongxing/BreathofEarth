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

## 2026-06-23 前端浏览器验证

临时 QA 服务：

```bash
python -m uvicorn output.multi_strategy_frontend_qa_server:app --host 127.0.0.1 --port 8765
```

验证方式：

- 使用 QA-only FastAPI 服务，不启动正式数据调度器。
- 使用 Playwright 打开 `http://127.0.0.1:8765/`。
- 登录 QA mock 前端。
- 关闭家训阅读弹窗后等待 `#stage95-shadow-audit .stage95-card` 渲染。
- 检查 Stage 9.5 多策略面板文本、控制面和移动端布局。
- 用户完成手动页面验收并确认当前效果满意。

自动断言结果：

| 检查项 | 结果 |
|--------|------|
| 显示多策略影子观察标题 | PASS |
| 显示 `ATTENTION / 需人工关注` | PASS |
| 显示 `NOT_APPROVED / 未批准` | PASS |
| 显示 `UNAVAILABLE` | PASS |
| 显示 `production_90_10` | PASS |
| 显示只读提示 | PASS |
| Stage 9.5 面板内控制面数量 | `0` |
| 移动端 `innerWidth` | `390` |
| 移动端 `scrollWidth` | `390` |
| 移动端横向溢出 | PASS，无横向溢出 |

截图产物：

- `output/playwright/multi-strategy-stage95-desktop-fixed.png`
- `output/playwright/multi-strategy-stage95-mobile-fixed.png`

这些截图属于 ignored `output/` 下的本地 QA 产物，不提交仓库。

已修复的问题：

- 首轮移动端截图发现页面横向溢出。
- 根因是顶部导航和部分 Stage 9.5 卡片/grid 的最小宽度共同撑宽页面。
- 已通过 PR #12 修复移动端收缩规则、多策略表格容器和家训弹窗移动端可见性。

下一步：

1. 保持 Stage 9.5 多策略面板只读，不增加交易、杠杆或 Shadow 转 Live 控件。
2. 准备真实券商只读环境下的多策略 shadow 观察，但继续禁止订单提交。
3. 若接入真实 L1/L2 或保证金快照，先更新 runner/API 测试，再更新前端显示。
