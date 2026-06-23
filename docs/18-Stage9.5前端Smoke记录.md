# Stage 9.5 前端 Smoke 记录

> 本记录验证前端只读观察面板能正确展示离线 Stage 9.5 smoke 结果。
> 它不连接真实券商，不提交订单，不构成实盘杠杆批准。

## 2026-06-23

输入报告目录：

```bash
output/stage95-smoke
```

临时 QA 服务：

```bash
python -m uvicorn output.stage95_frontend_qa_server:app --host 127.0.0.1 --port 8765
```

验证方式：

- 使用系统 Chrome 运行 Playwright 浏览器检查。
- 登录 QA-only mock 前端。
- 关闭家训阅读弹窗后等待 `#stage95-shadow-audit .stage95-card` 渲染。
- 检查 Stage 9.5 面板文本和控件。

断言结果：

| 检查项 | 结果 |
|--------|------|
| 显示 `需人工关注` | PASS |
| 显示 `实盘杠杆: 未批准` | PASS |
| 显示 `Live leveraged execution NOT YET APPROVED` | PASS |
| 显示 `UNAVAILABLE` 保证金状态 | PASS |
| 显示 `WARNING` 影子账本状态 | PASS |
| 显示 `1/60` 观察覆盖率 | PASS |
| 显示券商不可用次数 | PASS |
| 显示只读提示，不提供交易/杠杆/Shadow 转 Live | PASS |
| Stage 9.5 面板内无 button/link/input/select/textarea/role=button/onclick | PASS |

截图产物：

- `output/playwright/stage95-smoke-desktop.png`
- `output/playwright/stage95-smoke-mobile.png`

这些截图属于本地 QA 产物，位于 ignored `output/` 目录，不提交仓库。

已知 QA-only 噪音：

- console 中出现 withdrawals 接口 404，因为临时 QA 服务没有实现资金管理 mock。
- 该错误发生在 `loadWithdrawals()`，不影响 Stage 9.5 面板渲染和治理断言。

结论：

前端 Stage 9.5 面板符合当前治理红线：

- 缺失券商保证金数据会显示为需人工关注。
- 离线 `UNAVAILABLE` 不会被渲染成安全。
- `live_leverage_approved` 保持未批准。
- 面板没有任何交易、加杠杆或 Shadow 转 Live 控件。

下一步：

1. 推送当前文档与 QA 记录提交。
2. 按 [IBKR 只读接入清单](D:/codeSpace/BreathofEarth/docs/19-IBKR%E5%8F%AA%E8%AF%BB%E6%8E%A5%E5%85%A5%E6%B8%85%E5%8D%95.md) 准备真实 IBKR 只读接入的环境变量和凭证。
3. 在真实只读环境下重复 Stage 9.5 smoke，但继续禁止订单提交。
