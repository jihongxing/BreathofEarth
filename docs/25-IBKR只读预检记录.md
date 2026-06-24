# IBKR 只读预检记录

> 本记录只用于 Stage 9.5 真实券商只读观察准备。
> 它不是实盘授权，不启用订单提交，不启用实盘杠杆。

## 2026-06-24 静态预检

运行命令：

```bash
python -m live.ibkr_readonly_preflight
```

运行结果：

| 项目 | 结果 |
|------|------|
| 状态 | `NOT_READY` |
| 交易能力 | `trading_disabled=True` |
| 实盘杠杆 | `live_leverage_approved=False` |
| 连接尝试 | `connection.attempted=False` |
| 输出文件 | `data/shadow/latest_ibkr_readonly_preflight.json` |

阻断项：

- 缺少必需环境变量：`IBKR_API_BASE_URL`, `IBKR_ACCOUNT_ID`
- 缺少显式 conid 映射：`SPY`, `TLT`, `GLD`, `SHV`, `QQQ`

警告项：

- 缺少建议环境变量：`IBKR_VERIFY_TLS`, `IBKR_TIMEOUT_SEC`, `IBKR_ORDER_TIF`, `IBKR_LISTING_EXCHANGE`, `IBKR_REPLY_CONFIRM_LIMIT`

结论：

预检按设计 fail closed 到 `NOT_READY`。这说明在缺少真实券商只读环境变量和生产资产 conid 映射时，系统不会尝试连接券商，也不会把缺失配置误判为安全状态。

## 下一次预检入口

1. 参考 `.env.ibkr-readonly.example` 在本地安全位置配置只读环境变量。
2. 不设置 `IBKR_ENABLE_ORDER_SUBMISSION`。
3. 不设置 `XIRANG_ENABLE_LIVE_CORE_EXECUTION`。
4. 不设置 `XIRANG_LIVE_CORE_APPROVAL_ID`。
5. 重新运行：

```bash
python -m live.ibkr_readonly_preflight
```

只有状态达到 `READY_FOR_READONLY_CONNECT`，才允许人工执行：

```bash
python -m live.ibkr_readonly_preflight --connect
```

`--connect` 仍然只允许 `BrokerMode.READ_ONLY` 连接检查。任何 `FAIL_CLOSED / NOT_READY / ATTENTION` 都必须先排查，不能进入真实券商 Stage 9.5 观察。
