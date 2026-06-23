# IBKR 只读接入清单

> 这份清单只用于 Stage 9.5 的真实券商只读观察准备。
> 它不启用订单提交，不启用实盘杠杆，不构成交易授权。

## 一、目标

目标是把 IBKR 接入到 `READ_ONLY` 观察链路里，用于：

- 读取报价。
- 读取账户快照。
- 读取保证金字段。
- 验证 `shadow_sync`、`margin_monitor` 和前端 Stage 9.5 面板在真实券商环境下的表现。

## 二、必须保留的红线

- 不设置 `IBKR_ENABLE_ORDER_SUBMISSION=1`。
- 不设置 `XIRANG_ENABLE_LIVE_CORE_EXECUTION=1`。
- 不设置 `XIRANG_LIVE_CORE_APPROVAL_ID`。
- 不提交真实订单。
- 不撤销真实订单。
- 不做 Shadow 到 Live 的自动切换。
- 不把缺失的保证金字段解释成安全。

## 三、环境变量

### 1. 只读连接

至少准备：

```bash
IBKR_API_BASE_URL=https://127.0.0.1:5000/v1/api
IBKR_ACCOUNT_ID=<真实账户号或纸面账户号>
IBKR_VERIFY_TLS=true
IBKR_TIMEOUT_SEC=10
IBKR_ORDER_TIF=DAY
IBKR_LISTING_EXCHANGE=SMART
IBKR_REPLY_CONFIRM_LIMIT=3
```

如果是本地 Client Portal Gateway，自签证书和端口设置需要与实际部署一致。这里不把任何“方便调试”的设置当成生产默认。

### 2. 标的映射

如果 IBKR 对标的返回多个 `conid`，必须显式设置：

```bash
IBKR_CONID_SPY=<conid>
IBKR_CONID_TLT=<conid>
IBKR_CONID_GLD=<conid>
IBKR_CONID_SHV=<conid>
IBKR_CONID_QQQ=<conid>
```

只读观察阶段，宁可失败，也不要模糊映射。

## 四、接入前检查

### 1. 连接检查

- `/iserver/accounts` 可访问。
- 账户号能在 `/portfolio/accounts` 返回列表里找到。
- `BrokerMode.READ_ONLY` 能创建。
- `IBKRAdapter.connect()` 返回成功。

### 2. 报价检查

- `SPY / TLT / GLD / SHV / QQQ` 都能返回正价格。
- 如果某个报价缺失，`shadow_sync` 必须写 warning。
- 如果价格非正，必须 fail closed。

### 3. 保证金检查

- `NetLiquidation`。
- `ExcessLiquidity`。
- `FullMaintainMarginReq`。

缺失任何一个字段都必须被标记为 `UNAVAILABLE` 或 `PARTIAL`，不能推导安全结论。

### 4. 前端检查

- Stage 9.5 面板必须继续显示 `Live leveraged execution NOT YET APPROVED`。
- `UNAVAILABLE / WARNING / ATTENTION` 必须可见。
- 不能出现交易、加杠杆或 Shadow 转 Live 控件。

## 五、接入顺序

1. 先在本地离线 smoke 目录复跑一遍，确认手册和前端已一致。
2. 再把 `XIRANG_SHADOW_AUDIT_DIR` 指到真实只读观察目录。
3. 最后只启用读取报价和账户快照，不启用任何提交订单链路。

## 六、成功标准

只读接入算成功，不是因为“看起来能跑”，而是因为以下几件事都成立：

- 真实券商连接可达。
- 真实报价可读。
- 真实保证金字段可读，或者缺失时明确暴露为 `UNAVAILABLE / PARTIAL`。
- 前端页面仍然保持只读治理状态。
- 任何异常都没有被误渲染成安全。

## 七、失败时怎么做

如果真实只读接入失败：

- 先不要改策略。
- 先不要调阈值。
- 先不要放宽风控。
- 先修连接、标的映射、字段解析和前端显示。

这一步的价值是确认现实世界有没有打穿我们的观察假设，不是把现实硬拽成回测。

## 八、下一步

如果这份清单准备好了，下一步就是：

1. 在真实只读环境里跑一次 Stage 9.5 smoke。
2. 把真实券商返回和离线 smoke 的差异记录进 QA 文档。
3. 继续保持 `live_leverage_approved = false`，直到人工评审通过。
