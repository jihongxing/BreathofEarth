# Changelog

所有重要变更记录在此。格式遵循 [Keep a Changelog](https://keepachangelog.com/)。

## [Unreleased] - 2026-04-17

### 核心边界收紧

- 明确出金长期原则：系统只允许申请与留痕，不允许自动批准或自动执行
- 继续拆分 `Core / Stability / Alpha` 边界，Alpha 以独立资金账本运行
- 启动时强制执行 `validate_config()`，配置不合法时拒绝启动

### 券商接入与观察面板

- 完成 `Broker Sync` 只读同步、对账、历史时间轴、异常持续次数与连续漂移天数展示
- 将 `Broker Sync / Shadow Run / Core 实盘结果` 接入统一观察面板
- `daily_runner` 已接入：
  - 券商同步前置硬闸门
  - broker sync policy 按中美市场分别配置
  - 执行白名单矩阵
  - 执行后对账闭环

### 最小真实交易能力

- `IBKRAdapter` 已具备最小真实交易协议骨架
  - 下单
  - 查单
  - 撤单
  - reply 确认链处理
- `FutuAdapter` 已补到同一层级
  - 下单
  - 查单
  - 撤单
  - 费用查询与回执解析
- `BrokerExecutor` 已接入最小真实执行主链路
- 真实交易默认关闭，必须显式开启券商级环境变量

### 执行审计

- 新增执行审计表：`db/schema_broker_execution.sql`
- 新增 `broker_execution_events` 持久化真实订单提交、状态轮询、回执结果
- `daily_runner` 会在与主运行同一事务中写入执行审计事件

### 文档同步

- 更新 `docs/13-券商接入主备与沙箱实施方案.md`，标记当前已推进到“最小交易能力边界”
- 更新 `docs/01/02/03/07/09`，统一当前项目口径
- 更新根目录 `README.md`，移除过期宣传型回测表述，改为反映当前真实状态

## [0.4.0] - 2026-04-04

### Web 前端 & API 服务

- 新增完整 Web UI（单页应用）
  - 仪表盘：净资产、收益率、回撤、持仓分布图表
  - 资金管理：出金申请 + 多签审批流程
  - 家族月报：在线查看 + PDF 导出
  - 调仓历史、达尔文沙盒、数据状态页
  - 中英双语切换（i18n.js）
  - 登录页 + JWT 认证（admin / member / viewer 三级权限）
- 新增 RESTful API 层（`api/routes/`）
  - 8 个路由模块：auth / dashboard / portfolio / governance / admin / alpha / data / report
  - JWT 认证中间件、依赖注入、请求模型
- 前端零外部依赖
  - Chart.js CDN → 本地 `chart.min.js`
  - Tailwind CSS CDN → 本地 `tailwind-local.css`（纯 CSS 手写替代）
  - favicon 用 data URI 内联，无额外请求
  - 断网环境完全可用

### 数据管理

- 新增 `data/data_manager.py` 多源数据管理器
  - 优先 akshare（新浪/东财），fallback yfinance
  - 本地 CSV 缓存优先，增量更新，限流保护
  - 支持中国 A 股 ETF + 美股 + 全球市场
- 新增 `data/scheduler.py` 自动更新调度器
  - 每天 18:00 Live 更新，每周六 10:00 全量更新
  - 启动时自动检查数据新鲜度
  - 随 FastAPI lifespan 启停

### Alpha 策略沙盒

- 新增 `engine/alpha/` 达尔文竞技场
  - `arena.py`: 策略擂台，自动回测 + 排名
  - `momentum.py`: 动量策略
  - `grid_trading.py`: 网格策略
  - `covered_call.py`: 备兑看涨策略
  - `registry.py`: 策略注册表

### 治理与风控

- 新增 `engine/governance.py` 出金治理模块
  - 大额出金多签审批 + 冷却期
  - 审计日志追踪
- 新增 `engine/tax_optimizer.py` 税损收割优化器
- 新增 `engine/cashflow.py` 现金流管理
- 新增 `engine/execution/twap.py` TWAP 智能拆单引擎

### 测试

- 新增单元测试覆盖（`tests/`）
  - test_portfolio_engine / test_risk_engine / test_data_validator
  - test_alpha_arena / test_tax_optimizer / test_tax_harvest_integration
  - test_twap_executor / test_twap_backtest

### 其他

- `.gitignore` 新增 `.ai/` `.claude/` `.kiro/` 排除规则
- 新增 `runner/dashboard.py` 终端仪表盘
- 新增多市场数据文件（market_cn/eu/india/global/chimerica.csv）

## [0.3.0] - 2026-03-30

### 执行层框架

- 新增 `engine/execution/` 四阶段执行层架构
  - `base.py`: 统一接口（TradeOrder / ExecutionResult / BaseExecutor）
  - `paper.py`: Phase 1 仿真执行器
  - `manual.py`: Phase 2 人工执行器（生成具体买卖股数指令）
  - `broker.py`: Phase 3/4 券商执行器骨架（IBKR / 富途适配器）
  - `factory.py`: 执行器工厂，通过 `XIRANG_EXECUTOR` 环境变量切换阶段
- 新增 `docs/09-实盘路线图.md`

### 报告系统 V2.0

- `runner/report.py` 升级为终极看板，新增 5 个维度：
  - ASCII 净值走势图
  - 基准对比（SPY / 60-40 / 通胀）
  - 收益归因（四大资产各自贡献）
  - 持仓偏离度预警（距下次调仓的距离）
  - 水下时间（最长回撤恢复周期）
  - 系统健康评分（自动打分 + 问题提示）
- 新增 `runner/mock_report.py` 模拟报告生成器

## [0.2.0] - 2026-03-30

### 全自动运行系统

- `runner/daily_runner.py` 重构为 cron 驱动模式，跑完即退出
  - 幂等性保护（daily_runs 表，一天只运行一次）
  - 数据拉取失败自动重试 3 次（间隔 60 秒）
  - 数据合理性校验 Fail-safe（价格异常 / 涨跌幅 > 25% 自动中止）
  - 每次运行后自动备份数据库（保留 30 天）
  - 运行日志写入 `logs/xirang.log`
  - 支持 `--force` 参数强制重跑
- 新增 `engine/data_validator.py` 数据校验模块
- 新增 `engine/notifier.py` 通知推送模块
  - 支持 Telegram / 企业微信 / 飞书 / 钉钉
  - 静默与唤醒机制：无动作时零通知
- `main.py` 简化为纯查询 API，去掉 APScheduler 依赖
- 新增 `.gitignore`（凭证 / 数据库 / 日志全部排除）
- 新增 `docs/07-上线前审计清单.md`（8 项安全审计全部通过）
- 新增 `docs/08-部署指南.md`

## [0.1.0] - 2026-03-30

### 核心引擎与回测验证

- Phase 0: 数据准备
  - `data/data_loader.py`: 从 Yahoo Finance 拉取 SPY/TLT/GLD/SHV 日线数据（2005-2025）
  - 从 FRED 拉取 CPI 月度数据
  - 数据清洗：Adjusted Close、交易日对齐、SHV 缺失填充、月度对齐

- Phase 1: 最简回测
  - `backtest/simple_backtest.py`: 等权永久组合 + 双轨再平衡
  - 结果：CAGR 7.42%，MDD -17.20%，夏普 0.75
  - MDD 未通过 -15% 标准，确认需要风控引擎

- Phase 2: 状态机风控回测
  - `backtest/stateful_backtest.py`: IDLE / PROTECTION 双模式状态机
  - 经历 6 轮调参（详见 `docs/06-Phase2调参记录.md`）：
    - 第 1 轮：基础状态机，MDD -15.68%
    - 第 2 轮：全面收紧，过度防御，CAGR 暴跌
    - 第 3 轮：寻找中间点，发现问题在检测精度
    - 第 4 轮：精准化相关性检测（转折点），误触发从 17 次降到 10 次
    - 第 5 轮：收紧回撤阈值到 -10%，失败，过早介入更差
    - 第 6 轮：引入双层防线（保险丝 + 总闸），MDD -14.81%，三项全通过
  - 最终结果：CAGR 7.34%，MDD -14.81%，夏普 0.77

- Phase 3: 引擎模块化
  - `engine/config.py`: 全局参数集中管理
  - `engine/portfolio.py`: 状态机引擎
  - `engine/risk.py`: 风控引擎
  - `engine/market_data.py`: 行情服务
  - `backtest/engine_backtest.py`: 用正式引擎跑历史数据，结果与 Phase 2 完全一致
  - `runner/daily_runner.py`: 每日运行器
  - `main.py`: FastAPI 服务
  - `db/`: SQLite 持久化层

### 文档体系

- 从 9 份 AI 对话体文档整理为 6 份工程文档
- 原始文档归档至 `docs/archive/`
- 项目命名为"息壤"（Xi-Rang）
