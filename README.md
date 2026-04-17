# 息壤 Xi-Rang

> 家族资产的低错误率控制系统。  
> 优先级固定为：`稳定 > 抗通胀 > 收益`。

息壤不是为了追求更刺激的收益，而是为了减少错误、克制交易、长期跑赢通胀。

## 当前状态

当前项目状态：`已完成最小真实交易能力边界，进入阶段性暂停扩功能 + 文档收口阶段`

已经落地：

- `Core / Stability / Alpha` 边界进一步收紧，Alpha 独立账本运行
- 出金坚持“只能申请，不可自动批准/执行”
- `Broker Sync / Shadow Run / 执行前硬闸门 / 执行后对账` 已接入主链路
- `IBKR / Futu / Paper` 三类适配器骨架已具备
- `BrokerExecutor` 已接入最小真实执行骨架
- 真实执行审计流水已落库：`broker_execution_events`
- 首页与只读观察面板已能查看 `Core / Broker Sync / Shadow Run`

明确不做或暂不推进：

- 自动出金
- 自动主备切换
- 刺激交易冲动的操作台
- 在账本闭环前继续扩收益增强模块

## 它是什么

一个运行在本地或服务器上的 Python 系统，每日收盘后拉取行情，做数据校验、风控判断、调仓决策，并把结果写入账本与审计记录。

系统允许：

- 入金自动入账与分层分配
- Core 的小额、白名单、强约束真实调仓
- 真实账户同步、影子运行、执行后对账

系统不允许：

- 自动出金批准
- 自动出金执行
- Alpha 与主仓混账
- 用模拟利润充当正式收益

## 它不是什么

- 不是高频交易机器人
- 不是追涨杀跌系统
- 不是鼓励频繁交易的前端产品
- 不是“默认打开就自动下单”的实盘工具

更准确地说，它是一个带有最小执行能力的资产控制系统，而不是一个追求交易活跃度的交易产品。

## 当前可执行结果

2026-04-17 本地执行：

```bash
python -m backtest.engine_backtest
```

当前引擎回测结果为：

- 年化收益（CAGR）：`7.65%`
- 最大回撤（MDD）：`-18.12%`
- 夏普比率：`0.61`

当前判定：

- `CAGR ≥ 通胀 + 2%`：通过
- `MDD ≤ -15%`：未通过
- `夏普 > 0.5`：通过

因此，当前仓库不应再对外表述为“三项标准全部通过”。更诚实的说法是：

- 方向仍然成立
- 当前执行版本仍需继续调优风控与回测口径
- 系统价值更多来自边界硬化和低错误率，而不是宣称已经得到完美收益曲线

## 运行边界

### 入金

- 可以自动化进入 Stability
- 可在不破坏风险边界时补充 Core

### 出金

- 只能发起请求
- 只能人工审批
- 只能人工执行
- 系统只负责留痕、审批链、审计记录

### Alpha / Lab

- 不是主仓收益增强器
- 是独立实验仓
- 独立资金账本、独立净值、独立审计
- 不进入正式收益汇报

## 券商与执行现状

当前已接入的最小能力：

- `IBKR`
  - 只读同步
  - 对账
  - 最小真实下单 / 查单 / 撤单骨架
- `Futu`
  - 只读同步
  - 对账
  - 最小真实下单 / 查单 / 撤单骨架
- `Paper`
  - 模拟券商接口
  - 用于 Shadow / Sandbox / 回归测试

真实执行不是默认开启的。即使代码已具备能力，也必须显式开启券商级环境变量，并通过：

- 券商同步覆盖交易日
- 对账不漂移
- 市场与资产白名单
- 订单数 / 换手 / 单笔金额上限
- 执行后对账闭环

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 运行引擎回测
python -m backtest.engine_backtest

# 启动 Web 服务
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# 手动运行每日任务
python -m runner.daily_runner
```

## 常用命令

```bash
python -m runner.daily_runner --force
python -m runner.report
python -m runner.report --days 30
python -m runner.broker_sync --portfolio us
python -m runner.broker_sync --portfolio cn --role backup
```

## 项目结构

```text
BreathofEarth/
├── api/                  # FastAPI 路由与只读 API
├── backtest/             # 回测脚本
├── data/                 # 行情数据与调度
├── db/                   # SQLite 与 schema
├── docs/                 # 项目文档
├── engine/               # 核心引擎、执行层、治理层、Alpha
├── frontend/             # 只读观察面板
├── runner/               # daily_runner / broker_sync / shadow_run / report
├── tests/                # 回归测试
├── CHANGELOG.md
├── DEPLOY.md
├── README.md
└── main.py
```

## 文档入口

- [项目概述](D:/codeSpace/BreathofEarth/docs/01-%E9%A1%B9%E7%9B%AE%E6%A6%82%E8%BF%B0.md)
- [系统设计](D:/codeSpace/BreathofEarth/docs/02-%E7%B3%BB%E7%BB%9F%E8%AE%BE%E8%AE%A1.md)
- [技术方案](D:/codeSpace/BreathofEarth/docs/03-%E6%8A%80%E6%9C%AF%E6%96%B9%E6%A1%88.md)
- [上线前审计清单](D:/codeSpace/BreathofEarth/docs/07-%E4%B8%8A%E7%BA%BF%E5%89%8D%E5%AE%A1%E8%AE%A1%E6%B8%85%E5%8D%95.md)
- [实盘路线图](D:/codeSpace/BreathofEarth/docs/09-%E5%AE%9E%E7%9B%98%E8%B7%AF%E7%BA%BF%E5%9B%BE.md)
- [家族资产稳健化审计](D:/codeSpace/BreathofEarth/docs/12-%E5%AE%B6%E6%97%8F%E8%B5%84%E4%BA%A7%E7%A8%B3%E5%81%A5%E5%8C%96%E5%AE%A1%E8%AE%A1.md)
- [券商接入主备与沙箱实施方案](D:/codeSpace/BreathofEarth/docs/13-%E5%88%B8%E5%95%86%E6%8E%A5%E5%85%A5%E4%B8%BB%E5%A4%87%E4%B8%8E%E6%B2%99%E7%AE%B1%E5%AE%9E%E6%96%BD%E6%96%B9%E6%A1%88.md)

## 一句话总结

息壤当前最值得信任的地方，不是“它已经能帮你赚很多”，而是“它越来越不容易帮你犯大错”。
