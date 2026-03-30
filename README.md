# 息壤 Xi-Rang

> 息壤：中国神话中能自己生长、永不耗尽的土壤。

状态机驱动的自动化资产配置系统。稳定优先，抗通胀，防止人为犯错。

## 它是什么

一个跑在服务器上的 Python 程序，每天收盘后自动拉取 ETF 行情，通过风控状态机决定是否调仓，把结果记录到 SQLite，有动作时推送通知。你唯一要做的事情是充值。

## 它不是什么

- 不是交易机器人（不自动下单，只生成指令）
- 不是预测系统（不判断涨跌，只做再平衡）
- 不是高频策略（一年调仓约 50 次）

## 核心策略

四资产等权永久组合 + 双层风控断路器。

```
正常模式:   SPY 25% | TLT 25% | GLD 25% | SHV 25%
常规保护:   SPY 10% | TLT 20% | GLD 20% | SHV 50%   ← 回撤 -12% 触发
紧急避险:   SPY  3% | TLT  7% | GLD 15% | SHV 75%   ← 回撤 -14% 触发
```

## 回测成绩（2005-2025，21年）

```
年化收益(CAGR):  7.34%
最大回撤(MDD):  -14.81%
夏普比率:        0.77
调仓次数:        51 次（平均每年 2.4 次）
保护模式:        278 天（占比 5.3%）
```

三项成功标准全部通过：CAGR 跑赢通胀+2%，MDD 控制在 -15% 以内，夏普 > 0.5。

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 准备数据（拉取 2005-2025 ETF + CPI 数据）
python data/data_loader.py

# 跑一次回测验证
python -m backtest.engine_backtest

# 启动每日运行（首次）
python -m runner.daily_runner

# 查看报告
python -m runner.report
```

## 部署到服务器

```bash
# 上传代码
git clone <repo> /opt/xirang && cd /opt/xirang
pip install -r requirements.txt

# 首次运行
python -m runner.daily_runner

# 配置 cron（每个工作日美东收盘后运行）
crontab -e
# 0 22 * * 1-5 cd /opt/xirang && python3 -m runner.daily_runner >> logs/cron.log 2>&1

# 配置通知（可选）
echo "TELEGRAM_BOT_TOKEN=xxx" >> .env
echo "TELEGRAM_CHAT_ID=xxx" >> .env
```

详见 [docs/08-部署指南.md](docs/08-部署指南.md)。

## 日常命令

```bash
python -m runner.report              # 查看全部历史报告
python -m runner.report --days 30    # 最近 30 天
python -m runner.report --days 90    # 最近 90 天
python -m runner.daily_runner        # 手动运行一次
python -m runner.daily_runner --force  # 强制重跑今天
```

## 项目结构

```
xi-rang/
├── engine/                  # 核心引擎
│   ├── config.py            #   全局参数（经 6 轮调参验证）
│   ├── portfolio.py         #   状态机（IDLE / PROTECTION）
│   ├── risk.py              #   风控引擎（回撤 + 相关性崩溃检测）
│   ├── market_data.py       #   行情服务（Yahoo Finance）
│   ├── data_validator.py    #   数据校验（Fail-safe）
│   ├── notifier.py          #   通知推送（Telegram/微信/飞书/钉钉）
│   └── execution/           #   执行层（四阶段演进框架）
│       ├── base.py          #     统一接口
│       ├── paper.py         #     Phase 1: 仿真（当前）
│       ├── manual.py        #     Phase 2: 人工执行
│       ├── broker.py        #     Phase 3/4: 券商 API
│       └── factory.py       #     工厂（环境变量切换）
├── backtest/                # 回测
│   ├── simple_backtest.py   #   Phase 1: 纯被动永久组合
│   ├── stateful_backtest.py #   Phase 2: 带风控状态机
│   └── engine_backtest.py   #   用正式引擎跑历史数据
├── runner/                  # 运行器
│   ├── daily_runner.py      #   每日运行（cron 驱动）
│   └── report.py            #   汇总报告 V2.0
├── data/                    # 市场数据
│   └── data_loader.py       #   数据拉取与清洗
├── db/                      # 数据库
│   ├── schema.sql           #   表结构
│   └── database.py          #   SQLite 封装
├── docs/                    # 文档
├── main.py                  # FastAPI 服务（可选）
└── requirements.txt
```

## 从仿真到实盘

```
Phase 1: Paper Trading（当前）→ 3-6 个月验证
Phase 2: 人工执行（系统出指令，你手动买卖）→ 6-12 个月
Phase 3: 半自动（券商 API + 人工确认）→ 3-6 个月
Phase 4: 全自动（只在风控触发时通知）
```

切换只需改一个环境变量 `XIRANG_EXECUTOR`，核心引擎代码零修改。详见 [docs/09-实盘路线图.md](docs/09-实盘路线图.md)。

## 文档

| 文档 | 内容 |
|------|------|
| [01-项目概述](docs/01-项目概述.md) | 息壤是什么、核心理念 |
| [02-系统设计](docs/02-系统设计.md) | 三层结构、状态机、资金流、风控 |
| [03-技术方案](docs/03-技术方案.md) | Python 单体 + SQLite |
| [04-MVP验证计划](docs/04-MVP验证计划.md) | 三阶段验证路径 |
| [05-组织与合规备忘](docs/05-组织与合规备忘.md) | 法律边界提醒 |
| [06-Phase2调参记录](docs/06-Phase2调参记录.md) | 风控引擎 6 轮调参全记录 |
| [07-上线前审计清单](docs/07-上线前审计清单.md) | 8 项安全审计 |
| [08-部署指南](docs/08-部署指南.md) | 服务器部署 + cron 配置 |
| [09-实盘路线图](docs/09-实盘路线图.md) | 四步演进路径 + 执行层架构 |

## 技术栈

- Python 3.12+
- SQLite（零运维）
- FastAPI（可选 API 服务）
- yfinance（行情数据）
- pandas / numpy（计算）
- matplotlib（回测图表）

## 许可

私人项目，仅供个人 / 家族使用。
