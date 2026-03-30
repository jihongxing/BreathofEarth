把前面的状态机落成 **API 设计 + 数据结构 + 数据库模型 + 关键算法接口**。默认是**后端服务（RESTful + 事件驱动）**，支持将来接入券商/交易所执行层。

---

# 一、系统分层与服务划分

[ API Gateway ]  
      ↓  
[ Auth Service ]  
[ Account Service ]  
[ Portfolio Service ]  ← 组合与状态机核心  
[ Rebalance Service ]  ← 调仓引擎  
[ Risk Service ]       ← 风控引擎  
[ Capital Service ]    ← 资金流（入金/出金/分配）  
[ Alpha Service ]      ← 增长层（可选）  
[ Market Data Service ]  
[ Scheduler / Worker ] ← 定时任务 & 事件处理  
[ Execution Adapter ]  ← 券商/ETF交易接口（可插拔）

---

# 二、核心数据模型（领域对象）

## 1）User / Account

User {  
  id: UUID,  
  email: string,  
  created_at: timestamp  
}  
  
Account {  
  id: UUID,  
  user_id: UUID,  
  base_currency: "USD" | "CNY" | "SGD",  
  status: "ACTIVE" | "SUSPENDED",  
  created_at: timestamp  
}

---

## 2）Portfolio（组合总对象）

Portfolio {  
  id: UUID,  
  account_id: UUID,  
  
  state: "IDLE" | "INFLOW" | "REBALANCING" | "WITHDRAWAL" | "RISK_ALERT" | "PROTECTION",  
  
  target_allocation: {  
    core: 0.75,  
    stability: 0.15,  
    alpha: 0.10  
  },  
  
  created_at: timestamp,  
  updated_at: timestamp  
}

---

## 3）Layer（分层）

Layer {  
  id: UUID,  
  portfolio_id: UUID,  
  
  type: "CORE" | "STABILITY" | "ALPHA",  
  state: string,  // 子状态机  
  
  nav: number,        // 当前净值  
  cash: number,       // 可用现金  
  
  created_at: timestamp,  
  updated_at: timestamp  
}

---

## 4）Asset（资产定义）

Asset {  
  id: UUID,  
  symbol: string,     // ETF / 基金代码  
  name: string,  
  asset_class: "EQUITY" | "BOND" | "GOLD" | "COMMODITY" | "CASH",  
  
  liquidity_score: number,  
  expense_ratio: number  
}

---

## 5）Position（持仓）

Position {  
  id: UUID,  
  layer_id: UUID,  
  asset_id: UUID,  
  
  quantity: number,  
  avg_price: number,  
  market_value: number,  
  
  target_weight: number,  
  current_weight: number,  
  
  updated_at: timestamp  
}

---

## 6）Transaction（交易记录）

Transaction {  
  id: UUID,  
  account_id: UUID,  
  type: "DEPOSIT" | "WITHDRAWAL" | "BUY" | "SELL" | "REBALANCE",  
  
  asset_id: UUID | null,  
  amount: number,  
  quantity: number | null,  
  price: number | null,  
  
  status: "PENDING" | "COMPLETED" | "FAILED",  
  created_at: timestamp  
}

---

## 7）Rebalance Task（调仓任务）

RebalanceTask {  
  id: UUID,  
  portfolio_id: UUID,  
  
  trigger_type: "THRESHOLD" | "TIME" | "CASHFLOW",  
  status: "PENDING" | "RUNNING" | "DONE",  
  
  deviation_snapshot: json,  
  orders: json,  
  
  created_at: timestamp  
}

---

## 8）Risk Event（风控事件）

RiskEvent {  
  id: UUID,  
  portfolio_id: UUID,  
  
  type: "CORRELATION_BREAK" | "DRAWDOWN" | "LIQUIDITY" | "ALPHA_STOP",  
  
  severity: "LOW" | "MEDIUM" | "HIGH",  
  action_taken: string,  
  
  created_at: timestamp  
}

---

# 三、数据库模型（SQL结构）

（简化版，PostgreSQL）

CREATE TABLE users (  
  id UUID PRIMARY KEY,  
  email TEXT,  
  created_at TIMESTAMP  
);  
  
CREATE TABLE accounts (  
  id UUID PRIMARY KEY,  
  user_id UUID,  
  base_currency TEXT,  
  status TEXT,  
  created_at TIMESTAMP  
);  
  
CREATE TABLE portfolios (  
  id UUID PRIMARY KEY,  
  account_id UUID,  
  state TEXT,  
  target_allocation JSONB,  
  created_at TIMESTAMP,  
  updated_at TIMESTAMP  
);  
  
CREATE TABLE layers (  
  id UUID PRIMARY KEY,  
  portfolio_id UUID,  
  type TEXT,  
  state TEXT,  
  nav NUMERIC,  
  cash NUMERIC,  
  updated_at TIMESTAMP  
);  
  
CREATE TABLE assets (  
  id UUID PRIMARY KEY,  
  symbol TEXT,  
  asset_class TEXT  
);  
  
CREATE TABLE positions (  
  id UUID PRIMARY KEY,  
  layer_id UUID,  
  asset_id UUID,  
  quantity NUMERIC,  
  avg_price NUMERIC,  
  market_value NUMERIC,  
  target_weight NUMERIC,  
  current_weight NUMERIC  
);  
  
CREATE TABLE transactions (  
  id UUID PRIMARY KEY,  
  account_id UUID,  
  type TEXT,  
  asset_id UUID,  
  amount NUMERIC,  
  status TEXT,  
  created_at TIMESTAMP  
);  
  
CREATE TABLE rebalance_tasks (  
  id UUID PRIMARY KEY,  
  portfolio_id UUID,  
  trigger_type TEXT,  
  status TEXT,  
  deviation_snapshot JSONB,  
  orders JSONB  
);  
  
CREATE TABLE risk_events (  
  id UUID PRIMARY KEY,  
  portfolio_id UUID,  
  type TEXT,  
  severity TEXT,  
  action_taken TEXT  
);

---

# 四、API设计（核心接口）

## 1）账户 & 入金

### 入金

POST /api/v1/deposit

{  
  "account_id": "uuid",  
  "amount": 10000  
}

---

## 2）获取组合状态

GET /api/v1/portfolio/{id}

返回：

{  
  "state": "IDLE",  
  "nav": 105000,  
  "layers": [...]  
}

---

## 3）触发再平衡（内部/系统调用）

POST /api/v1/rebalance/trigger

{  
  "portfolio_id": "uuid",  
  "trigger_type": "THRESHOLD"  
}

---

## 4）执行调仓

POST /api/v1/rebalance/execute

---

## 5）提现请求

POST /api/v1/withdraw

{  
  "account_id": "uuid",  
  "amount": 5000  
}

---

## 6）风险状态查询

GET /api/v1/risk/{portfolio_id}

---

# 五、核心算法接口（服务内部）

---

## 1）偏离度计算

def calc_deviation(current_weight, target_weight):  
    return (current_weight - target_weight) / target_weight

---

## 2）再平衡决策

def should_rebalance(portfolio):  
    for position in portfolio.positions:  
        if abs(calc_deviation(...)) > 0.2:  
            return True  
    return False

---

## 3）订单生成（关键）

def generate_orders(portfolio):  
    # 1. 用现金补低配  
    # 2. 再卖高配  
    # 3. 最小交易成本  
    return orders

---

## 4）风控检测

def detect_correlation_break(data):  
    if stock_bond_corr > 0.5:  
        return True

---

# 六、调度系统（Scheduler）

---

## 定时任务

Daily:  
  - 更新市场数据  
  - 风控检测  
  
Monthly:  
  - 收益分析  
  
Quarterly:  
  - 检查再平衡  
  
Yearly:  
  - 强制再平衡

---

# 七、事件驱动（建议）

使用 Event Bus（Kafka / Redis Stream）：

---

## 关键事件

DEPOSIT_RECEIVED  
REBALANCE_TRIGGERED  
RISK_TRIGGERED  
WITHDRAWAL_REQUESTED  
ALPHA_STOPPED

---

# 八、执行层（非常关键）

Execution Adapter:  
  
- IBKR API  
- 富途  
- 雪球（模拟）  
- 自建撮合（模拟环境）

---

# 九、系统最关键的约束（必须写死）

1. Core Layer 不允许被直接卖出（除非再平衡）  
2. Alpha 最大 10%  
3. 风控优先级 > 一切交易  
4. 再平衡频率限制（≥90天）

---

# 十、最终系统一句话（工程定义）

> 这是一个以“状态机驱动 + 事件触发 + 风控优先”为核心的自动资产配置执行系统