### 第一个月：税务优化与智能执行（确定性收益）

#### Week 1-2: Tax-Loss Harvesting（美股账户）

**目标**：实现自动税损收割，预计每年节省 5-10 万美元税款

**技术方案**：

```python
# 新增模块：engine/tax_optimizer.py

class TaxLossHarvester:
    """税损收割引擎"""
    
    # 高相关性 ETF 替代品映射表
    SUBSTITUTES = {
        "SPY": ["VOO", "IVV"],      # 标普500
        "TLT": ["VGLT", "SPTL"],    # 长期国债
        "GLD": ["IAU", "GLDM"],     # 黄金
    }
    
    def scan_harvestable_losses(self, portfolio, min_loss_pct=0.05):
        """扫描可收割的税损（亏损 > 5%）"""
        harvestable = []
        for position in portfolio.positions:
            unrealized_loss = position.cost_basis - position.market_value
            if unrealized_loss > position.cost_basis * min_loss_pct:
                harvestable.append({
                    "asset": position.asset,
                    "loss": unrealized_loss,
                    "substitute": self._find_best_substitute(position.asset)
                })
        return harvestable
    
    def execute_harvest(self, position, substitute):
        """执行收割：卖出亏损资产，买入替代品"""
        # 1. 卖出亏损资产
        sell_order = self.broker.sell(position.asset, position.quantity)
        
        # 2. 立即买入替代品（保持仓位不变）
        buy_order = self.broker.buy(substitute, position.quantity)
        
        # 3. 记录到数据库（用于 30 天后换回）
        self.db.save_harvest_event(
            date=today,
            sold_asset=position.asset,
            substitute=substitute,
            loss_harvested=position.unrealized_loss,
            washsale_safe_date=today + timedelta(days=31)
        )
        
        return sell_order, buy_order
    
    def check_washsale_reversals(self):
        """检查是否可以换回原资产（避免 Wash Sale）"""
        events = self.db.get_pending_reversals(today)
        for event in events:
            if today >= event.washsale_safe_date:
                # 换回原资产
                self.broker.sell(event.substitute, event.quantity)
                self.broker.buy(event.sold_asset, event.quantity)
```

**集成到 daily_runner**：

```python
# 在 runner/daily_runner.py 中新增
def run_year_end_tax_optimization(self, portfolio_id="us"):
    """年末税务优化（仅美股账户）"""
    if datetime.now().month != 12:
        return
    
    harvester = TaxLossHarvester(self.db, self.broker)
    losses = harvester.scan_harvestable_losses(self.portfolio)
    
    if losses:
        logger.info(f"发现 {len(losses)} 个可收割税损，总计 ${sum(l['loss'] for l in losses):,.2f}")
        for loss in losses:
            harvester.execute_harvest(loss)
```

**里程碑**：

- Day 7: 完成 `TaxLossHarvester` 模块，通过单元测试
- Day 14: 在 Paper Trading 环境验证，模拟 2020-2025 年的税损收割效果

---

#### Week 3-4: 智能订单执行（TWAP 拆单）

**目标**：大额调仓时降低滑点，预计节省 0.1-0.3% 成本

**技术方案**：

```python
# 新增模块：engine/execution/smart_order.py

class TWAPExecutor:
    """时间加权平均价格执行器"""
    
    def __init__(self, broker, time_window_minutes=120):
        self.broker = broker
        self.time_window = time_window_minutes
    
    def execute_large_order(self, asset, quantity, side="BUY"):
        """将大单拆分为小单，在时间窗口内均匀执行"""
        # 1. 计算拆单策略
        num_slices = 20  # 拆成 20 个小单
        slice_size = quantity / num_slices
        interval = self.time_window * 60 / num_slices  # 秒
        
        # 2. 异步执行（后台任务）
        executed_slices = []
        for i in range(num_slices):
            # 每隔 interval 秒执行一次
            time.sleep(interval)
            
            # 执行小单
            order = self.broker.market_order(asset, slice_size, side)
            executed_slices.append(order)
            
            logger.info(f"TWAP [{i+1}/{num_slices}] {side} {asset} {slice_size:.2f} @ ${order.fill_price:.2f}")
        
        # 3. 计算平均成交价
        avg_price = sum(o.fill_price * o.quantity for o in executed_slices) / quantity
        total_cost = sum(o.fill_price * o.quantity for o in executed_slices)
        
        return {
            "avg_price": avg_price,
            "total_cost": total_cost,
            "slices": executed_slices
        }
```

**触发条件**：

```python
# 在 portfolio.py 的 _execute_rebalance 中
def _execute_rebalance(self, order: RebalanceOrder):
    """执行再平衡：智能拆单"""
    for i, asset in enumerate(ASSETS):
        target_value = order.target_weights[i] * self.nav
        current_value = self.positions[i]
        delta = target_value - current_value
        
        # 如果调仓金额 > 50 万，使用 TWAP
        if abs(delta) > 500000:
            executor = TWAPExecutor(self.broker, time_window_minutes=120)
            side = "BUY" if delta > 0 else "SELL"
            result = executor.execute_large_order(asset, abs(delta), side)
            logger.info(f"TWAP 执行完成: {asset} 平均价 ${result['avg_price']:.2f}")
        else:
            # 小额直接市价单
            self.broker.market_order(asset, delta)
```

**里程碑**：

- Day 21: 完成 `TWAPExecutor` 模块
- Day 28: 在模拟环境测试，对比"市价单 vs TWAP"的滑点差异

---

### 第二个月：防败家子协议（治理层）

#### Week 5-6: 多签与时间锁（软约束版）

**目标**：大额出金需要家族成员确认，防止冲动决策

**技术方案**：

```python
# 新增模块：engine/governance.py

class WithdrawalGovernance:
    """出金治理引擎"""
    
    def __init__(self, db, notifier):
        self.db = db
        self.notifier = notifier
        self.pending_withdrawals = {}
    
    def request_withdrawal(self, amount, reason, requester):
        """发起出金请求"""
        # 1. 判断是否需要多签
        if amount > 500000:  # 50 万以上需要确认
            withdrawal_id = self._create_pending_withdrawal(amount, reason, requester)
            
            # 2. 发送通知给家族成员
            self.notifier.send_withdrawal_alert(
                to=["family_member_1@email.com", "family_member_2@email.com"],
                amount=amount,
                reason=reason,
                approval_link=f"https://xirang.family/approve/{withdrawal_id}"
            )
            
            # 3. 设置 7 天冷却期
            self.db.save_withdrawal_request(
                id=withdrawal_id,
                amount=amount,
                reason=reason,
                requester=requester,
                status="PENDING",
                expires_at=datetime.now() + timedelta(days=7)
            )
            
            return {"status": "PENDING", "id": withdrawal_id, "expires_in": "7 days"}
        else:
            # 小额直接通过
            return self._execute_withdrawal(amount, reason)
    
    def approve_withdrawal(self, withdrawal_id, approver):
        """批准出金"""
        request = self.db.get_withdrawal_request(withdrawal_id)
        
        # 记录批准人
        self.db.add_approval(withdrawal_id, approver, datetime.now())
        
        # 检查是否满足多签条件（比如需要 2/3 家族成员同意）
        approvals = self.db.get_approvals(withdrawal_id)
        if len(approvals) >= 2:
            return self._execute_withdrawal(request.amount, request.reason)
        else:
            return {"status": "WAITING_MORE_APPROVALS", "current": len(approvals), "required": 2}
    
    def _execute_withdrawal(self, amount, reason):
        """实际执行出金"""
        # 从 SHV（现金）中提取
        self.portfolio.withdraw(amount, reason)
        self.db.record_withdrawal(datetime.now(), amount, reason, status="EXECUTED")
        return {"status": "EXECUTED", "amount": amount}
```

**Web 界面（FastAPI）**：

```python
# main.py 中新增路由
@app.post("/api/withdraw")
async def request_withdrawal(amount: float, reason: str, requester: str):
    governance = WithdrawalGovernance(db, notifier)
    result = governance.request_withdrawal(amount, reason, requester)
    return result

@app.get("/api/approve/{withdrawal_id}")
async def approve_withdrawal(withdrawal_id: str, approver: str):
    governance = WithdrawalGovernance(db, notifier)
    result = governance.approve_withdrawal(withdrawal_id, approver)
    return result
```

**里程碑**：

- Day 35: 完成 `WithdrawalGovernance` 模块
- Day 42: 部署 Web 界面，家族成员可以通过链接批准/拒绝

---

#### Week 7-8: 只读面板（家族看板）

**目标**：让家人看到财富增长，但无法操作

**技术方案**：

```python
# 新增：runner/dashboard.py

class FamilyDashboard:
    """家族只读看板"""
    
    def generate_report(self, portfolio_id="us"):
        """生成可视化报告"""
        # 1. 获取最近 90 天的数据
        snapshots = self.db.get_snapshots(portfolio_id, days=90)
        
        # 2. 计算关键指标
        current_nav = snapshots[-1].nav
        nav_90d_ago = snapshots[0].nav
        return_90d = (current_nav - nav_90d_ago) / nav_90d_ago
        
        # 3. 生成图表（使用 matplotlib）
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # NAV 曲线
        axes[0, 0].plot([s.date for s in snapshots], [s.nav for s in snapshots])
        axes[0, 0].set_title("净资产变化（90 天）")
        
        # 权重分布（饼图）
        current_weights = snapshots[-1].weights
        axes[0, 1].pie(current_weights, labels=ASSETS, autopct='%1.1f%%')
        axes[0, 1].set_title("当前资产配置")
        
        # 回撤曲线
        drawdowns = [s.drawdown for s in snapshots]
        axes[1, 0].fill_between(range(len(drawdowns)), drawdowns, 0, color='red', alpha=0.3)
        axes[1, 0].set_title("回撤监控")
        
        # 调仓历史
        rebalances = self.db.get_transactions(portfolio_id, days=90)
        axes[1, 1].bar(range(len(rebalances)), [r.turnover for r in rebalances])
        axes[1, 1].set_title("调仓记录")
        
        # 保存为图片
        output_path = Path("dashboard/family_report.png")
        fig.savefig(output_path, dpi=150)
        
        return {
            "current_nav": current_nav,
            "return_90d": return_90d,
            "report_image": str(output_path)
        }
```

**自动推送**：

```python
# 每周日晚上 8 点，自动生成报告并发送给家人
@app.on_event("startup")
async def schedule_weekly_report():
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=send_family_report,
        trigger="cron",
        day_of_week="sun",
        hour=20,
        minute=0
    )
    scheduler.start()

def send_family_report():
    dashboard = FamilyDashboard(db)
    report = dashboard.generate_report()
    
    notifier.send_email(
        to=["family@email.com"],
        subject="息壤周报 - 本周净资产增长 {:.2%}".format(report["return_90d"]),
        body="详见附件",
        attachments=[report["report_image"]]
    )
```

**里程碑**：

- Day 49: 完成看板生成逻辑
- Day 56: 部署自动推送，家人每周收到报告

---

### 第三个月：达尔文沙盒（谨慎探索）

#### Week 9-10: 备兑期权策略（Covered Call）

**目标**：在 Alpha 层（10% 资金）试运行备兑期权，目标年化 +3-5%

**技术方案**：

```python
# 新增模块：engine/alpha/covered_call.py

class CoveredCallStrategy:
    """备兑期权策略：持有 SPY，卖出虚值看涨期权"""
    
    def __init__(self, broker, capital):
        self.broker = broker
        self.capital = capital
        self.spy_shares = 0
        self.sold_calls = []
    
    def initialize(self):
        """初始化：买入 SPY"""
        spy_price = self.broker.get_price("SPY")
        self.spy_shares = self.capital / spy_price
        self.broker.buy("SPY", self.spy_shares)
    
    def monthly_roll(self):
        """每月滚动：卖出下月到期的虚值看涨期权"""
        # 1. 平掉上个月的期权（如果还没到期）
        for call in self.sold_calls:
            if not call.is_expired():
                self.broker.buy_to_close(call)
        
        # 2. 卖出新的看涨期权（Delta 0.3，约 10% 虚值）
        spy_price = self.broker.get_price("SPY")
        strike = spy_price * 1.10  # 虚值 10%
        expiry = self._next_monthly_expiry()
        
        call = self.broker.sell_call(
            underlying="SPY",
            strike=strike,
            expiry=expiry,
            quantity=self.spy_shares / 100  # 每 100 股对应 1 张期权
        )
        
        self.sold_calls.append(call)
        
        # 3. 记录权利金收入
        premium = call.premium * call.quantity * 100
        logger.info(f"卖出备兑期权: SPY {strike} Call, 收入权利金 ${premium:.2f}")
        
        return premium
```

**集成到 Alpha 层**：

```python
# 在 runner/daily_runner.py 中
def run_alpha_strategies(self, portfolio_id="us"):
    """运行 Alpha 策略（每月第一个交易日）"""
    if not self._is_first_trading_day_of_month():
        return
    
    # 从 Core 层划拨 10% 资金到 Alpha 层
    alpha_capital = self.portfolio.nav * 0.10
    
    # 运行备兑期权策略
    covered_call = CoveredCallStrategy(self.broker, alpha_capital)
    premium = covered_call.monthly_roll()
    
    # 记录收益
    self.db.save_alpha_transaction(
        date=today,
        strategy="covered_call",
        premium=premium,
        portfolio_id=portfolio_id
    )
```

**里程碑**：

- Day 63: 完成备兑期权策略模块
- Day 70: 在 Paper Trading 环境运行 1 个月，观察收益和风险

---

#### Week 11-12: 策略竞技场框架

**目标**：搭建多策略并行运行的框架，为未来扩展做准备

**技术方案**：

```python
# 新增模块：engine/alpha/arena.py

class StrategyArena:
    """策略竞技场：多策略并行运行 + 自动淘汰"""
    
    def __init__(self, total_capital):
        self.total_capital = total_capital
        self.strategies = []
    
    def register_strategy(self, strategy, initial_allocation):
        """注册策略"""
        self.strategies.append({
            "strategy": strategy,
            "capital": initial_allocation,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "status": "ACTIVE"
        })
    
    def quarterly_evaluation(self):
        """季度评估：根据夏普比率重新分配资金"""
        # 1. 计算每个策略的夏普比率
        for s in self.strategies:
            returns = self.db.get_strategy_returns(s["strategy"].name, days=90)
            s["sharpe"] = self._calculate_sharpe(returns)
            s["max_drawdown"] = self._calculate_mdd(returns)
        
        # 2. 淘汰表现最差的策略
        worst = min(self.strategies, key=lambda x: x["sharpe"])
        if worst["sharpe"] < 0.3:  # 夏普 < 0.3 直接淘汰
            worst["status"] = "SUSPENDED"
            logger.warning(f"策略 {worst['strategy'].name} 被暂停（夏普 {worst['sharpe']:.2f}）")
        
        # 3. 重新分配资金（按夏普比率加权）
        active_strategies = [s for s in self.strategies if s["status"] == "ACTIVE"]
        total_sharpe = sum(s["sharpe"] for s in active_strategies)
        
        for s in active_strategies:
            s["capital"] = self.total_capital * (s["sharpe"] / total_sharpe)
            logger.info(f"策略 {s['strategy'].name} 新资金: ${s['capital']:,.2f}")
```

**里程碑**：

- Day 77: 完成竞技场框架
- Day 84: 注册 2-3 个策略（备兑期权、网格交易、动量轮动），并行运行
- Day 90: 完成第一次季度评估，生成策略对比报告

---

## 物理实体化（Bonus：第 4 个月）

如果前 3 个月进展顺利，可以考虑将系统"硬件化"：

### 硬件方案

**1. 树莓派 5 + 工业级 SSD**

- 树莓派 5（8GB RAM）：$80
- 1TB NVMe SSD：$100
- 被动散热机箱（铝合金）：$50
- 电子墨水屏（7.5 寸）：$150
- UPS 不间断电源：$100

**总成本：$480**

**2. 系统部署**

```bash
# 在树莓派上部署息壤
git clone https://github.com/your-repo/xirang.git /opt/xirang
cd /opt/xirang
pip3 install -r requirements.txt

# 配置开机自启
sudo systemctl enable xirang.service

# 配置电子墨水屏（显示实时 NAV）
python3 -m runner.epaper_display
```

**3. 物理防护**

- 机箱加装物理锁（防止未授权访问）
- 数据库加密（SQLCipher）
- SSH 只允许密钥登录，禁用密码

---

## 总结

这个 90 天计划的核心思想是：

1. **第一个月**：做确定性高、ROI 高的事（税务优化）
2. **第二个月**：做长期价值高的事（治理层）
3. **第三个月**：做探索性的事（Alpha 策略），但保持谨慎

**关键原则**：

- 每个模块都要有单元测试
- 每个功能都要在 Paper Trading 环境验证
- 不要一次性上线所有功能，分阶段灰度发布

**最终目标**：

> 让息壤从"一个 Python 脚本"，进化为"一台可以传承 50 年的家族财富永动机"。

准备好开始了吗？我们可以从第一周的 Tax-Loss Harvesting 模块开始动手。